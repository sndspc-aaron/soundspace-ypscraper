import requests as rq
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from urllib.parse import urlparse, parse_qsl

# Initialize cache
cache = {}

def cached_request(url, session):
    if url not in cache:
        try:
            response = session.get(url)
            response.raise_for_status()
            cache[url] = response
        except rq.RequestException as e:
            print(f"Request error: {e}")
            return None
    return cache[url]

def read_lines_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = [line.strip() for line in file.readlines()]
            return lines if lines else ["No data in file"]
    except IOError as e:
        print(f"File error: {e}")
        return []

def extract(url, session):
    headers = {'User-Agent': 'Mozilla/5.0 ...'}
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        soup = bs(response.content, 'lxml')
        return soup.find_all('div', class_='info')
    except rq.RequestException as e:
        print(f"Request error: {e}")
        return []

def extract_business_info(item, query):
    business_url = f"http://www.yellowpages.com{item.find('a', class_='business-name')['href']}" if item.find('a', class_='business-name') else ''
    rank = item.find('h2', class_='n').text.split('.')[0] if item.find('h2', class_='n') else ''
    return {
        'name': item.find('a', class_='business-name').text if item.find('a', class_='business-name') else '',
        'phone': item.find('div', class_='phones phone primary').text if item.find('div', class_='phones phone primary') else '',
        'address': item.find('div', class_='adr').get_text(separator=', ') if item.find('div', class_='adr') else '',
        'website': item.find('a', class_='track-visit-website')['href'] if item.find('a', class_='track-visit-website') else '',
        'yp_url': business_url,
        f"{query}_rank": rank
    }

def transform(articles, location, query, business_data, session):
    for item in articles:
        info = extract_business_info(item, query)
        unique_id = (info['name'], info['address'])

        business_data.setdefault(unique_id, info).update({
            'location': location,
            'search_datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        process_follow_links(item, unique_id, business_data, session)

    return business_data

def process_follow_links(item, unique_id, business_data, session):
    follow_links = [a['href'] for a in item.find_all('a', class_='business-name') if "#" not in a['href']]
    for link in follow_links:
        details_response = cached_request('http://www.yellowpages.com' + link, session)
        if details_response:
            details_page = bs(details_response.content, "lxml")
            update_business_details(unique_id, business_data, details_page)
            space_image_link = extract_space_image(details_page, session)
            if space_image_link:
                business_data[unique_id]['space_image'] = space_image_link

def process_follow_links(item, unique_id, business_data, session):
    follow_links = [a['href'] for a in item.find_all('a', class_='business-name') if "#" not in a['href']]
    for link in follow_links:
        details_response = cached_request('http://www.yellowpages.com' + link, session)
        if details_response:
            details_page = bs(details_response.content, "lxml")
            update_business_details(unique_id, business_data, details_page)
            space_image_link = extract_space_images(details_page, session)
            if space_image_link:
                business_data[unique_id]['space_image'] = space_image_link

def extract_space_images(details_page, session):
    # Find the 'media-thumbnail collage-pic' link and follow it
    media_thumbnail_link = details_page.find('a', class_='media-thumbnail collage-pic')
    if media_thumbnail_link:
        gallery_link = 'http://www.yellowpages.com' + media_thumbnail_link['href']
        gallery_response = cached_request(gallery_link, session)
        if gallery_response:
            gallery_page = bs(gallery_response.content, 'lxml')
            
            # Find all links with 'data-media' attribute
            data_media_links = gallery_page.find_all('a', attrs={'data-media': True})
            image_urls = [link.find('img')['src'] for link in data_media_links if link.find('img')]
            
            # Return a comma-separated string of image URLs
            return ', '.join(image_urls)
    return None

def update_business_details(unique_id, business_data, details_page):
    additional_details = {
        'slogan': get_text_or_none(details_page, "h2", class_="slogan"),
        'general_info': get_text_or_none(details_page, "dd", class_="general-info"),
        'neighborhood': get_text_or_none(details_page, "dd", class_="neighborhoods"),
        'email': extract_email(details_page),
        'extra_phones': get_text_or_none(details_page, "dd", class_="extra-phones", sep=' '),
        'social_links': get_text_or_none(details_page, "dd", class_="social-links", sep=', '),
        'categories': get_text_or_none(details_page, "dd", class_="categories"),
        'hour_category': get_text_or_none(details_page, "span", class_="hour-category"),
        'other_info': extract_other_info(details_page),
        'detailed_hours': extract_detailed_hours(details_page)
    }
    business_data[unique_id].update(additional_details)

def get_text_or_none(soup, tag, class_=None, sep=''):
    element = soup.find(tag, class_=class_)
    if element:
        return element.get_text(separator=sep).strip()
    return ''

def extract_email(details_page):
    email_element = details_page.find('a', class_='email-business')
    return email_element.get('href').split(':')[1] if email_element else ''

def extract_other_info(details_page):
    other_info_dd = details_page.find("dd", class_="other-information")
    if other_info_dd:
        return ', '.join([p.get_text(separator=' ', strip=True).replace(" :", ":") 
                          for p in other_info_dd.find_all('p') if p.get_text(strip=True)])
    return ''

def extract_detailed_hours(details_page):
    hours_div = details_page.find('div', class_='open-details')
    if hours_div and (hours_table := hours_div.find('table')):
        return ', '.join([f"{row.find('th').get_text().strip()} {row.find('td').get_text().strip()}"
                          for row in hours_table.find_all('tr') if row.find('th') and row.find('td')])
    return ''

def save_to_csv(business_data, filename):
    if not os.path.exists('exports'):
        os.makedirs('exports')

    # Check if the file already exists
    file_path = f'exports/{filename}'
    if os.path.exists(file_path):
        # Load existing data
        existing_df = pd.read_csv(file_path)
        # Convert business_data to DataFrame
        new_df = pd.DataFrame.from_dict(business_data, orient='index')
        # Combine new data with existing data
        combined_df = pd.concat([existing_df, new_df]).drop_duplicates().reset_index(drop=True)
    else:
        # Convert business_data to DataFrame
        combined_df = pd.DataFrame.from_dict(business_data, orient='index')

    # Ensure 'space_image' column is included
    if 'space_image' not in combined_df.columns:
        combined_df['space_image'] = None

    # Rearrange columns with term ranks first, 'slogan', 'general_info', 'yp_url', and 'space_image' at the end
    cols = combined_df.columns.tolist()
    rank_cols = [col for col in cols if '_rank' in col]
    other_cols = [col for col in cols if col not in rank_cols + ['slogan', 'general_info', 'yp_url', 'space_image']]
    combined_df = combined_df[rank_cols + other_cols + ['slogan', 'general_info', 'yp_url', 'space_image']]

    # Save the combined DataFrame to CSV
    combined_df.to_csv(file_path, index=False)

def concurrent_extraction(urls, session):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(extract, url, session): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                results.append((url, data))
            except Exception as e:
                print(f"Request generated an exception: {e}")
    return results

def generate_urls(cities, queries, pages):
    urls = []
    for city in cities:
        formatted_city = rq.utils.quote(city)
        for query in queries:
            for x in range(1, pages + 1):
                url = f'https://www.yellowpages.com/search?search_terms={query}&geo_location_terms={formatted_city}&page={x}'
                urls.append(url)
    return urls

def main(cities_file, queries_file, pages):
    start_time = time.time()
    cities = read_lines_from_file(cities_file)
    queries = read_lines_from_file(queries_file)
    business_data = {}
    session = rq.Session()

    urls = generate_urls(cities, queries, pages)
    total_iterations = len(urls)
    with tqdm(total=total_iterations, desc="Scraping Progress", unit="city") as pbar:
        articles = concurrent_extraction(urls, session)
        for url, article_data in articles:
            # Extract city and query from URL using urlparse and parse_qsl
            parsed_url = urlparse(url)
            query_dict = dict(parse_qsl(parsed_url.query))
            city = query_dict.get('geo_location_terms', '')
            query = query_dict.get('search_terms', '')

            transform(article_data, city, query, business_data, session)
            pbar.update(1)

    save_to_csv(business_data, 'combined_yp_scrape.csv')

    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print(f"Saved YP scrape to exports/combined_yp_scrape.csv")
    print(f"Total time taken: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

if __name__ == "__main__":
    main('cities.txt', 'queries.txt', 1)
