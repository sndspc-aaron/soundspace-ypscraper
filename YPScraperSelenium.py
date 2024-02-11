import aiohttp
import asyncio
import backoff
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime
import os
import logging
from urllib.parse import urlparse, parse_qsl, quote
import config
import time
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('webdriver_manager').setLevel(logging.ERROR)

cache = {}

@backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=5)
async def cached_request(url, session):
    if url not in cache:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                cache[url] = await response.text()
        except aiohttp.ClientError as e:
            logging.error(f"Request error for URL {url}: {e}")
            cache[url] = None  # Ensure cache doesn't store failed attempts
            raise  # Reraise the exception to trigger the backoff
    return cache[url]

def clear_cache():
    global cache
    cache = {}

def read_lines_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = [line.strip() for line in file.readlines()]
            return lines if lines else ["No data in file"]
    except IOError as e:
        print(f"File error: {e}")
        return []

async def extract(url, session):
    try:
        content = await cached_request(url, session)
        if content:
            soup = bs(content, 'lxml')
            return soup.find_all('div', class_='info')
        else:
            logging.warning(f"No content received from URL: {url}")
            return []
    except aiohttp.ClientError as e:
        logging.error(f"Request error for URL {url}: {e}")
        return []

def extract_business_info(item, query):
    business_url = f"{config.DOMAIN}{item.find('a', class_='business-name')['href']}" if item.find('a', class_='business-name') else ''
    rank = item.find('h2', class_='n').text.split('.')[0] if item.find('h2', class_='n') else ''
    return {
        'name': item.find('a', class_='business-name').text if item.find('a', class_='business-name') else '',
        'phone': item.find('div', class_='phones phone primary').text if item.find('div', class_='phones phone primary') else '',
        'address': item.find('div', class_='adr').get_text(separator=', ') if item.find('div', class_='adr') else '',
        'website': item.find('a', class_='track-visit-website')['href'] if item.find('a', class_='track-visit-website') else '',
        'yp_url': business_url,
        f"{query}_rank": rank
    }

async def transform(articles, location, query, business_data, session):
    for item in articles:
        info = extract_business_info(item, query)
        unique_id = (info['name'], info['address'])

        if ',' in location:
            city, state = location.split(', ', 1)
        else:
            city, state = location, ''

        if unique_id in business_data:
            existing_info = business_data[unique_id]
            existing_info.update(info)
            existing_info['city'] = city
            existing_info['state'] = state
            existing_info['search_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            info.update({
                'city': city,
                'state': state,
                'search_datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            business_data[unique_id] = info

        await process_follow_links(item, unique_id, business_data, session)

    return business_data

async def process_follow_links(item, unique_id, business_data, session):
    follow_links = [a['href'] for a in item.find_all('a', class_='business-name') if "#" not in a['href']]
    follow_link_tasks = [process_follow_link(config.DOMAIN + link, unique_id, business_data, session) for link in follow_links]
    await asyncio.gather(*follow_link_tasks)

async def process_follow_link(link, unique_id, business_data, session):
    details_response = await cached_request(link, session)
    if details_response:
        details_page = bs(details_response, "lxml")
        update_business_details(unique_id, business_data, details_page)
        space_image_link = await extract_space_images(details_page, session)
        if space_image_link:
            business_data[unique_id]['space_image'] = space_image_link

async def extract_space_images(details_page, session):
    media_thumbnail_link = details_page.find('a', class_='media-thumbnail collage-pic')
    if media_thumbnail_link:
        gallery_link = config.DOMAIN + media_thumbnail_link['href']
        
        # Corrected WebDriver instantiation
        options = Options()
        options.headless = True  # Run in headless mode
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # Use Selenium to fetch the gallery page
        driver.get(gallery_link)
        content = await asyncio.to_thread(run_selenium, gallery_link)
        driver.quit()
        
        gallery_page = bs(content, 'lxml')
        data_media_links = gallery_page.find_all('a', attrs={'data-media': True})
        image_urls = [link.find('img')['src'] for link in data_media_links if link.find('img')]
        return ', '.join(image_urls)
    return None

def run_selenium(url):
    options = Options()
    options.headless = True
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    content = driver.page_source
    driver.quit()
    return content

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
    base_columns = config.DFCOL_ORDER

    dynamic_columns = set()
    for data in business_data.values():
        dynamic_columns.update(data.keys())

    dynamic_columns.difference_update(base_columns)

    combined_columns = sorted(dynamic_columns) + base_columns
    new_df = pd.DataFrame.from_dict(business_data, orient='index')
    new_df = new_df.reindex(columns=combined_columns)

    if not os.path.exists('exports'):
        os.makedirs('exports')

    file_path = os.path.join(config.EXPORTS_PATH, filename)
    new_df = pd.DataFrame.from_dict(business_data, orient='index')

    new_df = new_df.reindex(columns=combined_columns)

    new_df.to_csv(file_path, index=False)
    
async def concurrent_extraction(urls, session):
    tasks = [asyncio.create_task(extract(url, session)) for url in urls]
    results = await asyncio.gather(*tasks)
    return zip(urls, results)

def generate_urls(cities, queries, pages, domain):
    urls = []
    for city in cities:
        formatted_city = quote(city)
        for query in queries:
            for x in range(1, pages + 1):
                url = f'{domain}/search?search_terms={query}&geo_location_terms={formatted_city}&page={x}'
                urls.append(url)
    return urls
  
async def main():
    clear_cache()
    start_time = time.time()
    cities = read_lines_from_file(config.CITIES_FILE_PATH)
    queries = read_lines_from_file(config.QUERIES_FILE_PATH)
    
    business_data = {}
    
    async with aiohttp.ClientSession() as session:
        headers = {
            'User-Agent': config.USER_AGENT
        }
        session.headers.update(headers)

        urls = generate_urls(cities, queries, config.PAGE_LIMIT, config.DOMAIN)
        chunks = [urls[i:i + config.CONCURRENT_REQUESTS] for i in range(0, len(urls), config.CONCURRENT_REQUESTS)]

        with tqdm(total=len(urls), desc="Scraping Progress", unit="pages") as pbar:
            for chunk in chunks:
                tasks = [asyncio.create_task(process_url(url, session, business_data)) for url in chunk]
                await asyncio.gather(*tasks)
                pbar.update(len(chunk))

    end_time = time.time()
    end_time_str = time.strftime('%m_%d_%Y_%H-%M-%S', time.localtime(end_time))
    save_to_csv(business_data, f'ypscrape_{end_time_str}.csv')

    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    logging.info(f"Saved results to {config.EXPORTS_PATH}/ypscrape_{end_time_str}.csv")
    logging.info(f"Total time taken: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

async def process_url(url, session, business_data):
    # logging.info(f"Processing URL: {url}")
    article_data = await extract(url, session)
    parsed_url = urlparse(url)
    query_dict = dict(parse_qsl(parsed_url.query))
    city = query_dict.get('geo_location_terms', '')
    query = query_dict.get('search_terms', '')

    await transform(article_data, city, query, business_data, session)

if __name__ == "__main__":
    asyncio.run(main())
