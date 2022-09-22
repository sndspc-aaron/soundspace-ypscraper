import requests
from bs4 import BeautifulSoup
import pandas
import time

# Create input fields, empty list
location = input('City: ')
query = input('Search: ')
pages = int(input('Pages of results: '))+1
main_list = []

# BeautifulSoup args, scraper root element
def extract(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content, 'html.parser')
    return soup.find_all('div', class_='info')

# Scrape tags, transform and append dictionary to main_list
def transform(articles):
    for item in articles:
        try:
            ad_pill = item.find('span', class_='ad-pill').text
        except:
            ad_pill = ''
        if 'Ad' not in ad_pill:
            name = item.find('a', class_='business-name').text
            rank = item.find('h2', class_ ='n').text.split('.')[0]
            try:
                phone = item.find('div', class_ = 'phones phone primary').text
            except: phone = ''
            try:
                address = item.find('div', class_='adr').get_text(separator=', ')
            except: address = ''
            try:
                hours = item.find('div', class_ = 'open-status open').text
            except:
                hours = ''
            try:
                website = item.find('a', class_ = 'track-visit-website')['href']
            except:
                website = ''

            follow_links = [ 
                a['href'] for a in item.find_all('a', class_='business-name')
                if "#" not in a['href']
            ]

            for link in follow_links:
                s = BeautifulSoup(requests.get('http://www.yellowpages.com'+link).content, "lxml")
                try:
                    neighborhood = s.find("dd", class_="neighborhoods").text
                except: 
                    neighborhood = ''
                try:
                    email = s.find('a', class_='email-business').get('href').split(':')[1]
                except:
                    email = ''
                try:
                    extra_phones = s.find("dd", class_="extra-phones").get_text(separator=' ')
                except:
                    extra_phones = ''
                try:
                    social_links = s.find("dd", class_="social-links").get_text(separator=', ')
                except:
                    social_links = ''
                try:
                    categories = s.find("dd", class_="categories").get_text()
                except:
                    categories = ''
                try:
                    other_info = s.find("dd", class_="other-information").get_text(separator=', ')
                except:
                    other_info = ''

            business = {
                'rank': rank,
                'name': name,
                'phone': phone,
                'extra_phones': extra_phones,
                'email': email,
                'address': address,
                'neighborhood': neighborhood,
                'hours': hours,
                'website': website,
                'social_links': social_links,
                'categories': categories,
                'other_info': other_info,
            }
            main_list.append(business)
    return

# Dataframe for main_list, csv export args
def load():
    df = pandas.DataFrame(main_list)
    df.to_csv(f'YPexports/{location}_{query}.csv', index=False)

# Loop for number of pages entered
for x in range(1,pages):
    print(f'Getting page {x}')
    articles = extract(f'https://www.yellowpages.com/search?search_terms={query}&geo_location_terms={location}&page={x}')
    transform(articles)
    time.sleep(2)

load()
print(f'Saved to {location}_{query}.csv')