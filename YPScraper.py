import requests as rq 
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import datetime;

# Input fields, empty list for scrape results
location = input('City: ')
query = input('Search: ')
pages = int(input('Max pages of results: '))+1
main_list = []

# Store current time
ct = datetime.datetime.now()
ts = ct.timestamp()

# BeautifulSoup args, scraper root element
def extract(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}
    r = rq.get(url, headers=headers)
    soup = bs(r.content, 'html.parser')
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
                s = bs(rq.get('http://www.yellowpages.com'+link).content, "lxml")
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
                'name': name,
                'email': email,
                'phone': phone,
                'website': website,
                'address': address,
                'YPrank': rank,
                'YPneighborhood': neighborhood,
                'YPextra_phones': extra_phones,
                'YPhours': hours,
                'YPsocial_links': social_links,
                'YPcategories': categories,
                'YPother_info': other_info,
                'YPsearch_location': location,
                'YPsearch_term': query,
                'TPsearch_datetime': ts,
            }
            main_list.append(business)
    return

# Dataframe for main_list, csv export args
def load():
    df = pd.DataFrame(main_list)
    df.to_csv(f'exports/yp_{location}_{query}.csv', index=False)

# Loop for number of pages entered
for x in range(1,pages):
    print(f'Getting page {x}')
    articles = extract(f'https://www.yellowpages.com/search?search_terms={query}&geo_location_terms={location}&page={x}')
    transform(articles)
    time.sleep(2)

load()
print(f'Saved YP scrape to exports/yp_{location}_{query}.csv')
