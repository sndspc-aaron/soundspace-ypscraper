# Configuration for web scraping
PAGE_LIMIT = 2
DOMAIN = 'https://www.yellowpages.com'
CONCURRENT_REQUESTS = 40
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"

# File paths
CITIES_FILE_PATH = 'data/cities.txt'
QUERIES_FILE_PATH = 'data/queries.txt'
EXPORTS_PATH = 'exports/'

# Configuration for data frame columns
DFCOL_ORDER = ['name', 'phone', 'address', 'website', 'yp_url', 'city', 'state', 'search_datetime', 'slogan', 'general_info', 'neighborhood', 'email', 'extra_phones', 'social_links', 'categories', 'hour_category', 'other_info', 'detailed_hours', 'space_image']
