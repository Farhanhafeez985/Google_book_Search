import csv
import re

import scrapy
from urllib.parse import urlencode
from urllib.parse import urlparse
import json
import pandas as pd

API_KEY = 'Your Api key'


def get_url(url, row):
    payload = {'api_key': API_KEY, 'url': url, 'autoparse': 'true', 'country_code': row['Geo Location']}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


def create_google_url(row, site=''):
    google_dict = {'q': row['Word'], 'num': row['Pages'], }
    if site:
        web = urlparse(site).netloc
        google_dict['as_sitesearch'] = web
        return f'http://{row["Website"]}/search?' + urlencode(google_dict)
    return f'http://{row["Website"]}/search?' + urlencode(google_dict)


class GoogleSpider(scrapy.Spider):
    name = 'book'
    urls = []
    allowed_domains = ['api.scraperapi.com']
    custom_settings = {'ROBOTSTXT_OBEY': False, 'LOG_LEVEL': 'INFO',
                       'CONCURRENT_REQUESTS_PER_DOMAIN': 10,
                       'RETRY_TIMES': 5,
                       'FEED_URI': r'output.xlsx',
                       'FEED_FORMAT': 'xlsx',
                       'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
                       }

    def start_requests(self):
        df = pd.read_excel('..\\googlebook\\google.xlsx', sheet_name=0)
        for index, row in df.iterrows():
            row = row.to_dict()
            url = create_google_url(row)
            yield scrapy.Request(get_url(url, row), callback=self.parse, meta={'row': row})

    def parse(self, response):
        result_json = json.loads(response.text)
        row = response.meta['row']
        for result in result_json['organic_results']:
            link = result['link']
            if re.search(r'(https?:\/\/)?([\w\d]+\.)+' + r'(' + row['Geo Location'] + r')', link):
                item = {'Website': row['Website'], 'Word': row['Word'],
                        'Geo Location': row['Geo Location'], 'link': link}
                yield item

        next_page = result_json['pagination']['nextPageUrl']
        if next_page:
            yield scrapy.Request(get_url(next_page, row), callback=self.parse, meta={'row': row})
