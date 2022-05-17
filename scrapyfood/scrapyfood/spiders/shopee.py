import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd 
from datetime import datetime
import sys
import os
from fake_useragent import UserAgent

class ShopeeSpider(scrapy.Spider):
    name = 'shopee'
    AUTOTHROTTLE_ENABLED = True

    def start_requests(self):
        # scraping detail product from the list of url data
        df_urls = pd.read_csv(f"data/shopee_api_url_products_{self.request}.csv", names=["url"])
        urls = df_urls['url'].to_list()
        for l in urls:
            yield scrapy.Request(url=l, callback=self.parse)

    def parse(self, response):
        # initiate dictionary
        df = {}

        # preprocessing the detail product raw
        raw = response.json()
        # store data to dictionary
        item_id = raw["data"]["itemid"]
        df["item_id"] = item_id
        name_raw = raw["data"]["name"]
        name_encode = name_raw.encode("ascii", "ignore")
        df["name"] = name_encode.decode() 
        shop_id = raw["data"]["shopid"]
        df["shop_id"] = shop_id
        categories_name = []
        categories = raw["data"]["categories"]
        for x in categories:
            categories_name.append(x["display_name"])
        df['category'] = categories_name
        desc_raw = raw["data"]["description"]
        desc_encode = desc_raw.encode("ascii", "ignore")
        df['description'] = str(desc_encode.decode()) 
        df["stock"] = raw["data"]["stock"]
        df['price'] = int(raw["data"]["price"]/100000)
        df["sold"] = raw["data"]["sold"]
        df["images_url"] = "http://cf.shopee.co.id/file/"+raw["data"]["images"][0]
        df["url_api"] = f"https://shopee.co.id/api/v4/item/get?itemid={item_id}&shopid={shop_id}"

        yield df

if __name__ == "__main__":
    # set the data store
    outlet = sys.argv[1]
    # print(outlet)

    # handling for data duplicate or trailing data
    if os.path.exists(f"data/shopee/raw_products_{outlet}.json"):
        os.remove(f"data/shopee/raw_products_{outlet}.json")
    
    uri = f"data/shopee/raw_products_{outlet}.json"

    ua = UserAgent()
    userAgent = ua.random

    process = CrawlerProcess(settings = {
        'FEED_URI' : uri,
        'FEED_FORMAT' : 'json',
        'USER_AGENT' : 'Mozilla/5.0 (Linux; Android 7.0; AGS-L09 Build/HUAWEIAGS-L09; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/59.0.3071.125 Safari/537.36 YandexSearch/7.71/apad YandexSearchBrowser/7.71',
        'LOG_LEVEL' : 'INFO',
        'CONCURRENT_REQUESTS' : 64,
        'REQUEST_CUE' : 100,
        'COOKIES_ENABLED' : False,
        'TEST' : False
    })

    process.crawl(ShopeeSpider, request=outlet)
    process.start()

# try it manual
# open terminal, cd scrapyfood
# run the code below (request=name the outlet want to scrap)
# scrapy crawl shopee -a request=timurasaindonesi -o ../data/shopee/info_products_timurasaindonesi.json