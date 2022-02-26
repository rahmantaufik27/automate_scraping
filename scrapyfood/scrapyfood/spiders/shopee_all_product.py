import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd 
from datetime import datetime
import sys
import os
from dotenv import load_dotenv

# initiate the variable environment
load_dotenv()
data_placed = os.environ['DATA_PLACED']

class ShopeeAllProductSpider(scrapy.Spider):
    name = 'shopee_all_product'
    AUTOTHROTTLE_ENABLED = True

    def start_requests(self):
        # scraping detail product from the list of url data
        if data_placed == "LOCAL":
            df_urls = pd.read_csv(f"shopee_url/shopee_url_produk_test.csv", names=["url"])
        else:
            df_urls = pd.read_csv(f"shopee_url/shopee_url_produk_test.csv", names=["url"])
        urls = df_urls['url'].to_list()
        for l in urls:
            yield scrapy.Request(url=l, callback=self.parse)

    def parse(self, response):
        # initiate dictionary
        df = {}

        # preprocessing the detail product raw
        raw = response.json()
        # store data to dictionary
        itemid = raw["data"]["itemid"]
        df["item_id"] = itemid
        name_raw = raw["data"]["name"]
        name_encode = name_raw.encode("ascii", "ignore")
        df["name"] = name_encode.decode() 
        shopid = raw["data"]["shopid"]
        df["shop_id"] = shopid
        categories_name = []
        categories = raw["data"]["categories"]
        for x in categories:
            categories_name.append(x["display_name"])
        df['category'] = categories_name
        desc_raw = raw["data"]["description"]
        desc_encode = desc_raw.encode("ascii", "ignore")
        df['description'] = str(desc_encode.decode()) 
        # attributes = raw["data"]["attributes"]
        df["item_status"] = raw["data"]["item_status"]
        df["is_wholesale"] = raw["data"]["can_use_wholesale"]
        # wholesale_tier_list = raw["data"]["wholesale_tier_list"]
        df["stock"] = raw["data"]["stock"]
        price_min_before_discount = raw["data"]["price_min_before_discount"]
        df["price_min_before_discount"] = int(price_min_before_discount/100000)
        price_max_before_discount = raw["data"]["price_max_before_discount"]
        df["price_max_before_discount"] = int(price_max_before_discount/100000)
        df["discount"] = raw["data"]["discount"]
        price_min = raw["data"]["price_min"]
        df["price_min"] = int(price_min/100000)
        price_max = raw["data"]["price_max"]
        df["price_max"] = int(price_max/100000)
        price = raw["data"]["price"]
        df['price'] = int(price/100000)
        df["historical_sold"] = raw["data"]["historical_sold"]
        df["sold"] = raw["data"]["sold"]
        df["rating"] = raw["data"]["item_rating"]["rating_star"]
        df["review_count"] = raw["data"]["cmt_count"]
        df["like_count"] = raw["data"]["liked_count"]
        df["shop_location"] = raw["data"]["shop_location"]
        df["images_id"] = raw["data"]["images"]
        df["url_api"] = f"https://shopee.co.id/api/v4/item/get?itemid={itemid}&shopid={shopid}"

        yield df

if __name__ == "__main__":
    # set the data store
    dt = datetime.date(datetime.now())
    if data_placed == "LOCAL":
        uri = f"data/shopee/raw_all_products_{dt}.json"
    else:
        uri = f"data/shopee/raw_all_products_{dt}.json"

    process = CrawlerProcess(settings = {
        'FEED_URI' : uri,
        'USER_AGENT' : 'Mozilla/5.0 (Windows 98) AppleWebKit/5360 (KHTML, like Gecko) Chrome/40.0.861.0 Mobile Safari/5360',
        'LOG_LEVEL' : 'INFO',
        'CONCURRENT_REQUESTS' : 64,
        'REQUEST_CUE' : 100,
        'COOKIES_ENABLED' : False,
        'TEST' : False
    })

    process.crawl(ShopeeAllProductSpider)
    process.start()