import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd 
from datetime import datetime
import sys
import os
from google.cloud import storage
from google.oauth2 import service_account
import time

class ShopeeAllSellerSpider(scrapy.Spider):
    name = 'shopee_all_seller'
    AUTOTHROTTLE_ENABLED = True

    def start_requests(self):

        # initiate the configuration
        cs = service_account.Credentials.from_service_account_file("external_files/fd-storage-python.json")
        storage_client = storage.Client(self.client_name, credentials=cs)
        bucket = storage_client.get_bucket(self.bucket_name)
        # set the file
        if self.name_type == "test":
            if not os.path.exists(f"daily_url/shopee_url_seller_test.csv"):
                blob = bucket.blob(f"website/shopee_url/shopee_url_seller_test.csv")
                blob.download_to_filename(f"daily_url/shopee_url_seller_test.csv")
                time.sleep(2)
            df_urls = pd.read_csv(f"daily_url/shopee_url_seller_test.csv", names=["url"])
        else:
            if not os.path.exists(f"daily_url/shopee_url_seller_all.csv"):
                blob = bucket.blob(f"website/shopee_url/shopee_url_seller_all.csv")
                blob.download_to_filename(f"daily_url/shopee_url_seller_all.csv")
                time.sleep(2)
            df_urls = pd.read_csv(f"daily_url/shopee_url_seller_all.csv", names=["url"])

        urls = df_urls['url'].to_list()
        for l in urls:
            yield scrapy.Request(url=l, callback=self.parse)

    def parse(self, response):
        # initiate dictionary
        df = {}

        # preprocessing the detail seller raw
        raw = response.json()
        # store data to dictionary
        shop_id = raw["data"]["shopid"]
        df["shop_id"] = int(shop_id)
        name_raw = raw["data"]["name"]
        name_encode = name_raw.encode("ascii", "ignore")
        df["outlet_name"] = name_encode.decode()
        df["is_official_shop"] = raw["data"]["is_official_shop"]
        df["item_count"] = raw["data"]["item_count"]
        df["follower_count"] = raw["data"]["follower_count"]
        df["chat_performance"] = raw["data"]["response_rate"]
        df["chat_count"] = raw["data"]["response_time"]
        df["rating_good"] = raw["data"]["rating_good"]
        df["rating_normal"] = raw["data"]["rating_normal"]
        df["rating_bad"] = raw["data"]["rating_bad"]
        df["rating_star"] = raw["data"]["rating_star"]
        df["shop_location"] = raw["data"]["shop_location"]
        df["detail_location"] = raw["data"]["place"]
        df["url_api"] = f"https://shopee.co.id/api/v4/seller/get_shop_info?shopid={shop_id}"

        yield df

if __name__ == "__main__":
    # get and set the configuration 
    name_type = sys.argv[1]
    client_name = sys.argv[2]
    bucket_name = sys.argv[3]
    dt = datetime.date(datetime.now())

    # handling for data duplicate or trailing data
    if os.path.exists(f"data/shopee/raw_all_sellers_{dt}.json"):
        os.remove(f"data/shopee/raw_all_sellers_{dt}.json")

    uri = f"data/shopee/raw_all_sellers_{dt}.json"

    process = CrawlerProcess(settings = {
        'FEED_URI' : uri,
        'FEED_FORMAT' : 'json',
        'USER_AGENT' : 'Mozilla/5.0 (Windows 98) AppleWebKit/5360 (KHTML, like Gecko) Chrome/40.0.861.0 Mobile Safari/5360',
        'LOG_LEVEL' : 'INFO',
        'CONCURRENT_REQUESTS' : 64,
        'REQUEST_CUE' : 100,
        'COOKIES_ENABLED' : False,
        'TEST' : False
    })

    process.crawl(ShopeeAllSellerSpider, name_type=name_type, client_name=client_name, bucket_name=bucket_name)
    process.start()