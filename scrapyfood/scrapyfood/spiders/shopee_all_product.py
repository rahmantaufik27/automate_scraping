import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd 
from datetime import datetime
import sys
import os
from google.cloud import storage
from google.oauth2 import service_account
import time
from fake_useragent import UserAgent
# from dotenv import load_dotenv
# load_dotenv()

# proxy_api_key = os.environ['SCRAPER_API_SECRET']

class ShopeeAllProductSpider(scrapy.Spider):
    name = 'shopee_all_product'
    # AUTOTHROTTLE_ENABLED = True

    def start_requests(self):

        # initiate the configuration
        cs = service_account.Credentials.from_service_account_file("external_files/fd-storage-python.json")
        storage_client = storage.Client(self.client_name, credentials=cs)
        bucket = storage_client.get_bucket(self.bucket_name)
        # set the file
        if self.name_type == "test":
            if not os.path.exists(f"daily_url/shopee_url_product_test.csv"):
                blob = bucket.blob(f"website/shopee_url/shopee_url_product_test.csv")
                blob.download_to_filename(f"daily_url/shopee_url_product_test.csv")
                time.sleep(2)
            df_urls = pd.read_csv(f"daily_url/shopee_url_product_test.csv", names=["url"])
        else:
            if not os.path.exists(f"daily_url/shopee_url_product_all.csv"):
                blob = bucket.blob(f"website/shopee_url/shopee_url_product_all.csv")
                blob.download_to_filename(f"daily_url/shopee_url_product_all.csv")
                time.sleep(2)
            df_urls_all = pd.read_csv(f"daily_url/shopee_url_product_all.csv", names=["url"])

            if batch != 99:
                batch_number = 10000*int(batch)
                batch_number_2 = batch_number+10000
                df_urls = df_urls_all[batch_number:batch_number_2]
            else:
                df_urls = df_urls_all[990000:]

            # if batch=="a":
            #     df_urls = df_urls_all[0:100000]
            # elif batch=="b":
            #     df_urls = df_urls_all[100000:200000]
            # elif batch=="c":
            #     df_urls = df_urls_all[200000:300000]
            # elif batch=="d":
            #     df_urls = df_urls_all[300000:400000]
            # elif batch=="e":
            #     df_urls = df_urls_all[400000:500000]
            # elif batch=="f":
            #     df_urls = df_urls_all[500000:600000]
            # elif batch=="g":
            #     df_urls = df_urls_all[600000:700000]
            # elif batch=="h":
            #     df_urls = df_urls_all[700000:800000]
            # elif batch=="i":
            #     df_urls = df_urls_all[800000:900000]
            # else:
            #     df_urls = df_urls_all[900000:]


        urls = df_urls['url'].to_list()
        for l in urls:
            # l = f"https://api.scraperapi.com/?api_key={proxy_api_key}&url={l}&keep_headers=true"
            yield scrapy.Request(url=l, callback=self.parse)

    def parse(self, response):
        # initiate dictionary
        df = {}

        # preprocessing the detail product raw
        raw = response.json()
        # store data to dictionary
        item_id = raw["data"]["itemid"]
        df["item_id"] = int(item_id)
        name_raw = raw["data"]["name"]
        name_encode = name_raw.encode("ascii", "ignore")
        df["name"] = name_encode.decode() 
        shop_id = raw["data"]["shopid"]
        df["shop_id"] = int(shop_id)
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
        df["url_api"] = f"https://shopee.co.id/api/v4/item/get?itemid={item_id}&shopid={shop_id}"

        yield df

if __name__ == "__main__":
    # get and set the configuration
    name_type = sys.argv[1]
    client_name = sys.argv[2]
    bucket_name = sys.argv[3]
    batch = sys.argv[4]
    dt = datetime.date(datetime.now())

    # handling for data duplicate or trailing data
    if os.path.exists(f"data/shopee/raw_all_products_{dt}.json"):
        os.remove(f"data/shopee/raw_all_products_{dt}.json")

    # uri = f"data/shopee/raw_all_products_{dt}.json"
    # if batch=="a":
    #     uri = f"data/shopee/raw_all_products_1.json"
    # elif batch=="b":
    #     uri = f"data/shopee/raw_all_products_2.json"
    # elif batch=="c":
    #     uri = f"data/shopee/raw_all_products_3.json"
    # elif batch=="d":
    #     uri = f"data/shopee/raw_all_products_4.json"
    # elif batch=="e":
    #     uri = f"data/shopee/raw_all_products_5.json"
    # elif batch=="f":
    #     uri = f"data/shopee/raw_all_products_6.json"
    # elif batch=="g":
    #     uri = f"data/shopee/raw_all_products_7.json"
    # elif batch=="h":
    #     uri = f"data/shopee/raw_all_products_8.json"
    # elif batch=="i":
    #     uri = f"data/shopee/raw_all_products_9.json"
    # else:
    #     uri = f"data/shopee/raw_all_products_10.json"
    
    uri = f"data/shopee/raw_all_products_{batch}.json"

    ua = UserAgent(verify_ssl=False)
    user_agent = ua.random
    print("User Agent: ",user_agent)

    process = CrawlerProcess(settings = {
        'FEED_URI' : uri,
        'FEED_FORMAT' : 'json',
        # 'USER_AGENT' : 'Mozilla/5.0 (Linux; Android 7.0; AGS-L09 Build/HUAWEIAGS-L09; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/59.0.3071.125 Safari/537.36 YandexSearch/7.71/apad YandexSearchBrowser/7.71',
        # 'USER_AGENT' : 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'LOG_LEVEL' : 'INFO',
        'CONCURRENT_REQUESTS' : 1000,
        # 'REQUEST_CUE' : 100,
        'COOKIES_ENABLED' : False,
        'TEST' : False,
        'AUTOTHROTTLE_ENABLED' : False,
        # 'AUTOTHROTTLE_START_DELAY' : 3,
        # 'AUTOTHROTTLE_MAX_DELAY' : 30,
        # 'DOWNLOAD_DELAY' : 3,
        # 'CONCURRENT_ITEMS' : 500,
        # 'REACTOR_THREADPOOL_MAXSIZE' : 20,
        # 'RETRY_ENABLED' : False,
        # 'DOWNLOAD_TIMEOUT' : 15,
        # 'REDIRECT_ENABLED' : False,
        # 'AUTOTHROTTLE_TARGET_CONCURRENCY' : 1.0,
        # 'ROBOTSTXT_OBEY' : False,
        # 'TELNETCONSOLE_ENABLED': False,
        'RETRY_TIMES' : 10,
        'RETRY_HTTP_CODES' : [500, 503, 504, 400, 403, 404, 408],
        # 'PROXY_LIST' : '/home/rt/Desktop/WORK/SCRAPING_P/daily_url/proxy_list.txt',
        'PROXY_LIST' : '/usr/src/app/external_files/proxy_list.txt',
        'DOWNLOADER_MIDDLEWARES' : {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 900,
            'scrapy_proxies.RandomProxy': 900,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 900,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 900,
        },
        'PROXY_MODE' : 0,
    })

    process.crawl(ShopeeAllProductSpider, name_type=name_type, client_name=client_name, bucket_name=bucket_name, batch=batch)
    process.start()

# try it manual
# open terminal, cd scrapyfood
# run the code below
# scrapy crawl shopee_all_product -a local_test=(local_test) client_name=(client_name) bucket_name=(bucket_name) credentials_storage=(credentials_storage), name_type=(name_type) -o ../data/tokped/info_products_timurasaindonesi.json
# set the value of local_test, client_name and the others params to run the scrapy crawl