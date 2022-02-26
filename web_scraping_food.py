import pandas as pd
import sys
import os
from dotenv import load_dotenv
import csv
import time
from datetime import datetime
from fake_useragent import UserAgent
from google.cloud import storage
# selenium package with several modules
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException

# initiate the variable environment
load_dotenv()
data_placed = os.environ['DATA_PLACED']
# storage_client = storage.Client.from_service_account_json("")

class TokpedScraping:

    # scraping url products from single outlet
    def ingest_tokped_single_outlet(outlet):
        
        # link tokped as list
        link = [str("https://www.tokopedia.com/"+outlet)]
        
        # initiate the dictionary
        df = {}
        # scraping data will store to csv
        if data_placed == "LOCAL":
            csv_file = open(f'data/url_products_{outlet}.csv', 'w', encoding='utf-8')
        else:
            # csv_file = open(f'data/url_products_{outlet}.csv', 'w', encoding='utf-8')
            storage_client = storage.Client()
            bucket = storage_client.bucket("data_external_backup/website/data")
            # blob = bucket.blob("gs://data_external_backup/website/data")
            blob = bucket.blob(f"url_products_{outlet}.csv")
            blob.upload_from_filename(f"data/url_products_{outlet}.csv")
        writer = csv.writer(csv_file)
        

        # initiate and call the browser for scraping
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        # the setting ignore error/blocker, hopefully
        chrome_options.headless = True 
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-setuid-sandbox")
        ua = UserAgent(verify_ssl=False)
        userAgent = ua.random
        chrome_options.add_argument(f'user-agent={userAgent}')
        # call the browser driver, makesure the driver is already in folder
        driver = webdriver.Chrome("driver/chromedriver", options=chrome_options)

        # scraping url steps
        total_product = 0
        for l in link:
            # access the url
            print(l)
            driver.get(l)
            # the page of url will be more than or equal 1
            i=1
            while i>0:
                # setting the page to capture all contents
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 750);")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 1400);")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 2150);")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 2900);")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                # capture the url
                try:
                    time.sleep(3)
                    # make sure the path is always right to capture the product url
                    product = driver.find_elements_by_xpath('.//div[@class="css-qa82pd"]//div[@class="css-1c4umxf"]//div[@class="pcv3__container css-gfx8z3"]//div[@class="css-zimbi"]//a[@href]')
                    total_product = total_product + len(product)
                    print(len(product))
                    for k in range(0, len(product)):
                        linklist = driver.find_elements_by_xpath('.//div[@class="css-qa82pd"]//div[@class="css-1c4umxf"]//div[@class="pcv3__container css-gfx8z3"]//div[@class="css-zimbi"]//a[@href]')
                        url_product = linklist[k].get_attribute('href')
                        # print(url_w)
                        # store the data to dictionary and csv
                        df['url'] = url_product
                        writer.writerow(df.values())
                    
                except Exception as e:
                    print("Failed to get product url", e)
                    time.sleep(1)
                    pass

                # move to next page
                time.sleep(2)
                try:
                    go_to_page = driver.find_element_by_xpath('.//a[@data-testid="btnShopProductPageNext"]')
                    i = i+1
                    next_page = f'{l}/page/{i}'
                    driver.get(next_page)
                except Exception as e:
                    print("Page has ended")
                    # print(e)
                    i = 0
                    exit

        return total_product