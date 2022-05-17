import pandas as pd
import sys
import os
import csv
import time
import re
from datetime import datetime
from fake_useragent import UserAgent
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
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from scrapyfood.scrapyfood.processing import GeneralPreProcessing

# call the configuration for initiation
local_test = GeneralPreProcessing.access_configuration()

class TokpedScraping:

    # scraping url products from single outlet
    def ingest_url_tokped_single_outlet(outlet):
        
        # link tokped as list
        link = [str("https://www.tokopedia.com/"+outlet)]
        
        # initiate the dictionary
        df = {}
        # scraping data will store to csv
        # cleaning the outlet name first
        outlet_name = re.sub("[^a-zA-Z0-9 ]+", "", outlet)
        csv_file = open(f'data/tokped_url_products_{outlet_name}.csv', 'w', encoding='utf-8')
        writer = csv.writer(csv_file)

        # initiate and call the browser for scraping
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        # the setting ignore error/blocker, hopefully
        # chrome_options.headless = True # comment this line to see the browser running
        chrome_options.add_argument("--headless")
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
        if local_test == "TRUE":
            driver = webdriver.Chrome(executable_path="external_files/chromedriver", options=chrome_options)
        else:
            driver = webdriver.Chrome("/usr/bin/chromedriver", options=chrome_options)

        # scraping url steps
        total_product = 0
        for l in link:
            # access the url
            print(l)
            driver.get(l)
            
            # the page of url will be more than or equal 1
            i=1
            while i>0:
                
                # setting the page to capture all urls
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
                    # if found xpath next page then continue until can't find the xpath
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


class ShopeeScraping:

    # scraping url products from single outlet
    def ingest_url_shopee_single_outlet(outlet):

        # link shopee as list
        link = [str(f"https://shopee.co.id/{outlet}")]
        
        # initiate the dictionary
        df = {}
        # scraping data will store to csv
        # cleaning the outlet name first
        outlet_name = re.sub("[^a-zA-Z0-9 ]+", "", outlet)
        csv_file = open(f'data/shopee_url_products_{outlet_name}.csv', 'w', encoding='utf-8')
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
        if local_test == "TRUE":
            driver = webdriver.Chrome(executable_path="external_files/chromedriver", options=chrome_options)
        else:
            driver = webdriver.Chrome("/usr/bin/chromedriver", options=chrome_options)

        # scraping url steps
        total_product = 0
        for l in link:
            # access the first url
            l1 = f"{l}?page=0&sortBy=pop"
            print(l)
            driver.get(l1)
            
            # the page of url will be more than or equal 1
            i=1
            while i>0:
                
                # setting the page to capture all urls
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 1400);")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # capture the url
                try:
                    time.sleep(3)
                    # make sure the path is always right to capture the product url
                    product = driver.find_elements_by_xpath('.//div[@class="shop-search-result-view__item col-xs-2-4"]//a[@href]')
                    total_product = len(product)
                    print(len(product))
                    for k in range(0, len(product)):
                        product = driver.find_elements_by_xpath('.//div[@class="shop-search-result-view__item col-xs-2-4"]//a[@href]')
                        url_product = product[k].get_attribute('href')
                        df['url'] = url_product
                        writer.writerow(df.values())
                
                except Exception as e:
                    print("Failed to get product url", e)
                    time.sleep(1)
                    pass

                # move to next page
                time.sleep(2)
                try:
                    # if found xpath next page disabled then exit
                    go_to_page = driver.find_element_by_xpath('.//button[@class="shopee-button-outline shopee-mini-page-controller__next-btn shopee-button-outline--disabled"]')
                    print("Page has ended")
                    # print(e)
                    i = 0
                    exit
                except Exception as e:
                    i = i+1
                    next_page = f'{l}?page={i}&sortBy=pop'
                    driver.get(next_page)

        return total_product