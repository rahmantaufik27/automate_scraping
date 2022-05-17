import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import re
import sys
import os
from os import path

class TokpedSpider(scrapy.Spider):
    name = 'tokped'
    AUTOTHROTTLE_ENABLED = True

    def start_requests(self):
        # scraping detail product from the list of url data
        df_urls = pd.read_csv(f"data/tokped_url_products_{self.request}.csv", names=["url"])
        urls = df_urls['url'].to_list()
        for l in urls:
            yield scrapy.Request(url=l, callback=self.parse)

    def parse(self, response):
        # initiate dictionary
        df = {}

        # preprocessing the detail product raw
        info_raw = response.xpath('//script//text()').getall()
        info_raw = str(info_raw)
        info_raw = info_raw.replace("\\\\n", "")
        info_raw = info_raw.replace("\\\\", " ")
        # makesure the all paths is in correct path/tag/label
        nama = response.xpath('//h1[@data-testid="pdpProductName"]/text()').get()
        # cleaning the text
        shop_name = info_raw
        shop_name = shop_name[shop_name.find('"shopName "'):len(shop_name)]
        shop_name = shop_name[:shop_name.find(',')]
        shop_name = shop_name.split(':')
        shop_name = shop_name[1][1:].replace('"', '')
        cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        desc = info_raw
        desc = desc[desc.find('"Deskripsi ", "subtitle ":'):len(desc)]
        desc = desc[:desc.find('", "applink "')]
        desc = desc.split('": "')
        desc = desc[1]
        desc = re.sub(cleanr, '', desc)
        desc = desc.replace('u002F', '')
        weight = info_raw
        weight = weight[weight.find('"weight "'):len(weight)]
        weight = weight[:weight.find(',')]
        weight = weight.split(':')
        weight_unit = info_raw
        weight_unit = weight_unit[weight_unit.find('"weightUnit "'):len(weight_unit)]
        weight_unit = weight_unit[:weight_unit.find(',')]
        weight_unit = weight_unit.split(':')
        weight = weight[1] + " " + weight_unit[1][1:].replace('"', '') 
        diskon = info_raw
        diskon = diskon[diskon.find('"percentageAmount "'):len(diskon)]
        diskon = diskon[:diskon.find(',')]
        diskon = diskon.split(':') 
        diskon = diskon[1]
        harga1 = info_raw
        harga1 = harga1[harga1.find('"originalPrice "'):len(harga1)]
        harga1 = harga1[:harga1.find(',')]
        harga1 = harga1.split(':')
        harga1 = harga1[1]
        harga2 = response.xpath('//meta[@property="product:price:amount"]/@content').get()  
        etalase = info_raw
        etalase = etalase[etalase.find('"Etalase ", "subtitle ":'):len(etalase)]
        etalase = etalase[:etalase.find('", "applink "')]
        etalase = etalase.split(':')
        etalase = etalase[1][1:].replace('"', '')
        kategori = info_raw
        kategori = kategori[kategori.find('"Kategori ", "subtitle ":'):len(kategori)]
        kategori = kategori[:kategori.find('", "showAtFront "')]
        kategori = kategori.split(':')
        kategori = kategori[3][1:]
        kategori = kategori.split('u002F')
        kategori = kategori[5].replace(' ', '')
        try:
            min_order = info_raw
            min_order = min_order[min_order.find('"minimumOrder "'):len(min_order)]
            min_order = min_order[:min_order.find(',')]
            min_order = min_order.split(':')
            min_order = min_order[1][1:].replace('"', '')
        except:
            # using space and str is caused to adjusting the format from tokped
            min_order = "1 "
        try:
            stock = info_raw
            stock = stock[stock.find('"totalStockFmt "'):len(stock)]
            stock = stock[:stock.find(',')]
            stock = stock.split(':')
            stock = stock[1][1:].replace('"', '')
        except:
            stock = "0 "
        try:
            sold = info_raw
            sold = sold[sold.find('"countSold "'):len(sold)]
            sold = sold[:sold.find(',')]
            sold = sold.split(':')
            sold = sold[1][1:].replace('"', '')
        except:
            sold = "0 "
        raw_image_urls  = response.xpath('//meta[@property="og:image"]/@content').getall()
        images_url = []
        for img_url in raw_image_urls:
            images_url.append(response.urljoin(img_url))
        
        # store data to dictionary
        df['name'] = nama
        df['outlet'] = shop_name
        df['description'] = desc
        df['weight'] = weight
        if int(diskon) == 0:
            df['price'] = harga2
        else:
            df['price'] = harga1
        df['etalase'] = etalase
        df['main_category'] = kategori
        df['min_order'] = min_order
        df['stock'] = stock
        df['sold'] = sold
        df['images_url'] = images_url

        # print(df)
        yield df

# it is used to call the function through website
if __name__ == "__main__":
    # set the data store
    outlet = sys.argv[1]
    # print(outlet)

    # handling for data duplicate or trailing data
    if os.path.exists(f"data/tokped/raw_products_{outlet}.json"):
        os.remove(f"data/tokped/raw_products_{outlet}.json")
    
    uri = f"data/tokped/raw_products_{outlet}.json"

    # crawler setting
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

    # call this crawler
    process.crawl(TokpedSpider, request=outlet)
    process.start()

# try it manual
# open terminal, cd scrapyfood
# run the code below (request=name the outlet want to scrap)
# scrapy crawl tokped -a request=timurasaindonesi -o ../data/tokped/info_products_timurasaindonesi.json