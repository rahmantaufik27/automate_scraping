# AUTOMATIC SCRAPING BY WEBSITE
## FNB
### Scraping Data through URL Website

<br />

> Run the Scraping Program

1. Scraping data online
    - The data that can be scrap are including products detail and images
    - To scrap product detail, run (your_flask_ip_address)/(market_place)/(outlet_name) in your browser
    - wait the minutes, wait until the data will be download to your PC, the length of time depends on the amount of product that needs to be scraping
    - After the product detail have been downloaded, scraping the product images by (your_flask_ip_address)/(market_place)_images/(outlet_name)
    - currently, only tokped and shopee that can be scraped

<br />

> Run this program locally

1. Install docker
2. Build the image
```sudo docker build -t python-automatic-scraping . ```
3. Run the image
```sudo docker run python-automatic-scraping ```

> Another option to run the program locally without docker, <br />
> it can be used for checking the data scraping result also, but there are things that need to be considered

1. Install the requirements on your environment (env)
2. Activate the enviroment
3. Make sure the directory tree is same as below
```bash
├── data
│   ├── shopee
│       ├── images_files
│   └── tokped
│       ├── images_files
├── external_files
│   ├── chromedriver
│   ├── fd-storage-python.json
│   ├── fd-bigquery-python.json
├── scrapyfood
│   ├── scrapyfood
│       ├── spiders
│           ├── __init__.py
│           ├── shopee_all_product.py
│           ├── shopee_all_seller.py
│           ├── tokped.py
│       ├── items.py
│       ├── middlewares.py
│       ├── pipelines.py
│       ├── processing.py
│       ├── setting.py
├── daily_url
├── .env
├── .gitignore
├── README.md
├── main.py
└── requirements.txt
└── web_scraping_food.py
```
4. Download the chromedriver or another browser driver to run the selenium on your browser (make sure the version and the os are same) and save to external_files directory
5. Download the key json file to access the gcp storage, ask server manager man to get this file, or open this service and get the key from 'add key'
6. Download the url list file to daily_url, but it's conditional for checking the daily scraping function 
7. Run the program
    - run python main.py on terminal
    - open your localhost on browser
    - see the route list on main.py to scraping based on your needs

<br />

> Scraping Test Case

1. Access (your_flask_ip_address)/(market_place)_test/(menu)/(outlet_name)
2. There are several menu in test case including url, raw, product, image in both tokped and shopee test. While in shopee test there is all_product and all_seller menu
3. For example, we want to test scraping product from outlet ajibofficial at shopee, so access (your_flask_ip_address)/shopee_test/product/ajibofficial
4. In addition, there are scraping shopee daily test, access (your_flask_ip_address)/shopee_test/all_product/a or (your_flask_ip_address)/shopee_test/all_seller/a, then check the bigquery and storage for data test result 

<br />

> Cleaning the directory

- There is function can be used for cleaning the directory
- This function is intended to delete all raw files after scraping process, so storage will be lighter and program will be running smoothly
- To access this function, (your_flask_ip_address)/cleaning_data

<br />

> Upload to GCP Storage Schema

1. Access (your_flask_ip_address)/upload_to_storage/(menu)/(market_place)/(outlet_name)
2. There are several menu in upload schema including url, product, and image
3. The market place in url should be filled in, so for example (your_flask_ip_address)/upload_to_storage/image/shopee/ajibofficial
4. In addition, there is url that used storage schema too, which are (your_flask_ip_address)/shopee_all_product and (your_flask_ip_address)/shopee_all_seller, it used to scraping the daily shopee transaction

<br />

> Checking the data

1. You can check the external_data_sp dataset in bigquery and data_external_backup in storage for data test result
2. You can check the directory of the docker also, with this below code

<br />

> Daily scraping

- There is daily scraping in this program which is to scraping shopee product transaction. It run by cloud scheduler (here) with the specified schedule is 9 pm every day, then it stored to external_data_sp dataset in bigquery.