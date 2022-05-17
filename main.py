from flask import Flask, Response
import subprocess
import io
import os 
import re
from datetime import datetime
from flask import send_file
import zipfile
import time
from google.cloud import storage

# call the own function
from web_scraping_food import TokpedScraping, ShopeeScraping
from scrapyfood.scrapyfood.processing import TokpedPostProcessing, ShopeePostProcessing, GeneralPostProcessing, GeneralPreProcessing

# call the configuration for initiation
local_test = GeneralPreProcessing.access_configuration()
client_name, bucket_name = GeneralPreProcessing.access_storage_configuration()
bq_project_id, bq_table_product, bq_table_seller = GeneralPreProcessing.access_bq_configuration()
cs_storage, cs_bq = GeneralPreProcessing.access_credential_local()

# running app using web
app = Flask(__name__)

# set the current date
dt = datetime.date(datetime.now())

# homepage
@app.route("/")
def welcome():
    return """
        <br/><br/><br/><br/><br/>
        <center>
        <h1>WELCOME TO SCRAPING APP BY FOOD.ID</h1> 
        <h3>This App is used for scraping food and beverage products in several marketplace </h3>
        Contact data engineer if there is an error
        <br/><br/>Enjoy ;)
        </center>
    """

# scraping detail product by single outlet
@app.route("/tokped/<outlet>")
def tokped(outlet):

    # scraping the products urls first
    total_data = TokpedScraping.ingest_url_tokped_single_outlet(outlet)

    # cleaning the outlet name, to ignore special case such as . / in outlet name
    outlet = re.sub("[^a-zA-Z0-9 ]+", "", outlet)

    # handling if the products urls is not obtained
    if total_data > 0: 
        # scraping the detail products as raw data
        process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/tokped.py', outlet])
        process.wait()
        # then transform raw data into required csv
        len_data, df_csv = TokpedPostProcessing.transform_product(outlet)
        # download the data into local pc
        return send_file(
            io.BytesIO(df_csv.to_csv(index=False, encoding='utf-8').encode()),
            as_attachment=False,
            download_name=f'products_{outlet}.csv',
            mimetype='text/csv')
    
    else:
        return """
            <h2>ailed scraping the outlet of tokped due to invalid api url</h2>
            Please contact data engineer for more information
        """

# scraping images product by single outlet
@app.route("/tokped_images/<outlet>")
def tokped_images(outlet):

    # cleaning the outlet name
    outlet = re.sub("[^a-zA-Z0-9 ]+", "", outlet)
    
    # download images processing
    report = TokpedPostProcessing.download_images(outlet)
    if report != "":
        dir_name = f"data/tokped/images_{outlet}.zip"
        
        # download the data into local pc
        return send_file(dir_name)
    else:
        return """
            <h2>Failed download the product images</h2>
            Please contact data engineer for more information
        """

# scraping test/check the functions
@app.route("/tokped_test/<menu>/<outlet>")
def tokped_test(menu=None, outlet=None):
    # handle invalid url
    is_invalid = 0

    if menu=="url":
        total_data = TokpedScraping.ingest_url_tokped_single_outlet(outlet)
    elif menu=="raw":
        process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/tokped.py', outlet])
        process.wait()
    elif menu=="product":
        len_data, df_csv = TokpedPostProcessing.transform_product(outlet)
    elif menu=="image":
        report = TokpedPostProcessing.download_images(outlet)
    else:
        is_invalid = 1

    if is_invalid == 0:
        return "done"
    else:
        return """
            <h2>The url you were looking for is not found</h2>
            Please contact data engineer for more information
        """

@app.route("/shopee_test/<menu>/<outlet>")
def shopee_test(menu=None, outlet=None):
    # handle invalid url
    is_invalid = 0

    if menu=="url":
        total_data = ShopeeScraping.ingest_url_shopee_single_outlet(outlet)
        res = ShopeePostProcessing.transform_url(outlet)
    elif menu=="raw":
        process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee.py', outlet])
        process.wait()
    elif menu=="product":
        len_data, df_csv = ShopeePostProcessing.transform_product(outlet)
    elif menu=="image":
        report = ShopeePostProcessing.download_images(outlet)
    elif menu=="all_product":
        # set the name_type as a test, so it will download test product url, it just used for testing
        name_type = "test"
        batch = 0
        process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, batch])
        process.wait()
        ShopeePostProcessing.upload_product_to_storage_bigquery(name_type)
        time.sleep(2)
        GeneralPostProcessing.delete_all_data()
    elif menu=="all_seller":
        name_type = "test"
        process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_seller.py', name_type, client_name, bucket_name])
        process.wait()
        ShopeePostProcessing.upload_seller_to_storage_bigquery(name_type)
        time.sleep(2)
        GeneralPostProcessing.delete_all_data()
    else:
        is_invalid = 1
    
    if is_invalid == 0:
        return "done"
    else:
        return """
            <h2>The url you were looking for is not found</h2>
            Please contact data engineer for more information
        """

# scraping detail product by single outlet
@app.route("/shopee/<outlet>")
def shopee(outlet):

    # scraping the products urls first
    total_data = ShopeeScraping.ingest_url_shopee_single_outlet(outlet)

    # cleaning the outlet name, to ignore special case such as . / in outlet name
    outlet = re.sub("[^a-zA-Z0-9 ]+", "", outlet)

    # handling if the products urls is not obtained
    if total_data > 0:
        # to get the detail of detail products, transform the url into api first
        res = ShopeePostProcessing.transform_url(outlet)
        # if the api url more than 1 then process scraping
        if res > 1:
            # scraping the detail products as raw data
            process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee.py', outlet])
            process.wait()
            # then transform raw data into required csv
            len_data, df_csv = ShopeePostProcessing.transform_product(outlet)
            # download the data into local pc
            return send_file(
                io.BytesIO(df_csv.to_csv(index=False, encoding='utf-8').encode()),
                as_attachment=False,
                download_name=f'products_{outlet}.csv',
                mimetype='text/csv')
        else:
            return """
                <h2>Failed scraping the outlet of shopee due to invalid api url</h2>
                Please contact data engineer for more information
            """
    else:
        return """
            <h2>Failed scraping the outlet of shopee due to url invalid</h2>
            Please contact data engineer for more information
        """

# scraping images product by single outlet
@app.route("/shopee_images/<outlet>")
def shopee_images(outlet):

    # cleaning the outlet name
    outlet = re.sub("[^a-zA-Z0-9 ]+", "", outlet)
    
    # download images processing
    report = ShopeePostProcessing.download_images(outlet)
    if report != "":
        dir_name = f"data/shopee/images_{outlet}.zip"
        
        # download the data into local pc
        return send_file(dir_name)
    else:
        return """
            <h2>Failed download the product images</h2>
            Please contact data engineer for more information
        """

# scraping all fnb product shopee
@app.route("/shopee_all_product")
def shopee_all_product():
    
    # set the name_type as a all, and it will download all product url
    name_type = "all"
    # process scraping data with the params configuration value
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "a"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "b"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "c"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "d"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "e"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "f"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "g"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "h"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "i"])
    # process.wait()
    # process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, "j"])
    # process.wait()

    for i in range(0, 99):
        j = str(i)
        process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_product.py', name_type, client_name, bucket_name, j])
        process.wait()

    # process for uploading to gcp storage and bigquery
    # ShopeePostProcessing.upload_product_to_storage_bigquery(name_type)
    # then, waiting and cleaning the data
    # time.sleep(10)
    # GeneralPostProcessing.delete_all_data()
    return "Done"

# scraping all fnb seller shopee
@app.route("/shopee_all_seller")
def shopee_all_seller():
    
    name_type = "all"
    # process scraping data with the params configuration values
    process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/shopee_all_seller.py', name_type, client_name, bucket_name])
    process.wait()
    # process for uploading to gcp storage and bigquery
    ShopeePostProcessing.upload_seller_to_storage_bigquery(name_type)
    # then, waiting and cleaning the data
    time.sleep(10)
    GeneralPostProcessing.delete_all_data()
    return "Done"

@app.errorhandler(404)
def page_not_found(e):
    return """
        <h2>The url you were looking for is not found</h2>
        Please contact data engineer for more information
    """

# try to upload from local to gcp storage
## for example /upload_to_storage/image/shopee/ajibofficial
@app.route("/upload_to_storage/<menu>/<mp>/<outlet>")
def upload_to_gcp_storage(menu=None, mp=None, outlet=None):

    if menu=="url":
        storage_client = storage.Client(client_name)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(f"website/data/{mp}_url_products_{outlet}.csv")
        blob.upload_from_filename(f"data/{mp}_url_products_{outlet}.csv")
        print("success upload url")
    elif menu=="product":
        storage_client = storage.Client(client_name)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(f"website/data/{mp}/products_{outlet}.csv")
        blob.upload_from_filename(f"data/{mp}/products_{outlet}.csv")
        print("success upload product")
    elif menu=="image":
        storage_client = storage.Client(client_name)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(f"website/data/{mp}/images_files/images_{outlet}.zip")
        blob.upload_from_filename(f"data/{mp}/images_{outlet}_{dt}.zip")
        print("success upload url images")
    else:
        print("failed upload")

    return "Done"

@app.route("/cleaning_data")
def cleaning_data():
    GeneralPostProcessing.delete_all_data()
    return "Done"

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)