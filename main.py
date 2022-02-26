from flask import Flask, Response
import subprocess
import io
import os 
from datetime import datetime
from flask import send_file
from dotenv import load_dotenv
import zipfile
import time

# initiate the variable environment
load_dotenv()
data_placed = os.environ['DATA_PLACED']

# call the own function
from web_scraping_food import TokpedScraping
from scrapyfood.scrapyfood.processing import TokpedPostProcessing

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

# scraping single outlet tokped
@app.route("/tokped/<outlet>")
def tokped(outlet):

    # scraping the products urls first
    total_data = TokpedScraping.ingest_tokped_single_outlet(outlet)

    # handling if the products urls is not obtained
    if total_data > 0: 
        # scraping the detail product as a raw data
        process = subprocess.Popen(['python3', 'scrapyfood/scrapyfood/spiders/tokped.py', outlet])
        process.wait()
        # then transform raw data into required csv
        len_data, df_csv = TokpedPostProcessing.transform_product(outlet)
        # download the data into local pc
        return send_file(
            io.BytesIO(df_csv.to_csv(index=False, encoding='utf-8').encode()),
            as_attachment=False,
            download_name=f'products_{outlet}_{dt}.csv',
            mimetype='text/csv')
    
    else:
        return """
            <h2>Failed scraping the outlet</h2>
            Please contact data engineer for more information
        """

@app.route("/tokped_images/<outlet>")
def tokped_images(outlet):
    # delete raw data product detail in here
    TokpedPostProcessing.delete_raw_data(outlet)

    # download images processing
    report = TokpedPostProcessing.download_images(outlet)
    if report != "":
        if data_placed == "LOCAL":
            dir_name = f"data/tokped/images_{outlet}_{dt}.zip"
        else:
            dir_name = f"data/tokped/images_{outlet}_{dt}.zip"
        
        # download the data into local pc
        return send_file(dir_name)
    else:
        return """
            <h2>Failed download the product images</h2>
            Please contact data engineer for more information
        """

@app.route("/tokped_test/<outlet>")
def tokped_test(outlet):
    return "done"

# scraping all outlet shopee
@app.route("/shopee_all_product")
def shopee_all_product():
    process = subprocess.Popen('python3 scrapyfood/scrapyfood/spiders/shopee_all_product.py', shell=True)
    process.wait()
    return "Done"

# @app.route("/shopee_all_seller")
# def shopee_all_seller():
#     process = subprocess.Popen('python3 scrapyfood/scrapyfood/spiders/shopee_all_seller.py', shell=True)
#     process.wait()
#     return "Done"

@app.errorhandler(404)
def page_not_found(e):
    return """
        <h2>The url you were looking for is not found</h2>
        Please contact data engineer for more information
    """

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=2700)