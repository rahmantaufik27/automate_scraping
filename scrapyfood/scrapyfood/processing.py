import pandas as pd
import re 
import os
from os import path
import sys
from datetime import datetime
import shutil
import requests
import json
from google.cloud import storage, bigquery
from google.oauth2 import service_account


# preprocessing to initiate the variable configurations and the credentials
class GeneralPreProcessing:
    def access_configuration():
        # if you want test in local without docker run, then local_test = "TRUE"
        local_test = "FALSE"
        return local_test

    # gcp storage initiation
    def access_storage_configuration():
        client_name = "FOOD-ID"
        bucket_name = "data_external_backup"
        return client_name, bucket_name

    # gcp bigquery initiation
    def access_bq_configuration():
        bq_project_id = "food-id-app"
        bq_table_product = "external_data_sp.EXTERNAL_PRODUCTS"
        bq_table_seller = "external_data_sp.EXTERNAL_SHOPS"
        return bq_project_id, bq_table_product, bq_table_seller
    
    # gcp access key initiation
    def access_credential_local():
        credentials_storage = "external_files/fd-storage-python.json"
        credentials_bq = "external_files/fd-bigquery-python.json"
        return credentials_storage, credentials_bq

# post processing for tokped scraping
class TokpedPostProcessing:

    # transform data raw products into csv
    def transform_product(self):
        
        # get the data from json raw
        path = f"data/tokped/raw_products_{self}.json"

        # store to dataframe
        df_raw = pd.read_json(path)

        # filter products based on category fnb
        # store the first length of raw data
        len1 = len(df_raw)
        # make sure to check the category in tokped up to date
        cat_fnb = ['sayur-buah', 'sayur', 'buah', 'protein-telur', 'susu-olahan-susu', 'telur', 'seafood', 'daging', 'minuman', 'tepung', 'beras', 'bumbu-bahan-masakan', 'makanan-jadi', 'produk-susu', 'bahan-kue', 'kopi', 'mie-pasta', 'makanan-ringan', 'paket-parcel-makanan', 'kue', 'makanan-beku', 'makanan-kaleng', 'makanan-kering', 'makanan-sarapan', 'teh', 'makanan-susu-bayi', 'makanan-susu-ibu-hamil', '']
        # update the dataframe
        df_raw = df_raw.loc[df_raw.main_category.isin(cat_fnb)]
        # store the length after filter by category
        len2 = len(df_raw)

        # transform to required csv
        clm = ["name", "category_id", "category_name", "price", "description", "fullfilment", "min_quantity", "stock", "sku", "weight", "delivery_area_id", "delivery_area_name", "delivery_partner_service_id", "delivery_partner_service_name", "storefront_name", "etalase", "images_url", "sold"]
        df_csv = pd.DataFrame(columns=clm)
        df_csv = pd.DataFrame(columns=clm)
        df_csv['name'] = df_raw['name'].map(lambda x: x.replace('/',''))
        df_csv['name'] = df_csv['name'].map(lambda x: x.replace('.',''))
        df_csv['price'] = df_raw['price']
        df_csv['description'] = df_raw['description'].str[:-1]
        df_csv['stock'] = df_raw['stock'].map(lambda x: x.replace(' ',''))
        df_csv['weight'] = df_raw['weight'].str[:-1]
        df_csv['storefront_name'] = df_raw['outlet'].str[:-1]
        df_csv['min_quantity'] = df_raw['min_order']
        df_csv['etalase'] = df_raw['etalase'].str[:-1]
        df_csv['category_name'] = df_raw['main_category'].map(lambda x: x.replace('-',' '))
        df_csv['category_name'] = df_csv['category_name'].str.title()
        df_csv['images_url'] = df_raw['images_url'].map(lambda x: x[0])
        df_csv['sold'] = df_raw['sold']
        df_csv.to_csv(f"data/tokped/products_{self}.csv", index=False)

        # return the length of data and the data itself
        len_data = len2            
        return len_data, df_csv
        
    # def delete_raw_data(self):
    #     # delete the raw file
    #     if os.path.exists(f"data/tokped/raw_products_{self}.json"):
    #         os.remove(f"data/tokped/raw_products_{self}.json")
    #     else:
    #         print("cannot remove")
    
    def download_images(self):
        # delete the old images files first
        if os.path.exists(f"data/tokped/images_files"):
            shutil.rmtree(f"data/tokped/images_files")
            path = os.path.join("data/tokped","images_files")
            os.mkdir(path)
            is_removed = 1
        else:
            print("cannot remove")
            is_removed = 0
        
        # then if success download the products images
        if is_removed == 1:
            # get the images url from csv raw
            df = pd.read_csv(f"data/tokped/products_{self}.csv")
            # filter to get only alphanumeric
            df["name"] = df["name"].str.replace(r'[^a-zA-Z0-9 ]+', '')

            # download files based on images url
            for u, n in zip(df["images_url"], df["name"]):
                response = requests.get(u)
                n = f"data/tokped/images_files/{self}_{n}.jpg"
                file = open(n, "wb")
                file.write(response.content)
                file.close()

            # store the data as zip and put to directory
            dt = datetime.date(datetime.now())
            dir_name = "data/tokped/images_files"
            output_filename = f"data/tokped/images_{self}"

            # return the successfull report
            report_zip = shutil.make_archive(output_filename, 'zip', dir_name)
        else:
            report_zip = ""
        
        return report_zip


# post processing for shopee scraping
class ShopeePostProcessing:

    # transform url shopee into api url
    def transform_url(self):

        # get the data from csv
        path = f"data/shopee_url_products_{self}.csv"

        # store the url of csv into dataframe
        df = pd.read_csv(path, names=["url"])
        df["code"] = df["url"].str.split('-i.').str[-1]
        df["code"] = df["code"].str.split('?').str[0]
        df["code_shopid"] = df["code"].str.split('.').str[0]
        df["code_itemid"] = df["code"].str.split('.').str[-1]

        # transform basic url into api url
        def transform_api(shopid, itemid):
            url = f"https://shopee.co.id/api/v4/item/get?itemid={itemid}&shopid={shopid}"
            return url
        
        # convert the api url list into csv
        res = 0
        df["api_url"] = df.apply(lambda x:transform_api(x.code_shopid, x.code_itemid), axis=1)
        df["api_url"].to_csv(f"data/shopee_api_url_products_{self}.csv", header=False, index=False)
        res = len(df["api_url"])

        return res

    # transform data raw products into csv
    def transform_product(self):
        
        # get the data from json raw
        path = f"data/shopee/raw_products_{self}.json"

        # store to dataframe
        df_raw = pd.read_json(path)

        # because the categories in shopee is confused, so to filter the category must be manual, sorry

        # convert to csv
        ## weight, storefront name, min quantity, etalase are not found
        ## so for storefront name is filled in by outlet name in url, min quantity is filled in by 1 (default), and etalase is filled in by category
        clm = ["name", "category_id", "category_name", "price", "description", "fullfilment", "min_quantity", "stock", "sku", "weight", "delivery_area_id", "delivery_area_name", "delivery_partner_service_id", "delivery_partner_service_name", "storefront_name", "etalase", "images_url"]
        df_csv = pd.DataFrame(columns=clm)
        df_csv['name'] = df_raw['name']
        df_csv['price'] = df_raw['price']
        df_csv['description'] = df_raw['description']
        df_csv['stock'] = df_raw['stock']
        # df_csv['weight'] = df_raw['weight']
        df_csv['storefront_name'] = f"{self}"
        df_csv['min_quantity'] = 1
        df_csv['etalase'] = df_raw['category']
        df_csv['category_name'] = df_raw['category']
        df_csv['images_url'] = df_raw['images_url']
        df_csv['sold'] = df_raw['sold']
        df_csv.to_csv(f"data/shopee/products_{self}.csv", index=False)

        # return the length of data and the data itself
        len_data = len(df_csv)            
        return len_data, df_csv

    def download_images(self):
        # delete the old images files first
        if os.path.exists(f"data/shopee/images_files"):
            shutil.rmtree(f"data/shopee/images_files")
            path = os.path.join("data/shopee","images_files")
            os.mkdir(path)
            is_removed = 1
        else:
            print("cannot remove")
            is_removed = 0

        # then if success download the products images
        if is_removed == 1:
            # get the images url from csv raw
            df = pd.read_csv(f"data/shopee/products_{self}.csv")
            # filter to get only alphanumeric
            df["name"] = df["name"].str.replace(r'[^a-zA-Z0-9 ]+', '')

            # download files based on images url
            for u, n in zip(df["images_url"], df["name"]):
                response = requests.get(u)
                n = f"data/shopee/images_files/{self}_{n}.jpg"
                file = open(n, "wb")
                file.write(response.content)
                file.close()

            # store the data as zip and put to directory
            dt = datetime.date(datetime.now())
            dir_name = "data/shopee/images_files"
            output_filename = f"data/shopee/images_{self}"

            # return the successfull report
            report_zip = shutil.make_archive(output_filename, 'zip', dir_name)
        else:
            report_zip = ""

        return report_zip

    def upload_product_to_storage_bigquery(self):
        # call preprocessing
        local_test = GeneralPreProcessing.access_configuration()
        client_name, bucket_name = GeneralPreProcessing.access_storage_configuration()
        bq_project_id, bq_table_product, bq_table_seller = GeneralPreProcessing.access_bq_configuration()
        credentials_storage, credentials_bq = GeneralPreProcessing.access_credential_local()
        ac_storage = service_account.Credentials.from_service_account_file(credentials_storage)
        ac_bq = service_account.Credentials.from_service_account_file(credentials_bq)
        
        # initiate the datetime for file/table name
        dt = datetime.date(datetime.now())
        dt_name = str(dt).replace("-","")

        # set different table and bucket for local test and online
        if self == "test":
            bucket_product = f"website/data/shopee/raw_all_products_test_{dt_name}.json"
            table_product = f"{bq_table_product}_TEST_{dt_name}"
        else:
            bucket_product = f"website/data/shopee/raw_all_products_{dt_name}.json"
            table_product = f"{bq_table_product}_{dt_name}"
        
        # upload to storage
        storage_client = storage.Client(client_name, credentials=ac_storage)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(bucket_product)
        blob.upload_from_filename(f"data/shopee/raw_all_products_{dt}.json") 
        print("storage upload done")
        
        # upload to bigquery
        # handling for duplicate table
        bq_client = bigquery.Client(project=bq_project_id, credentials=ac_bq)
        tables_list_client = bq_client.list_tables("external_data_sp")
        table_list = []
        for table in tables_list_client:
            t = table.dataset_id+"."+table.table_id
            table_list.append(t)

        if table_product in table_list:
            print("there is duplicated table, ", table_product)
        else:
            # get the dataframe
            df_product = pd.read_json(f"data/shopee/raw_all_products_{dt}.json")
            df_product["created_at"] = dt
            load_job = bq_client.load_table_from_dataframe(df_product, table_product)
            print("bigquery upload done, ",load_job.result())

    def upload_seller_to_storage_bigquery(self):
        # call preprocessing
        local_test = GeneralPreProcessing.access_configuration()
        client_name, bucket_name = GeneralPreProcessing.access_storage_configuration()
        bq_project_id, bq_table_product, bq_table_seller = GeneralPreProcessing.access_bq_configuration()
        credentials_storage, credentials_bq = GeneralPreProcessing.access_credential_local()
        ac_storage = service_account.Credentials.from_service_account_file(credentials_storage)
        ac_bq = service_account.Credentials.from_service_account_file(credentials_bq)

        # initiate the datetime for file/table name
        dt = datetime.date(datetime.now())
        dt_name = str(dt).replace("-","")
        
        # set different table and bucket for local test
        if self == "test":
            bucket_seller = f"website/data/shopee/raw_all_sellers_test_{dt_name}.json"
            table_seller = f"{bq_table_seller}_TEST"
        else:
            bucket_seller = f"website/data/shopee/raw_all_sellers_{dt_name}.json"
            table_seller = f"{bq_table_seller}"
        
        # upload to storage
        storage_client = storage.Client(client_name, credentials=ac_storage)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(bucket_seller)
        load_blob = blob.upload_from_filename(f"data/shopee/raw_all_sellers_{dt}.json")
        print("storage upload done")
        
        # upload to bigquery
        # get the dataframe first
        df_seller = pd.read_json(f"data/shopee/raw_all_sellers_{dt}.json")
        df_seller["created_at"] = dt
        bq_client = bigquery.Client(project=bq_project_id, credentials=ac_bq)
        load_job = bq_client.load_table_from_dataframe(df_seller, table_seller)
        print("bigquery upload done, ",load_job.result())


class GeneralPostProcessing:
    def delete_all_data():

        # initiate the directory
        dir_data = "data"
        dir_tokped = "data/tokped"
        dir_tokped_image = "data/tokped/images_files"
        dir_shopee = "data/shopee"
        dir_shopee_image = "data/shopee/images_files"

        # check the directory first
        if len(os.listdir(dir_data))==2 and len(os.listdir(dir_tokped))==1 and len(os.listdir(dir_tokped_image))==0 and len(os.listdir(dir_shopee))==1 and len(os.listdir(dir_shopee_image))==0:
            print("data directory is empty")
        else:
            # delete the directory
            shutil.rmtree(dir_data)
            
            # create the directory including the subdirectory
            os.mkdir(dir_data)
            os.mkdir(dir_tokped)
            os.mkdir(dir_tokped_image)
            os.mkdir(dir_shopee)
            os.mkdir(dir_shopee_image)
            print("data directory is clean")