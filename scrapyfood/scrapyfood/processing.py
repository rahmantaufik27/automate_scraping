import pandas as pd
import re 
import os
from datetime import datetime
from dotenv import load_dotenv
import shutil
import requests

# initiate the variable environment
load_dotenv()
data_placed = os.environ['DATA_PLACED']

# post processing
class TokpedPostProcessing:

    # transform data raw products into csv
    def transform_product(self):
        
        # get the data from json raw
        if data_placed == "LOCAL":
            path = f"data/tokped/raw_products_{self}.json"   
        else:
            path = f"data/tokped/raw_products_{self}.json"

        # store to dataframe
        df_raw = pd.read_json(path)
        # store the first length of raw data
        len1 = len(df_raw)

        # filter products based on category fnb
        # makesure to check the category in tokped up to date
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

        # convert to csv file
        if data_placed == "LOCAL":
            df_csv.to_csv(f"data/tokped/products_{self}.csv", index=False)
        else:
            df_csv.to_csv(f"data/tokped/products_{self}.csv", index=False)

        # if total data is more than 250 then then images should be download   
        len_data = len2            
        return len_data, df_csv
        
    def delete_raw_data(self):
        # delete the raw file
        if data_placed == "LOCAL":
            if os.path.exists(f"data/tokped/raw_products_{self}.json"):
                os.remove(f"data/tokped/raw_products_{self}.json")
            else:
                print("cannot remove")
        else:
            if os.path.exists(f"data/tokped/raw_products_{self}.json"):
                os.remove(f"data/tokped/raw_products_{self}.json")
            else:
                print("cannot remove")
    
    def download_images(self):
        # delete the old images files first
        if data_placed == "LOCAL":
            if os.path.exists(f"data/tokped/images_files"):
                shutil.rmtree(f"data/tokped/images_files")
                path = os.path.join("data/tokped","images_files")
                os.mkdir(path)
                is_removed = 1
            else:
                print("cannot remove")
                is_removed = 0
        else:
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
            if data_placed == "LOCAL":
                df = pd.read_csv(f"data/tokped/products_{self}.csv")  
            else:
                df = pd.read_csv(f"data/tokped/products_{self}.csv")

            # download files based on images url
            for u, n in zip(df["images_url"], df["name"]):
                response = requests.get(u)
                n = f"data/tokped/images_files/{self}_{n}.jpg"
                file = open(n, "wb")
                file.write(response.content)
                file.close()

            # store the data as zip and put to directory
            dt = datetime.date(datetime.now())
            if data_placed == "LOCAL":
                dir_name = "data/tokped/images_files"
                output_filename = f"data/tokped/images_{self}_{dt}"
            else:
                dir_name = "data/tokped/images_files"
                output_filename = f"data/tokped/images_{self}_{dt}"


            # return the successfull report
            report_zip = shutil.make_archive(output_filename, 'zip', dir_name)
        else:
            report_zip = ""
        
        return report_zip
