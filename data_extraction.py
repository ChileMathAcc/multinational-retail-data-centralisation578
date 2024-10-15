from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import json
import boto3


class DatabaseExtractor():
    '''
    Uses pandas to create a dataframe from a table in the RDB
    '''
    
    def read_rds_table(table : str):
        '''
        Retrieves a sql table from the RDB as a dataframe
        '''
        
        df = pd.read_sql_table(table, con = DatabaseConnector().init_db_engine(), index_col = 0)
        return df
    
    def retrieve_pdf_data(self, link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        '''
        Takes a link to a pdf-document and creates a dataframe of the tables in the pdf
        '''
        
        pdf_data = tabula.read_pdf(link, pages = 'all', output_format = "dataframe")[0]     #Reads the tables off the pdf
        return pdf_data
    
    dict_stores = DatabaseConnector().read_db_creds(yaml_file = 'store_data_API.yaml')
    def list_number_stores(endpoint = dict_stores['endpoint_number_stores'], api_key = dict_stores['x-api-key']):
        '''
        
        '''
        
        headers = {
        'Content-Type': 'application/json',
        'x-api-key': f'{api_key}'
        }
        
        response = requests.get(endpoint, headers = headers)
        
        number_stores = json.loads(response.text)['number_stores']
        return number_stores
    
    def retrieve_stores_data(endpoint = dict_stores['endpoint_store'], api_key = dict_stores['x-api-key']):
        '''
        
        '''
        
        headers = {
        'Content-Type': 'application/json',
        'x-api-key': f'{api_key}'
        }
        store_number = 0
        response = requests.get(eval(endpoint), headers = headers)
        store_data = json.loads(response.text)
        data = {key : [store_data[key]] for key in store_data.keys()}
        for store_number in range(1,DatabaseExtractor.list_number_stores()):
            response = requests.get(eval(endpoint), headers = headers)
            store_data = json.loads(response.text)
            for key in data.keys():
                data[key].append(store_data[key])
        data = pd.DataFrame.from_dict(data)
        
        return data
    
    def extract_from_s3(link = 's3://data-handling-public/products.csv'):
        '''
        
        '''
        
        link_components = link.split('/')
        bucket = link_components[2]
        key = link_components[3]
        s3_client = boto3.client('s3')
        s3_client.download_file(bucket, key, 'product_info.csv')
        
        product_info = pd.read_csv('product_info.csv')
        
        return product_info

