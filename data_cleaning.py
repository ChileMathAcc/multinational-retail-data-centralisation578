import data_extraction
import pandas as pd
import numpy as np


class data_cleaner():
    '''
    This takes data from data_exctration and cleans it in one of three ways.
    1. Changes the data types of the columns
    2. Drops NULL values
    3. Removes duplicates
    '''
    
    
    def __init__(self):
        clean_df = None
    
    def clean_user_data():
        '''
        Cleans the user data
        '''
        
        D = data_extraction.DatabaseConnector()         #Initialises from DatabaseConnector
        data = data_extraction.DatabaseExtractor.read_rds_table(table = D.list_db_tables()[2])      #Retrieves data in panda format
        data = data.infer_objects()      #Changes the data types of all the string-like columns
        data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], format = 'mixed', errors = 'coerce')      #Changes the data types of the datetime-like column
        data['join_date'] = pd.to_datetime(data['join_date'], format = 'mixed', errors = 'coerce')      #Changes the data types of the datetime-like column
        data = data.dropna(axis=0, how='any')       #Drops NULL values
        data = data.drop_duplicates()       #Drops duplicates
        return data
    
    def clean_card_data(link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        '''
        Cleans the payment card data
        '''

        pdf_data = data_extraction.DatabaseExtractor.retrieve_pdf_data(link)
        pdf_data['card_number'] = pdf_data['card_number'].str.replace('\D+','', regex = True)
        pdf_data['card_number'] = pd.to_numeric(pdf_data['card_number'], errors = 'coerce', downcast = 'integer').convert_dtypes()      #Converts the card_numbers to int64
        pdf_data['expiry_date'] = pd.to_datetime(pdf_data['expiry_date'], format = '%m/%y', errors = 'coerce')      #Converts the card expiry dates to datetime
        pdf_data['date_payment_confirmed'] = pd.to_datetime(pdf_data['date_payment_confirmed'], format = 'mixed', errors = 'coerce')        #Converts payment date to datetime
        pdf_data.dropna(axis = 0, how = 'any', inplace = True)          #Deletes NULL values from the dataframe
        
        return pdf_data
    
    def clean_store_data():
        
        store_data = data_extraction.DatabaseExtractor.retrieve_stores_data()
        store_data.drop(columns = 'lat', axis = 1, inplace = True)
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors = 'coerce', format = 'mixed')
        store_data.dropna(axis = 0, how = 'any', inplace = True, subset = 'opening_date')
        
        return store_data
    
    def convert_product_weights(df = data_extraction.DatabaseExtractor.extract_from_s3()):
        '''
        
        '''
        
        print(df.head(3))
        df['weight'] = df['weight'].astype('string')
        df['weight'] = df['weight'].str.replace('kg','')
        df['weight'] = df['weight'].str.replace('x', '*')
        df['weight'] = df['weight'].str.replace('.', '')
        df['weight'] = df['weight'].str.replace('^0+', '', regex = True)
        df['weight'] = df['weight'].apply(lambda x: x.replace('g', '* 0.001') if pd.notna(x) and 'g' in x else x)
        df['weight'] = df['weight'].apply(lambda x: float(eval(x)) if pd.notna(x) and '*' in x else x)
        
        return df