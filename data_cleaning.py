import data_extraction
import database_utils
import pandas as pd
import requests
import json


class data_cleaner():
    '''
    This takes data from data_exctration and cleans it in one of two ways.
    1. Changes the data types of the columns
    2. Drops NULL values
    '''
    
    
    def __init__(self):
        clean_df = None
    
    def clean_user_data():
        '''
        Cleans the user data
        '''
        
        #Initialises from DatabaseConnector
        D = data_extraction.DatabaseConnector()
        #Retrieves data in panda format
        data = data_extraction.DatabaseExtractor.read_rds_table(table = D.list_db_tables()[2])
        #Changes the data types of all the string-like columns
        data = data.infer_objects()
        #Changes the data types of the datetime-like column
        data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], format = 'mixed', errors = 'coerce')
        #Changes the data types of the datetime-like column
        data['join_date'] = pd.to_datetime(data['join_date'], format = 'mixed', errors = 'coerce')
        #Drops NULL values
        data = data.dropna(axis=0, how='any')
        #Drops duplicates
        data = data.drop_duplicates()
        
        return data
    
    def clean_card_data(link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        '''
        Cleans the payment card data
        '''
        
        #Loads in the data
        pdf_data = data_extraction.DatabaseExtractor.retrieve_pdf_data(link)
        #Removes digit character 
        pdf_data['card_number'] = pdf_data['card_number'].str.replace('\D+','', regex = True)
        #Converts the type of card_number to integers
        pdf_data['card_number'] = pd.to_numeric(pdf_data['card_number'], errors = 'coerce', downcast = 'integer').convert_dtypes()
        #Uses the regex %m/%y(MM/YY) to convert the expiry_date
        pdf_data['expiry_date'] = pd.to_datetime(pdf_data['expiry_date'], format = '%m/%y', errors = 'coerce')
        #Converts payment dates to datetime
        pdf_data['date_payment_confirmed'] = pd.to_datetime(pdf_data['date_payment_confirmed'], format = 'mixed', errors = 'coerce')
        #Deletes NULL values
        pdf_data.dropna(axis = 0, how = 'any', inplace = True)
        
        return pdf_data
    
    def clean_store_data():
        '''
        Cleans the store data
        '''
        
        #Retreives store data
        store_data = data_extraction.DatabaseExtractor.retrieve_stores_data()
        #Deletes the lat column due to excessive NULL values
        store_data.drop(columns = 'lat', axis = 1, inplace = True)
        #Converts the opening date to datetime
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors = 'coerce', format = 'mixed')
        #Deletes NULL values
        store_data.dropna(axis = 0, how = 'any', inplace = True, subset = 'opening_date')
        
        return store_data
    
    def convert_product_weights(df = data_extraction.DatabaseExtractor().extract_from_s3()):
        '''
        Converts the weight units of the weight column to kg
        '''
        
        #Converts from object type to string type for easier manipulation
        df['weight'] = df['weight'].astype('string')
        #Removes the kg tag for those already in kg
        df['weight'] = df['weight'].str.replace('kg','')
        #Replaces x with * for entries with bundled products i.e (4 x 12g)
        df['weight'] = df['weight'].str.replace('x', '*')
        #Removes the last . from entries
        df['weight'] = df['weight'].str.replace('.$', '', regex = True)
        #Remove leading zeros
        df['weight'] = df['weight'].str.replace('^0+', '', regex = True)
        #Uses a lambda function to covert g to kg
        df['weight'] = df['weight'].apply(lambda x: x.replace('g', '* 0.001') if pd.notna(x) and 'g' in x else x)
        #Converts to float and evaluates expressions of the form (4 * 12)
        df['weight'] = df['weight'].apply(lambda x: float(eval(x)) if pd.notna(x) and '*' in x else x)
        
        return df
    
    def clean_products_data(df = convert_product_weights()):
        '''
        Cleans the product_data
        '''
        
        #Converts the date_added column to datetime
        df['date_added'] = pd.to_datetime(df['date_added'], errors = 'coerce', format = 'mixed')
        #Deletes NULL values
        df.dropna(axis = 0, how = 'any', inplace = True)
        
        return df
    
    def clean_orders_table():
        '''
        Cleans the orders table
        '''
        
        #Retreives the list of tables
        tables = database_utils.DatabaseConnector().list_db_tables()
        #Load the orders table into a dataframe 
        df = data_extraction.DatabaseExtractor.read_rds_table(table = tables[3])
        #Deletes the columns first_name, last_name, level_o and 1
        df.drop(columns = ['first_name', 'last_name', '1', 'level_0'], axis = 1, inplace = True)
        
        return df
    
    def clean_date_details(link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        '''
        Cleans date details file
        '''
        
        #Headers for request
        header = {
            'Content-Type': 'application/json',
        }
        #Request the json file from the link
        request = requests.get(link, headers = header)
        #Loads the data from the request
        date_details = pd.DataFrame.from_dict(json.loads(request.text))
        #Combines the year, month and day columns to a single datetime formatted column
        date_details['time'] = pd.to_datetime(date_details[["year", "month", "day"]], errors = 'coerce', format = 'mixed')
        #Deletes the year, month and day columns
        date_details.drop(['year', 'month', 'day'], axis = 1, inplace = True)
        #Deletes rows with NULL values
        date_details.dropna(axis = 0, how = 'any', inplace = True)
        
        return date_details