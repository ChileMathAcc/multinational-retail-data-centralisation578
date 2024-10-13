import data_extraction
import pandas as pd


class data_cleaning():
    '''
    This takes data from data_exctration and cleans it in various ways.
    1. Changes the data types of the columns
    2. Drops NULL values
    3. Removes duplicates
    '''
    
    
    def __init__(self):
        clean_df = None
    
    def clean_user_data(self):
        D = data_extraction.DatabaseConnector()         #Initialises from DatabaseConnector
        data = data_extraction.DatabaseExtractor.read_rds_table(DatabaseConnector = D,
                                                                table = D.list_db_tables()[2])      #Retrieves data in panda format
        prefered_col_types = {
            'first_name': 'string',
            'last_name': 'string',
            'company': 'string',
            'email_address': 'string',
            'address': 'string',
            'country': 'string',
            'country_code': 'string',
            'phone_number': 'string',
            'user_uuid': 'string'
        }
        data = data.astype(prefered_col_types)      #Changes the data types of all the string-like columns
        data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], format = 'mixed', errors = 'coerce')      #Changes the data types of the datetime-like column
        data['join_date'] = pd.to_datetime(data['join_date'], format = 'mixed', errors = 'coerce')      #Changes the data types of the datetime-like column
        data = data.dropna(axis=0, how='any')       #Drops NULL values
        data = data.drop_duplicates()       #Drops duplicates
        return data

print(data_cleaning().clean_user_data().dtypes)
print(data_cleaning().clean_user_data().shape)
