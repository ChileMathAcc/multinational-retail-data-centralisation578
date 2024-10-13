import data_extraction
import pandas as pd


class data_cleaning():
    
    
    def __init__(self):
        clean_df = None
    
    def clean_user_data(self):
        D = data_extraction.DatabaseConnector()
        data = data_extraction.DatabaseExtractor.read_rds_table(DatabaseConnector = D,
                                                                table = D.list_db_tables()[2])
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
        data = data.astype(prefered_col_types)
        data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], format = 'mixed', errors = 'coerce')
        data['join_date'] = pd.to_datetime(data['join_date'], format = 'mixed', errors = 'coerce')
        data = data.dropna(axis=0, how='any')
        data = data.drop_duplicates()
        return data

print(data_cleaning().clean_user_data().dtypes)
print(data_cleaning().clean_user_data().shape)
