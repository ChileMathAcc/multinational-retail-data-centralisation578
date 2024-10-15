import data_cleaning
import data_extraction
import database_utils
import pandas as pd

D = data_cleaning.data_cleaner.clean_date_details()
print(type(D))
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_date_times', dataframe = D)