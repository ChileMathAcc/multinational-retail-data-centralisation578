import data_cleaning
import data_extraction
import database_utils
import pandas as pd

D = data_cleaning.data_cleaner.clean_products_data()
database_utils.DatabaseConnector().upload_to_db(table_name='dim_products', dataframe = D)