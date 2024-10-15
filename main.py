import data_cleaning
import data_extraction
import database_utils
import pandas as pd

D = data_cleaning.data_cleaner.clean_orders_table()
database_utils.DatabaseConnector().upload_to_db(table_name = 'orders_table', dataframe = D)