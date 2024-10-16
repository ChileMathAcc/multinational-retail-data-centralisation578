import data_cleaning
import database_utils


Data = data_cleaning.data_cleaner.clean_user_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_users', dataframe = Data)
print('Uploaded dim_users')

Data = data_cleaning.data_cleaner.clean_card_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_card_details', dataframe = Data)
print('Uploaded dim_card_details')

Data = data_cleaning.data_cleaner.clean_store_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_store_details', dataframe = Data)
print('Uploaded dim_store_details')

Data = data_cleaning.data_cleaner.clean_products_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_products', dataframe = Data)
print('Uploaded dim_products')

Data = data_cleaning.data_cleaner.clean_orders_table()
database_utils.DatabaseConnector().upload_to_db(table_name = 'orders_table', dataframe = Data)
print('Uploaded orders_table')

Data = data_cleaning.data_cleaner.clean_date_details()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_date_times', dataframe = Data)
print('Uploaded dim_date_times')