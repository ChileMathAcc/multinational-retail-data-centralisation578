import data_cleaning
import database_utils


Data = data_cleaning.data_cleaner.clean_user_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_users', dataframe = Data)

Data = data_cleaning.data_cleaner.clean_card_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_card_details', dataframe = Data)

Data = data_cleaning.data_cleaner.clean_store_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_store_details', dataframe = Data)

Data = data_cleaning.data_cleaner.clean_products_data()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_products', dataframe = Data)

Data = data_cleaning.data_cleaner.clean_orders_table()
Data.drop(columns = ['level_0', 'index'], axis = 1, inplace = True)
database_utils.DatabaseConnector().upload_to_db(table_name = 'orders_table', dataframe = Data)

Data = data_cleaning.data_cleaner.clean_date_details()
database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_date_times', dataframe = Data)