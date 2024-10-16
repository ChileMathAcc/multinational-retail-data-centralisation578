import data_cleaning
import database_utils

if __name__ == '__main__':
    
    #Cleans and uploads user data
    Data = data_cleaning.data_cleaner.clean_user_data()
    database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_users', dataframe = Data)
    print('Uploaded dim_users')
    
    #Cleans and uploads card data
    Data = data_cleaning.data_cleaner.clean_card_data()
    database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_card_details', dataframe = Data)
    print('Uploaded dim_card_details')
    
    #Cleans and uploads store_data
    Data = data_cleaning.data_cleaner.clean_store_data()
    database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_store_details', dataframe = Data)
    print('Uploaded dim_store_details')
    
    #Cleans and uploads product data
    Data = data_cleaning.data_cleaner.clean_products_data()
    database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_products', dataframe = Data)
    print('Uploaded dim_products')
    
    #Clean and uploads orders data
    Data = data_cleaning.data_cleaner.clean_orders_table()
    database_utils.DatabaseConnector().upload_to_db(table_name = 'orders_table', dataframe = Data)
    print('Uploaded orders_table')
    
    #Cleanings and uploads payment date data
    Data = data_cleaning.data_cleaner.clean_date_details()
    database_utils.DatabaseConnector().upload_to_db(table_name = 'dim_date_times', dataframe = Data)
    print('Uploaded dim_date_times')