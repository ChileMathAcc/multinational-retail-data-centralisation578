import data_cleaning
import data_extraction
import database_utils


df = data_cleaning.data_cleaner.clean_card_data()
table_name = 'card_data'
database_utils.DatabaseConnector().upload_to_db(dataframe = df, table_name = table_name)