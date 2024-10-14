from database_utils import DatabaseConnector
import pandas as pd
import tabula


class DatabaseExtractor():
    '''
    Uses pandas to create a dataframe from a table in the RDB
    '''
    
    def read_rds_table(table : str):
        '''
        Retrieves a sql table from the RDB as a dataframe
        '''
        
        df = pd.read_sql_table(table, con = DatabaseConnector.init_db_engine(), index_col = 0)
        return df
    
    def retrieve_pdf_data(self, link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        '''
        Takes a link to a pdf-document and creates a dataframe of the tables in the pdf
        '''
        
        pdf_data = tabula.read_pdf(link, pages = 'all', output_format = "dataframe")[0]     #Reads the tables off the pdf
        return pdf_data

