import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import tabula


class DatabaseConnector():
    '''
    Establishes a connection to AWS for data retrieval 
    '''
    
    def __init__(self):
        self.engine = None
        self.db_tables = []
        
    def read_db_creds(self):
        '''
        Opens a yaml document with the credentails for the database
        '''
        
        with open('db_creds.yaml', 'r') as d:
            db_creds = yaml.safe_load(d)
        return db_creds
    
    
    def init_db_engine(self):
        '''
        Uses the credentail from the above method to establish a connect to an RDB
        '''
        
        db_creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_creds['RDS_HOST']
        USER = db_creds['RDS_USER']
        PASSWORD = db_creds['RDS_PASSWORD']
        PORT = db_creds['RDS_PORT']
        DATABASE = db_creds['RDS_DATABASE']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        return self.engine
    
    def list_db_tables(self):
        '''
        Retrieves a list of tables from an RDB
        '''
        
        self.engine = self.init_db_engine()
        inspector = inspect(self.engine)
        self.db_tables = inspector.get_table_names()
        return self.db_tables
    

class DatabaseExtractor():
    '''
    Uses pandas to create a dataframe from a table in the RDB
    '''
    
    def read_rds_table(DatabaseConnector, table):
        df = pd.read_sql_table(table, con = DatabaseConnector.engine, index_col = 0)
        return df
    
    def retrieve_pdf_data(link):
        pdf_data = pd.DataFrame(tabula.read_pdf(link))
        return pdf_data