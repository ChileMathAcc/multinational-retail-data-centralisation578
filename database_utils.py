import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import psycopg2
import pandas as pd


class DatabaseConnector():
    '''
    Establishes a connection to AWS for data retrieval 
    '''
    
    def __init__(self):
        self.db_tables = []
        
    def read_db_creds(self, yaml_file):
        '''
        Opens a yaml document with the credentails for the database
        '''
        
        with open(yaml_file, 'r') as d:
            db_creds = yaml.safe_load(d)
        return db_creds
    
    
    def init_db_engine(self):
        '''
        Uses the credentail from the above method to establish a connect to an RDB
        '''
        
        db_creds = self.read_db_creds('db_creds.yaml')
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_creds['RDS_HOST']
        USER = db_creds['RDS_USER']
        PASSWORD = db_creds['RDS_PASSWORD']
        PORT = db_creds['RDS_PORT']
        DATABASE = db_creds['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        return engine
    
    def list_db_tables(self):
        '''
        Retrieves a list of tables from an RDB
        '''
        
        self.engine = self.init_db_engine()
        inspector = inspect(self.engine)
        self.db_tables = inspector.get_table_names()
        return self.db_tables
    
    def upload_to_db(dataframe : pd.DataFrame, table_name : str):
        '''
        Inserts a dataframe into a postgres database
        '''
        
        # Database connection details
        creds = self.read_db_creds('local_db_creds.yaml')
        DATABASE_TYPE = creds['DATABASE_TYPE']
        DBAPI = creds['DBAPI']
        USER = creds['USER']
        PORT = creds['PORT']
        DATABASE = creds['DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PORT}/{DATABASE}")
        
        try:
            db_conn = engine.connect(dbname='sales_data', user='postgres', host='localhost')
        except:
            print("Unable to connect to the database")
        with db_conn:
            dataframe.to_sql(name = table_name, con =  db_conn, if_exists = 'replace')
        db_conn.close()