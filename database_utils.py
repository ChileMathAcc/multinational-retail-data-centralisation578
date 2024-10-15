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
        self.engine = None
        self.db_tables = []
        
    def read_db_creds(self, yaml_file):
        '''
        Opens a yaml document with the credentails for the database
        '''
        
        #Opens a yaml file (read mode) and loads its data
        with open(yaml_file, 'r') as d:
            db_creds = yaml.safe_load(d)
            
        return db_creds
    
    
    def init_db_engine(self):
        '''
        Uses the credentail from the above method to establish a connect to an RDB
        '''
        
        #Reads postgres credentails from a yaml file
        db_creds = self.read_db_creds('db_creds.yaml')
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_creds['RDS_HOST']
        USER = db_creds['RDS_USER']
        PASSWORD = db_creds['RDS_PASSWORD']
        PORT = db_creds['RDS_PORT']
        DATABASE = db_creds['RDS_DATABASE']
        #Creates the engine used to connection to the database on the AWS server
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        self.engine = engine
        
        return engine
    
    def list_db_tables(self):
        '''
        Retrieves a list of tables from an RDB
        '''
        
        self.engine = self.init_db_engine()
        inspector = inspect(self.engine)
        #Uses the engine to retreive a list of the avialable tables
        self.db_tables = inspector.get_table_names()
        
        return self.db_tables
    
    def upload_to_db(self, dataframe : pd.DataFrame, table_name : str):
        '''
        Inserts a dataframe into a postgres database
        '''
        
        #Reads database connection details from a yaml file
        creds = self.read_db_creds('local_db_creds.yaml')
        DATABASE_TYPE = creds['DATABASE_TYPE']
        DBAPI = creds['DBAPI']
        USER = creds['USER']
        PORT = creds['PORT']
        DATABASE = creds['DATABASE']
        PASSWORD = creds['PASSWORD']
        ENDPOINT = creds['ENDPOINT']
        #Create the engine to the local server
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        #Starts a connection to the local server
        db_conn = engine.connect()
        with db_conn:
            #Uploads a dataframe to the local server
            dataframe.to_sql(name = table_name, con =  db_conn, if_exists = 'replace')
            #Closes the connection
            db_conn.close()
        