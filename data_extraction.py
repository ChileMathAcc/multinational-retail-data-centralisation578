import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd


class DatabaseConnector():
    
    
    def __init__(self):
        self.engine = None
        self.db_tables = []
        
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as d:
            db_creds = yaml.safe_load(d)
        return db_creds
    
    
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        #Database connection details
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
        self.engine = self.init_db_engine()
        inspector = inspect(self.engine)
        self.db_tables = inspector.get_table_names()
        return self.db_tables
    

class DatabaseExtractor():
    
    
    def read_rds_table(DatabaseConnector, table):
        df = pd.read_sql_table(table, con = DatabaseConnector.engine, index_col = 'index')
        return df
    

D = DatabaseConnector()
print(DatabaseExtractor.read_rds_table(DatabaseConnector = D, table = D.list_db_tables()[2]).head(5))