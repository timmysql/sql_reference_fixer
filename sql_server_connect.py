from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from configparser import ConfigParser
import sql_server_central_config as cfg
import inspect
# import db_config as cfg
# import pyodbc 

CENTRAL_SERVER = cfg.CENTRAL_SERVER
CENTRAL_DATABASE = cfg.CENTRAL_DATABASE


class CentralEngine:    
    def __init__(self): 
        pass
    
    def get_central_engine(self):
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"""SERVER={CENTRAL_SERVER};DATABASE={CENTRAL_DATABASE};Trusted_Connection=yes"""
        db = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        db_engine = create_engine(db,  connect_args = {'autocommit':True})
        return db_engine 
    
    def get_master_engine(self):
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"""SERVER={CENTRAL_SERVER};DATABASE=master;Trusted_Connection=yes"""
        db = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        db_engine = create_engine(db, connect_args = {'autocommit':True})
        return db_engine       
       

class GenericEngine:
    def __init__(self, server, database, user=None, password=None):
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        
    def get_engine(self):
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"""SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes"""
        db = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        db_engine = create_engine(db)
        return db_engine
    



def config_mssql(filename='db_config.ini', section='mssql'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            if param[0] == 'user': user = param[1]
            if param[0] == 'password': password = param[1]
            if param[0] == 'host': host = param[1]
            if param[0] == 'database': database = param[1]        
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"""SERVER={host};DATABASE={database};Trusted_Connection=yes"""
        db = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})        
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db, connection_string



def get_mssql_engine():    
    engine_string = config_mssql() 
    # print(engine_string[0])   
    db_engine = create_engine(engine_string[0])
    return db_engine

# def get_postgres_config():
#     engine_string = cfg.config_postgres()
#     db_engine = create_engine(engine_string, isolation_level="AUTOCOMMIT")
#     return db_engine


if __name__ == "__main__":
    # engine_string = cfg.config_mssql()
    # print(engine_string)
    # engine = get_mssql_engine()
    # print(engine)
    x = GenericEngine(server='SQL04',database='DBATools')
    print(x.get_engine())
    
    x = CentralEngine()
    print(x.get_central_engine())
    
    
    
    # inspect(engine, all=True)
    # pg = get_mssql_config()
    # print(connection_string)
    # print(pg)