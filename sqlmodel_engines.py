from sqlmodel import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
from rich import inspect
import sql_server_central_config as cfg


CENTRAL_SERVER = cfg.CENTRAL_SERVER
CENTRAL_DATABASE = cfg.CENTRAL_DATABASE

class Engine:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.central_server = 'DBA_CMS'
        self.central_db = 'DBATools'
        # self.refs_db = 'DBATools'
    
    def get_connection_url(self, server, database): 
        server = self.server
        database = self.database
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"""SERVER={server};DATABASE={database};Trusted_Connection=yes"""
        db = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        return db

    def build_engine(self, db):
        engine = create_engine(db, echo=True)  
        return engine
    
    def get_central_engine(self):
        db = self.get_connection_url(server=self.central_server, database=self.central_db)
        engine = self.build_engine(db)
        return engine
        
    def get_variable_engine(self):
        db = self.get_connection_url(server=self.server, database=self.database)
        engine = self.build_engine(db)        
        return engine
            

class Execute(Engine):
    def __init__(self, server):
        super().__init__(server)
    


def execute_tsql(server, database, tsql):
    e = Engine(server=server,database=database)
    engine = e.get_variable_engine()     
    with engine.connect() as con:
        rs = con.execute(text(tsql))
        # for row in rs:
        #     print(row)
        return rs


def main():
    database = 'RS'
    schema = 'dbo'
    entity = '612_spEGG_INVOICES_REPORT'
    # tsql = f"""SELECT * FROM sys.dm_sql_referenced_entities({schema}.{entity},'OBJECT')
    # tsql_ = f"""SELECT * FROM dbo.tEMPLOYEES;"""
    tsql_ = f"""SELECT * FROM {database}.sys.dm_sql_referenced_entities('{schema}.{entity}','OBJECT')"""    
    result = execute_tsql(server='SQL04', database='dbEMPLOYEES', tsql=tsql_)
    # inspect(result, methods=True)
    # print(result)
    for i in result:
        inspect(i,methods=True)
        input('stop... ')
    
    
    
        
        
if __name__ == '__main__': 
    main()
    # tsql = """EXEC cms.spReferencedEntities_SendToCentral"""
    # execute_tsql(server='SQL04',tsql=tsql)