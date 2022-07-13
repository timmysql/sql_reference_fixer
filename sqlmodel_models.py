from typing import Optional
# import pyodbc
from sqlmodel import Field, SQLModel, create_engine, Session, select
from sqlalchemy.engine import URL
from sqlmodel.sql.expression import Select, SelectOfScalar
from rich import inspect
import sql_server_connect as dbc


SelectOfScalar.inherit_cache = True  # type: ignore
Select.inherit_cache = True


def get_connection(server, database):    
    connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"""SERVER={server};DATABASE={database};Trusted_Connection=yes"""
    db = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    return db

def get_engine(db):
    engine = create_engine(db, echo=False)  
    return engine

# def get_engine():
#     db = get_connection(server=server, database=database)
#     engine = get_engine(db)
#     return engine    


# sqlite_file_name = "database.db"
# sqlite_url = f"sqlite:///{sqlite_file_name}"

 

class CentralReferences(SQLModel, table=True, schema='dbo'):
    __tablename__ = "tCAPTURE_REFERENCES_Central"
    id: Optional[int] = Field(default=None, primary_key=True)
    SchemaObject: str = None
    home_server_name: str = None
    home_database_name: str = None
    home_schema_name: str = None
    home_entity_name: str = None
    home_entity_type: str = None
    home_entity_create: str = None
    home_entity_alter: str = None
    referenced_server_name: str = None
    referenced_database_name: str = None
    referenced_schema_name: str = None
    referenced_entity_name: str = None
    desired_qualified_count: int = None
    current_qualified_count: int = None
    qualified_server_name: str = None
    qualified_database_name: str = None
    qualified_schema_name: str = None
    qualified_entity_name: str = None
    four_part_qualified: str = None
    three_part_qualified: str = None
    two_part_qualified: str = None
    one_part_qualified: str = None
    error_retrieving_info: str = None
    sp_refresh_text: str = None
    home_entity_name_conflict: str = None
    create_dt: str = None
  
  
def select_all(engine):
    with Session(engine) as session:
        statement = select(CentralReferences)
        results = session.exec(statement)    
        # print(results)
    return results  


class GetReferences:
    def __init__(self, home_server_name, home_database_name=None, referenced_server_name=None, home_schema_name=None, home_entity_name=None):
        server = 'CMSSQL'
        database = 'CMS_Central'        
        db = get_connection(server=server, database=database)
        self.engine = get_engine(db)
        self.home_server_name = home_server_name
        self.home_database_name = home_database_name
        self.referenced_server_name = referenced_server_name
        self.home_schema_name = home_schema_name
        self.home_entity_name = home_entity_name
        pass
    
    def home_server(self):
        with Session(self.engine) as session:
            statement = select(CentralReferences.home_server_name).distinct().where(CentralReferences.home_server_name == self.home_server_name)
            results = session.exec(statement).all()    
            # print(results)
        return results
    
    def home_server_dbs(self):
        if self.home_database_name:
            with Session(self.engine) as session:
                statement = select(CentralReferences.home_server_name, CentralReferences.home_database_name).distinct()\
                    .where(CentralReferences.home_server_name == self.home_server_name)\
                    .where(CentralReferences.home_database_name == self.home_database_name)\
                        .where(CentralReferences.referenced_server_name == self.referenced_server_name)
                results = session.exec(statement).all()
        else:  
            with Session(self.engine) as session:
                statement = select(CentralReferences.home_server_name, CentralReferences.home_database_name).distinct()\
                    .where(CentralReferences.home_server_name == self.home_server_name)\
                        .where(CentralReferences.referenced_server_name == self.referenced_server_name)
                results = session.exec(statement).all()              
            # print(results)
        return results     
              
    def list_home_objects(self):
        if self.home_database_name:
            with Session(self.engine) as session:
                statement = select(CentralReferences.home_server_name, CentralReferences.home_database_name, 
                                CentralReferences.home_schema_name,CentralReferences.home_entity_name, 
                                CentralReferences.home_entity_type, CentralReferences.home_entity_create, 
                                CentralReferences.home_entity_alter).distinct()\
                                        .where(CentralReferences.home_server_name == self.home_server_name)\
                                        .where(CentralReferences.home_database_name == self.home_database_name)\
                                        .where(CentralReferences.referenced_server_name == self.referenced_server_name)
        else:
            with Session(self.engine) as session:
                statement = select(CentralReferences.home_server_name, CentralReferences.home_database_name, 
                                CentralReferences.home_schema_name,CentralReferences.home_entity_name, 
                                CentralReferences.home_entity_type, CentralReferences.home_entity_create, 
                                CentralReferences.home_entity_alter).distinct()\
                                        .where(CentralReferences.home_server_name == self.home_server_name)\
                                        .where(CentralReferences.referenced_server_name == self.referenced_server_name)            
            
            results = session.exec(statement).all()    
            # print(results)
        return results    


class GetObjectReferences:
    def __init__(self, home_server_name, home_database_name, referenced_server_name, home_schema_name, home_entity_name):
        server = 'CMSSQL'
        database = 'CMS_Central'        
        db = get_connection(server=server, database=database)
        self.engine = get_engine(db)
        self.home_server_name = home_server_name
        self.home_database_name = home_database_name
        self.referenced_server_name = referenced_server_name
        self.home_schema_name = home_schema_name
        self.home_entity_name = home_entity_name
   
    def list_referenced_objects(self):
        with Session(self.engine) as session:
            statement = select(CentralReferences.SchemaObject,
                               CentralReferences.home_database_name,
                               CentralReferences.home_schema_name,
                               CentralReferences.home_entity_name,
                               CentralReferences.home_entity_type,
                               CentralReferences.home_entity_create, 
                               CentralReferences.home_entity_alter,
                               CentralReferences.referenced_server_name,
                               CentralReferences.referenced_database_name,
                               CentralReferences.referenced_schema_name,
                               CentralReferences.referenced_entity_name,
                               CentralReferences.error_retrieving_info,
                               CentralReferences.desired_qualified_count,
                               CentralReferences.current_qualified_count).distinct()\
                                    .where(CentralReferences.referenced_server_name == self.referenced_server_name)\
                                    .where(CentralReferences.home_database_name== self.home_database_name)\
                                    .where(CentralReferences.home_schema_name == self.home_schema_name)\
                                    .where(CentralReferences.home_entity_name == self.home_entity_name)
            results = session.exec(statement).all()    
            # print(results)
        return results                                      
        
        
        
        # query = f"""SELECT DISTINCT 
        #                     [SchemaObject],[home_database_name],[home_schema_name],[home_entity_name],[home_entity_type]
        #                     ,[home_entity_create], [home_entity_alter]
        #                     ,[referenced_server_name],[referenced_database_name],[referenced_schema_name]
        #                     ,[referenced_entity_name],[error_retrieving_info]
        #                     ,[desired_qualified_count]
        #                     ,[current_qualified_count]
        #             FROM 
        #                     dbo.{reference_table_name} RE 
        #             WHERE 
                            
        #                     lower([referenced_server_name]) =  lower('{current_server_ref}')
        #             AND    lower([home_database_name]) = lower('{home_database_name}')
        #             AND    lower([home_schema_name]) = lower('{home_schema_name}')
        #             AND    lower([home_entity_name]) = lower('{home_entity_name}') 
        #           --  AND    [home_entity_name] = 'spBOL_CUSTOMER_INFO'        
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            # [home_server_name], [home_database_name], [home_schema_name], [home_entity_name], [home_entity_type]  
            #         ,[home_entity_create], [home_entity_alter]
        
def select_all(engine):
    with Session(engine) as session:
        statement = select(CentralReferences)
        results = session.exec(statement)    
        # print(results)
    return results  


def create_model():
    engine = dbc.get_postgres_config() 
    SQLModel.metadata.create_all(engine)    
    
def main(): 
    x = GetReferences()  
    home = x.home_server_db()
    for thing in home:
        print(thing.home_server_name, thing.home_database_name)
    
    
    
if __name__ == '__main__':  
    main()
    
    # server = 'CMSSQL'
    # database = 'CMS_Central'
    # db = get_connection(server=server, database=database)
    # engine = get_engine(db)


    # with Session(engine) as session:
    #     statement = select(CentralReferences).where(CentralReferences.home_server_name == 'SQL04').where(CentralReferences.home_database_name == 'RS')
    #     results = session.exec(statement).all()    
    #     # print(results)
    # for x in results:
    #     print(x.home_server_name, x.home_database_name, x.home_schema_name, x.home_entity_name)
    #     input('press any key to continue')
        
        
        
        


