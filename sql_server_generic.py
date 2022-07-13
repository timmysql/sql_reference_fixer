import pyodbc
import pandas as pd
from sqlalchemy.sql import text
from sqlmodel_engines import Engine
from rich import inspect


import sql_server_connect as cxn
x = cxn.CentralEngine()
CENTRAL_ENGINE = x.get_central_engine()
MASTER_ENGINE = x.get_master_engine()

# def new_connection(server, database):
#     cnxn_trusted = pyodbc.connect(
#         Trusted_Connection='Yes',
#         Driver='{ODBC Driver 17 for SQL Server}',
#         Server=server,
#         Database=database,

#     )
#     return cnxn_trusted

def return_df(tsql,server, database):
    cnxn = new_connection(server=server, database=database)    
    cursor = cnxn.cursor()
    df = pd.read_sql(tsql, cnxn)
    cursor.close()
    cnxn.close()
    return df


def return_data(tsql,server, database):  
    cnxn = new_connection(server=server, database=database)    
    cursor = cnxn.cursor()
    # -----------------------------------------------
    # tsql = f"""SELECT * FROM ne_radio;"""
    cursor.execute(tsql) 
    data = cursor.fetchall() 
    cursor.close()
    cnxn.close()
    return data


def execute_sql(tsql,server, database): 
    cnxn = new_connection(server=server, database=database)    
    cursor = cnxn.cursor()
    # print(tsql)
    cursor.execute(tsql) 
    # data = cursor.fetchall() 
    cursor.commit()
    cursor.close()
    cnxn.close()
    # return data
    
    
    
def engine_return_data(engine, tsql):  
    with engine.connect() as connection:
        result = connection.execute(text(tsql))
    return result
    
    
def engine_return_df(engine, tsql):
    with engine.connect() as connection:     
        df = pd.read_sql(tsql, connection)        
    return df      
    
    
def central_engine_return_data(tsql):  
    with CENTRAL_ENGINE.connect() as connection:
        result = connection.execute(text(tsql))
    return result
    
    
def central_engine_return_df(tsql):
    with CENTRAL_ENGINE.connect() as connection:     
        df = pd.read_sql(tsql, connection)        
    return df    
        
        


def main():
    print('stub')

    
if __name__ == '__main__': 
    # e = MASTER_ENGINE
    # e = Engine(server='SQL04')

    tsql = "select test from test_table"
    data = central_engine_return_data(tsql)
    inspect(data)
    
    # tsql = "select test from test_table"
    # df = central_engine_return_df(tsql)
    # inspect(df)
    # engine_return_data()

    # print(cnxn)
    
    
    # tsql="""SELECT * FROM [DBATools].[dbo].[REFERENCED_ENTITIES]"""       

    # server='SQL05'
    # database='DBATools'
    # df = return_df(tsql=tsql,server=server, database=database)
    
    # print(df)
    
    
    
    
    # cnxn = new_connection(server='SQL05',database='DBATools')
 
    # cursor = cnxn.cursor()
    # df = pd.read_sql(tsql, cnxn)
    # cursor.close()
    # cnxn.close()    
    
    
# cnxn = pyodbc.connect(
#     Trusted_Connection='No',
#     Driver='{ODBC Driver 17 for SQL Server}',
#     Server=server,
#     Database=database,
#     UID=username,
#     PWD=password
# )


