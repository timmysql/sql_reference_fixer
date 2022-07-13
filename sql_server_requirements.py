import sql_server_generic as sql
import sql_server_connect as cxn
import sql_server_central_config as cfg
# import sp_loadrefs 
# import sp_helptext2
import inspect
import os
# import sql_server_connect as cxn
print_function = False

CENTRAL_SERVER = cfg.CENTRAL_SERVER
CENTRAL_DATABASE = cfg.CENTRAL_DATABASE
x = cxn.CentralEngine()
CENTRAL_ENGINE = x.get_central_engine()
MASTER_ENGINE = x.get_master_engine()


################################################################
# FACILITY SEARCH
################################################################

# def load_facilities():
#     tsql = f"""spLoad_facilities;"""
#     sql.execute_sql(tsql)
# def create_sp_loadrefs():
#     # if print_function == True: print(inspect.stack()[0][3])
#     # sp_stub = sp_helptext2.sp_helpdesk2_stub
#     sp_alter = sp_loadrefs.sp_loadrefs
#     print(sp_alter)
#     # sql.execute_sql(tsql=sp_stub, server=server, database='master')
#     # sql.execute_sql(tsql=sp_alter, server=server, database='master')    
    
# def create_loadrefs_procedure():
#     with open('sp_loadrefs.sql') as f:
#         lines = f.readlines()
#         max_index = len(lines)
#         for i in range(max_index):  
#             print(lines[i].rstrip())
        # print(lines.index)
###################################################################  
# STORE PROCEDURE
# Load Refs
###################################################################  
class SqlObjects:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.sql_dir = '\\sql\\'
        self.cwd = os.getcwd() + self.sql_dir
        
    def exec_from_file_name(self, file_name): 
        file_path = self.cwd + file_name       
        sql_file = open(file_path)          
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=self.database)           
        
        
    def create_sp_loadrefs(self):   
        file_path = self.cwd + "sql_create_loadrefs_sp.sql"     
        # sql_file = open("sql_create_loadrefs_sp.sql")
        sql_file = open(file_path)        
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=self.database)     


    def create_stub_loadrefs(self):  
        file_path = self.cwd + "sql_create_loadrefs_stub.sql"   
        # sql_file = open("sql_create_loadrefs_stub.sql")
        sql_file = open(file_path)        
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=self.database)    
        
        
    ###################################################################      
    # TABLES
    ###################################################################  
    def drop_refs_table(self):  
        file_path = self.cwd + "sql_drop_refs_table.sql"
        # sql_file = open("sql_drop_refs_table.sql")
        sql_file = open(file_path)        
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=self.database)          
        
    def create_refs_table(self):  
        file_path = self.cwd + "sql_create_refs_table.sql"
        # sql_file = open("sql_create_refs_table.sql")
        sql_file = open(file_path)        
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=self.database)      
        
    ###################################################################      
    # HELPTEXT2
    # helptext2 sp
    ###################################################################  
    def create_sp_helptext2(self):
        database='master'        
        file_path = self.cwd + "sql_create_helptext2_sp.sql"
        # sql_file = open("sql_create_helptext2_sp.sql")
        sql_file = open(file_path)          
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=database)     

    # helptext2 stub
    def create_stub_helptext2(self):
        database='master'
        file_path = self.cwd + "sql_create_helptext2_stub.sql"
        # sql_file = open("sql_create_helptext2_stub.sql")
        sql_file = open(file_path)        
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=database)       

    ###################################################################      
    # SEND TO CENTRAL
    # sp_send
    ###################################################################  
    def create_sp_sendtocentral(self):
        file_path = self.cwd + "sql_create_loadrefs_sendtocentral_sp.sql"       
        # sql_file = open("sql_create_loadrefs_sendtocentral_sp.sql")
        sql_file = open(file_path)        
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=self.database)     

    # helptext2 stub
    def create_stub_sendtocentral(self):
        file_path = self.cwd + "sql_create_loadrefs_sendtocentral_stub.sql"
        # sql_file = open("sql_create_loadrefs_sendtocentral_stub.sql")
        sql_file = open(file_path)        
        tsql = sql_file.read()   
        sql.execute_sql(tsql=tsql,server=self.server,database=self.database)       


    def read_file_test(self):  
        file_path = self.cwd + "sql_create_loadrefs_sp.sql"    
        sql_file = open(file_path)
        tsql = sql_file.read()   
        print(tsql)
    # sql.execute_sql(tsql=tsql,server=server,database=database)   

class ExecuteSql(SqlObjects):
    def __init__(self, server, database):
        super().__init__(server, database)

def main_by_server(server):
    """stub/create helptext2 sp    
    
    """   
    print('server: '  + server) 
    y = ExecuteSql(server=server, database='master')
    y.create_stub_helptext2()      
    y.create_sp_helptext2()    
    
    
    x = ExecuteSql(server=server, database='DBATools')
    x.drop_refs_table()
    x.create_refs_table()
    
    # stub/create loadrefs sp
    x.create_stub_loadrefs()
    x.create_sp_loadrefs()
     
    x.create_stub_sendtocentral()
    x.create_sp_sendtocentral()     
    
    
def main_all_servers():
    servers = ['wv-sql03','wv-sql03\\rosserp','SQL04','SQL05','IGNITION01','CMSSQL']
    for server in servers:
        main_by_server(server=server)
    print('end of main_all_servers')    
    
def create_db():
    database_name = CENTRAL_DATABASE
    create_db_string = ''
    # create_db_string += "USE master;" + '\n'
    create_db_string += f"IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{database_name}')" + '\n'
    create_db_string += f"BEGIN" + '\n'        
    create_db_string += f"CREATE DATABASE {database_name};" + '\n'
    create_db_string += f"END" + '\n'
    create_db_string += f"ELSE" + '\n'
    create_db_string += f"SELECT 'DATABASE ALREADY EXISTS'" + '\n'
    create_db_string += f"" + '\n'
    e = MASTER_ENGINE
    
    response = input(f"this will create database {CENTRAL_DATABASE} on server {CENTRAL_SERVER}.  Do you wish to continue? Enter y or n.")
    if response == 'y':
        returned = e.execute(create_db_string)
        print(f'database {CENTRAL_DATABASE} created on server {CENTRAL_SERVER}')
        inspect(returned)
    else:
        print('nothing done')
        
                
if __name__ == '__main__':
    database_name = CENTRAL_DATABASE
    create_db_string = ''
    # create_db_string += "USE master;" + '\n'
    create_db_string += f"IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{database_name}')" + '\n'
    create_db_string += f"BEGIN" + '\n'        
    create_db_string += f"CREATE DATABASE {database_name};" + '\n'
    create_db_string += f"END" + '\n'
    create_db_string += f"ELSE" + '\n'
    create_db_string += f"SELECT 'DATABASE ALREADY EXISTS'" + '\n'
    create_db_string += f"" + '\n'
    e = MASTER_ENGINE
    
    response = input(f"this will create database {CENTRAL_DATABASE} on server {CENTRAL_SERVER}.  Do you wish to continue? Enter y or n.")
    if response == 'y':
        returned = e.execute(create_db_string)
        print(f'database {CENTRAL_DATABASE} created on server {CENTRAL_SERVER}')
        inspect(returned)
    else:
        print('nothing done')
        
    
    
    
    
    
    
    
    # main_all_servers()
    
    # x = CreateSqlObjects(server='x', database='y')
    # x.read_file_test()
    # print(cwd)
    # read_file_test('x', 'y')
    
    # server = 'WV-SQL03\ROSSERP'
    # drop_refs_table(server=server)
    # create_refs_table(server=server)
    # create_stub_loadrefs(server=server)
    # create_sp_loadrefs(server=server)
    # create_stub_sendtocentral(server=server)
    # create_sp_sendtocentral(server=server)    
    
    # create_sp_helptext2(server=server,database='master')
    # create_stub_helptext2(server=server,database='master')    
    
    
    # create_loadrefs_procedure()
    # create_sp_loadrefs()
    # print('test')
    # truncate_facilities_stage()
    # tsql = f"""spLoad_facilities;"""
    # sql.execute_sql(tsql)