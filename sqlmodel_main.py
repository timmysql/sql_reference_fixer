# import new_supporting_objects as nso
import sql_server_generic as sql
# import new_reference_processing as ref
# import configs as cfg
import time
import os, shutil
# import pandas as pd
# import re
import inspect
# import junk.main_reference_lists as lst
# import sqlmodel_exporter as ex

# import main_bracketizer as brz
import sqlmodel_content_replacer as rpl
from sqlmodel_models import GetReferences, GetObjectReferences
from sqlmodel_exporter import GetSqlText
import sqlmodel_bracketizer as brz
from load_progress import Progress

# customs
# import sp_helptext2
# import sp_loadrefs
# import junk.main_reference_lists as lst
# import sys


pre_or_suf = 'prefix'
glb_print = 0
print_function = False
kwarg_printer = False

output_dir = 'Output'    
dateStr = time.strftime("%Y-%m-%d-%H-%M")  # For use as part of file name.
pre_or_suf = 'prefix' 
file_output = os.getcwd() + "\\" + output_dir 
file_root = os.getcwd() + "\\" + output_dir + "\\" + dateStr + "\\"

# def progress(count, total, status=''):
#     bar_len = 60
#     filled_len = int(round(bar_len * count / float(total)))

#     percents = round(100.0 * count / float(total), 1)
#     bar = '=' * filled_len + '-' * (bar_len - filled_len)

#     sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
#     sys.stdout.flush()  # As suggested by Rom Ruben (see: http://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console/27871113#comment50529068_27871113)

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')
    
    
class BaseAttributes:
    def __init__(self):

        self.pre_or_suf = 'prefix'
        self.glb_print = 0
        self.print_function = False
        self.kwarg_printer = False
        self.output_dir = 'Output'    
        self.dateStr = time.strftime("%Y-%m-%d-%H-%M")  # For use as part of file name.
        self.pre_or_suf = 'prefix' 
        self.file_output = os.getcwd() + "\\" + output_dir 
        self.file_root = os.getcwd() + "\\" + output_dir + "\\" + dateStr + "\\"                
        

def execute_load_refs(server, database):
    if print_function == True: print(inspect.stack()[0][3])
    tsql=f"""EXEC cms.usp_LoadReferencedEntities"""
    sql.execute_sql(tsql=tsql, server=server, database=database)    
    
    
def create_output_dir():
    if print_function == True: print(inspect.stack()[0][3])
    out_dir = file_root
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        # print('did not find ' + out_dir + ', so i created it')    

def create_output_dir():
    x = BaseAttributes()
    if print_function == True: print(inspect.stack()[0][3])
    out_dir = x.file_root
    if not os.path.exists(out_dir):
        os.makedirs(out_dir) 
    return out_dir     

 
def file_initializer(home_server_name, home_database_name):
        if print_function == True: print(inspect.stack()[0][3])
        # df = list_all_objects(referenced_server='wv-SQL01', referenced_db='VANDE_BERG')
        # db_names = (df[["home_database_name"]].drop_duplicates())    
        # for idx_s, rw_s in db_names.iterrows():
            # us = MultiRefs(server, rw_s.DatabaseName, '', '', '')
                        
        original_file_name = get_file_name(home_server_name, home_database_name, 'original')
        replace_file_name = get_file_name(home_server_name, home_database_name, 'replace')
        alter_file_name = get_file_name(home_server_name, home_database_name, 'alter')
        create_file_name = get_file_name(home_server_name, home_database_name, 'create')
        
        # initialize file necessary to insert USE [DB] GO statement at beginning of file
        initialize_file(original_file_name, home_database_name)
        initialize_file(replace_file_name, home_database_name) 
        initialize_file(alter_file_name, home_database_name)
        initialize_file(create_file_name, home_database_name)         
 
def initialize_file(filename, home_obj_db):
    if print_function == True: print(inspect.stack()[0][3])
    use_text = "USE [" + home_obj_db + "]\n"
    go_text = "GO" + "\n"
    # print(filename)
    write_file_contents(filename, use_text)
    write_file_contents(filename, go_text) 
 
 
def write_file_contents(file_name, content):
    if print_function == True: print(inspect.stack()[0][3])
    f_replace = open(file_root + file_name, "a+",encoding='utf-8')
    f_replace.write(content)
    f_replace.close() 
 

    
def get_file_name(home_obj_server, home_obj_db, extra):
    if print_function == True: print(inspect.stack()[0][3])
    return_file_name = ''
    if home_obj_server.lower() == "wv-sql03\\ROSSERP".lower():
        base_file_name = "ROSSERP" + "-" + home_obj_db 
    else:
        base_file_name = home_obj_server + "-" + home_obj_db

    if pre_or_suf == 'prefix':
        return_file_name = extra + '_' + base_file_name + '.sql'
    elif pre_or_suf == 'suffix':
        return_file_name = base_file_name + '_' + extra + '.sql'
    return return_file_name




def delete_output_files():    
    folder = file_output
    # print(folder)
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        # print(filename)
        # print(file_path)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
                # os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(f"""{file_path}\\""")
            # os.rmdir(file_path) 
            
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))




class MainThing:
        
    def __init__(self):
        self.home_server_name = 'WV-SQL03\ROSSERP'
        self.home_database_name = None
        # self.home_database_ignore 
        self.reference_table_db = 'CMS_Node' 
                
        self.referenced_server_name = 'PT-SQL04' 
        self.future_server_ref = 'SQL04'
        self.desired_qualified_count = 4
        
        
    
    def file_cleanup(self):
        delete_output_files()            
        # clear_terminal()
        
    def create_output_dir(self): 
        out_dir = create_output_dir()          
        return out_dir
    
    def initialize_files(self):
        x = GetReferences(home_server_name=self.home_server_name, 
                          home_database_name=self.home_database_name, 
                          referenced_server_name=self.referenced_server_name
                          )
        home_server_dbs = x.home_server_dbs()
        for row in home_server_dbs:
            file_initializer(home_server_name=row.home_server_name, home_database_name=row.home_database_name)
            
            
        ###############################################################################################
        # GET HOME OBJECTS
        ###############################################################################################            
        home_objects = x.list_home_objects()
        total = len(home_objects)
        progress = Progress(max=total, custom_text = f"thing:") 
        for obj in home_objects:
            # print('######################################################################################')
            # print('OBJECT')
            # print(obj.home_server_name, obj.home_database_name, obj.home_schema_name, obj.home_entity_name)
            
            ###############################################################################################
            # GET SQL TEXT
            ###############################################################################################
            x = GetSqlText(home_server_name=obj.home_server_name, home_database_name=obj.home_database_name, home_schema_name=obj.home_schema_name, home_entity_name=obj.home_entity_name)
            sql_text = x.get_sql_text()
            # print(sql_text)
               
            ###############################################################################################
            # GET REFERENCED OBJECTS
            ###############################################################################################                                                         
            y = GetObjectReferences(referenced_server_name = self.referenced_server_name,
                              home_server_name=obj.home_server_name, 
                              home_database_name=obj.home_database_name,
                              home_schema_name=obj.home_schema_name, 
                              home_entity_name=obj.home_entity_name)           
                              

            ref_objects = y.list_referenced_objects()
 
            ###############################################################################################
            # GET BRACKETS LIST
            ###############################################################################################  
            brackets_list = brz.bracket_reference_combos(refs=ref_objects, 
                                         future_server_ref=self.future_server_ref, 
                                         desired_qualified_count= self.desired_qualified_count )          
                
            original_file_name = get_file_name(self.home_server_name, obj.home_database_name, 'original')
            replace_file_name = get_file_name(self.home_server_name, obj.home_database_name, 'replace')   
            alter_file_name = get_file_name(self.home_server_name, obj.home_database_name, 'alter')
            create_file_name = get_file_name(self.home_server_name, obj.home_database_name, 'create')                     
                
                
            ###############################################################################################
            # REPLACE CONTENT
            ###############################################################################################                
            rpl.content_replacer(sql_text, 
                                brackets_list, 
                                original_file_name, 
                                replace_file_name, 
                                alter_file_name, 
                                create_file_name,
                                home_schema_name = obj.home_schema_name,
                                home_entity_name = obj.home_entity_name,
                                home_entity_type = obj.home_entity_type,
                                alter_entity =     obj.home_entity_alter,
                                current_server_ref = self.referenced_server_name 
            )  
            progress.update()              
                
                

        



if __name__ == '__main__':

    m = MainThing()
    m.file_cleanup()
    out_dir = m.create_output_dir()
    # print('out_dir:', out_dir)
    m.initialize_files()

                                 
                                 

      