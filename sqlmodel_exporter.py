# import pyodbc
# import main_bracketizer as brk
# import main_content_replacer as rpl
# import main_reference_lists as lst
# import main 
import sql_server_generic as sql 
import inspect 
print_function = False


# def get_sql_text(home_server_name, home_database_name, home_schema_name, home_entity_name ):

def print_kwargs(**kwargs):
    # print('printing kwargs ###################################################################')
    if print_function == True: print(inspect.stack()[0][3])
    for i, v in kwargs.items():
        print ("    ", i, ": ", v)
    # input("Press Enter to continue...")


class GetSqlText:
    def __init__(self, home_server_name, home_database_name, home_schema_name, home_entity_name):
        self.home_server_name = home_server_name
        self.home_database_name = home_database_name
        self.home_schema_name = home_schema_name
        self.home_entity_name = home_entity_name

    def get_sql_text(self):        
        exp_sql = f"""EXEC sp_helptext2 N'[{self.home_schema_name}].[{self.home_entity_name}]';"""
        sql_text_to_search = sql.return_data(exp_sql,self.home_server_name, self.home_database_name)        
        return sql_text_to_search
    
    
if __name__ == '__main__':
    pass
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # object = 'sp_WhoIsActive12345'
        
    # home_server = 'SQL05'
    # reference_table_db = 'DBATools'  
    # current_server_ref = 'WV-SQL2'  
    # future_server_ref = 'SQL05' 
    # referenced_db = 'RS'     
    # any_referenced_db = True  
    
    # home_obj_kwargs = {"home_server_name": "SQL05",
    #                     "home_database_name": "DBATools",
    #                     "home_schema_name": "dbo",
    #                     "home_entity_name": object,
    #                     "home_entity_type": "STORED PROCEDURE",
    #                     "future_server_ref": "YEE"
        
        
    # }       
    
        
    # kwargs = {"current_server_ref": current_server_ref, 
    #             "referenced_db": referenced_db, 
    #             "home_server": home_server, 
    #             "reference_table_db": reference_table_db, 
    #             "any_referenced_db": any_referenced_db}    
    # kwargs.update(home_obj_kwargs)
    
    
    # print_kwargs(**kwargs)
    # sql_text = get_sql_text(**kwargs)
    # # print(sql_text)
    # # for i in sql_text:
    # #     print(i[0])
    
    # original_content = ""
    # # original_content += "USE [" + home_obj_db + "]\n"
    # # original_content += "GO" + "\n"

    # replace_content = ""
    # # replace_content += "USE [" + home_obj_db + "]\n"
    # go_content = "GO" + "\n"    


    # max_index = len(sql_text)        
    # for i in range(max_index):        
    #     txt = sql_text[i].Text  
    #     print(txt)
    #     if 'CREATE PROC' in txt:
    #         print('foudn thing!!!')
    #         print(f"""CREATE PROCEDURE [{kwargs['home_schema_name']}].[{kwargs['home_entity_name']}]""")
            
    #         input("Press Enter to continue...")      
    #     # content_string = replace_strings(brackets_list, txt.rstrip())
    #     # # print(content_string)
    #     # replace_content += content_string.rstrip() + "\n"
    #     # original_content += txt.rstrip() + "\n"                
    #     # print(str(i) + ' ' + str(content_string))        