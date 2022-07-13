import inspect 
# import re
import os 
import time
import sqlmodel_main as main
# import junk.main as main


pre_or_suf = 'prefix'
glb_print = 0
print_function = False
print_debug = False

output_dir = 'Output'    
dateStr = time.strftime("%Y-%m-%d-%H-%M")  # For use as part of file name.
pre_or_suf = 'prefix' 
file_output = os.getcwd() + "\\" + output_dir 
file_root = os.getcwd() + "\\" + output_dir + "\\" + dateStr + "\\"

debug_replacement = False


# # Using regular expressions to ignore case issues
# def i_replace(search, replace, string):    
#     if print_function == True: print(inspect.stack()[0][3])
#     search = re.escape(search)
#     # print('search:  ' + search)
#     # print('replace: ' + replace)
#     # print('string:  ' + string)
#     pattern = re.compile(search, re.IGNORECASE)
#     # print(pattern)
#     return_string = pattern.sub(replace, string)
#     input("i_replace Enter to continue...")
#     # return_string = re.sub(search, replace, string, flags=re.I)
#     return return_string
# def swap_dictionary(create_type, home_schema_name, home_entity_name):
    
#     create_v1 = f"""CREATE {create_type} {home_schema_name}.{home_entity_name}"""   
#     create_v2 = f"""CREATE {create_type} [{home_schema_name}].{home_entity_name}"""   
#     create_v3 = f"""CREATE {create_type} [{home_schema_name}].[{home_entity_name}]"""   
#     create_v4 = f"""CREATE {create_type} {home_schema_name}.[{home_entity_name}]"""   
#     create_v5 = f"""CREATE {create_type} [{home_entity_name}]"""   
#     create_v6 = f"""CREATE {create_type} {home_entity_name}"""
    
#     # create_v7 = f"""CREATE PROC {home_schema_name}.{home_entity_name}"""   
#     # create_v8 = f"""CREATE {create_type} [{home_schema_name}].{home_entity_name}"""   
#     # create_v9 = f"""CREATE {create_type} [{home_schema_name}].[{home_entity_name}]"""   
#     # create_v10 = f"""CREATE {create_type} {home_schema_name}.[{home_entity_name}]"""   
#     # create_v11 = f"""CREATE {create_type} [{home_entity_name}]"""   
#     # create_v12 = f"""CREATE {create_type} {home_entity_name}"""    
    
    
   
#     my_dict = {"create_v1": create_v1, "create_v2": create_v2 ,"create_v3": create_v3, "create_v4": create_v4, "create_v5": create_v5, "create_v6": create_v6}
#     # for key, value in my_dict.items():
#     #     print(key) 
#     #     print(value)
#     return my_dict    

def swap_dictionary(create_type, home_schema_name, home_entity_name):
    my_list = []
    my_list.append(f"""CREATE {create_type} {home_schema_name}.{home_entity_name}""")
    my_list.append(f"""CREATE {create_type} [{home_schema_name}].{home_entity_name}""")
    my_list.append(f"""CREATE {create_type} [{home_schema_name}].[{home_entity_name}]""")
    my_list.append(f"""CREATE {create_type} {home_schema_name}.[{home_entity_name}]""")
    my_list.append(f"""CREATE {create_type} [{home_entity_name}]""")
    my_list.append(f"""CREATE {create_type} {home_entity_name}""")
    
    if create_type == 'PROCEDURE':
        my_list.append(f"""CREATE PROC {home_schema_name}.{home_entity_name}""")
        my_list.append(f"""CREATE PROC [{home_schema_name}].{home_entity_name}""")
        my_list.append(f"""CREATE PROC [{home_schema_name}].[{home_entity_name}]""")
        my_list.append(f"""CREATE PROC {home_schema_name}.[{home_entity_name}]""")
        my_list.append(f"""CREATE PROC [{home_entity_name}]""")
        my_list.append(f"""CREATE PROC {home_entity_name}""")
    
    my_dict = {}
    prefix = 'create_v'

    for i in my_list:
        idx = my_list.index(i)
        my_dict |= {prefix+str(idx):i}

    # for key, value in my_dict.items():
    #     print(key, value) 


    return my_dict 
    
    
def swap_looper(txt, create_dict, alter_entity):
    if print_function == True: print(inspect.stack()[0][3])
    tmp_txt = txt.replace(' ', '')
    for key, value in create_dict.items():

        if value.replace(' ', '') in tmp_txt:
            
            create_tmp = value
            new_txt = txt.replace(create_tmp, alter_entity + ' ') 
            return new_txt
        else:
            new_txt = txt
        
    return new_txt      


def create_entity_swapper(txt, home_schema_name, home_entity_name, home_entity_type, alter_entity):
    if print_function == True: print(inspect.stack()[0][3])
    txt = txt
    tmp_txt = txt.replace(' ','') 
    # print('txt: ' + txt)
    # print('tmp_txt: ' + tmp_txt)
    
    # print('alter_entity: ' + alter_entity)
    create_type = 'PROCEDURE'
    if home_entity_type == 'SQL_STORED_PROCEDURE':
        create_type == 'PROCEDURE'                        
    if home_entity_type == 'VIEW':
        create_type = 'VIEW'
    if home_entity_type == 'SQL_SCALAR_FUNCTION':
        create_type = 'FUNCTION'
    if home_entity_type == 'SQL_INLINE_TABLE_VALUED_FUNCTION':
        create_type = 'FUNCTION'
    if home_entity_type == 'SQL_TABLE_VALUED_FUNCTION':
        create_type = 'FUNCTION'
        

    if create_type =='PROCEDURE':
        # print('hey... a procedure!!!!')
        create_dict = swap_dictionary(create_type=create_type, home_schema_name=home_schema_name, home_entity_name=home_entity_name)
        new_txt = swap_looper(txt, create_dict, alter_entity)
        return new_txt
        # print('new_txt: ' + new_txt)
        create_dict = swap_dictionary(create_type='PROC', home_schema_name=home_schema_name, home_entity_name=home_entity_name)
        new_txt = swap_looper(txt, create_dict, alter_entity)                
        return new_txt

    if create_type !='PROCEDURE':
        # print('hey... a procedure!!!!')
        create_dict = swap_dictionary(create_type=create_type, home_schema_name=home_schema_name, home_entity_name=home_entity_name)
        new_txt = swap_looper(txt, create_dict, alter_entity)
        # print('new_txt: ' + new_txt)
        return new_txt
    # return new_txt

def replace_strings(df, text_to_search):
    if print_function == True: print(inspect.stack()[0][3])
    # print(df.index)
    content_string = text_to_search
    # print('Text to Search', text_to_search.lower())
    for idx, rw in df.iterrows():
    
        search_orig = rw.Search
        replace_orig = rw.Replace
        text_orig = text_to_search
        
        search_prc = search_orig.lower().rstrip()
        # replace_prc = replace_orig
        text_prc = text_orig.lower().rstrip()
        
        # if print_debug == True:        
        # print('search_orig: ' + rw.Search)
        # print('replace_orig: ' + rw.Replace)
        # print('text_orig: ' + text_to_search)
        
        # print('search_prc: ' + rw.Search.lower().rstrip())
        # print('replace_prc: ' + rw.Replace)
        # print('text_prc: ' + text_to_search.lower().rstrip())        
        
            
        if search_prc in text_prc:
            # input('thing...................................')
            if debug_replacement:
                print('##################################################################################################################')
                print('Found: ' + search_prc)
                print('In: ' + text_prc)
                print('##################################################################################################################')
            
            len_srch_prc = len(search_prc)
            len_txt_orig = len(search_prc)
            # if print_debug == True:
            # input('thing2.........................')
            # print('len_srch_orig: ' + str(len(search_orig)))
            # print('len_srch_prc: ' + str(len(search_prc)))
            # print('len_txt_orig: ' + str(len(text_orig)))
            # print('len_txt_prc: ' + str(len(text_prc)))  
                  
            begin_position = text_prc.find(search_prc)
            if print_debug == True:            
                print('huh: ' + str(begin_position))   
                           
            
            # [wv-sql2].[ignitiondb].[dbo].[printreasoncode]
            if print_debug == True:            
                print(text_orig[begin_position:len_txt_orig+begin_position])
                
            begin_txt = text_orig[:begin_position]
            end_txt = text_orig[len_txt_orig+begin_position:]
                                    
            replace_string = begin_txt + replace_orig + end_txt
                    
            content_string = replace_string
            
            # content_string = i_replace(rw.Search, rw.Replace, content_string)
            # print('content_string: ' + content_string)
            # input("Press Enter to continue...")
            return content_string
        # return content_string
    return content_string


def content_replacer(sql_text_to_search, brackets_list, original_file_name, replace_file_name, alter_file_name, create_file_name, home_schema_name,home_entity_name,home_entity_type,alter_entity, current_server_ref):
    if print_function == True: print(inspect.stack()[0][3])
    original_content = ""    
    replace_content = ""
    alter_content = ""
    create_content = ""
    # replace_content += "USE [" + home_obj_db + "]\n"
    go_content = "GO" + "\n"
        
    # df = sql_text_to_search
    max_index = len(sql_text_to_search)
    # print(max_index)
    
    for i in range(max_index):        
        txt = sql_text_to_search[i].Text 
        new_txt = txt
        create_content_string = replace_strings(brackets_list, txt.rstrip())
        if 'CREATE' in new_txt:
            new_txt = create_entity_swapper(txt=new_txt, home_schema_name=home_schema_name, home_entity_name=home_entity_name, home_entity_type=home_entity_type, alter_entity=alter_entity)                 
            alter_content_string = replace_strings(brackets_list, new_txt.rstrip())
            content_string = replace_strings(brackets_list, new_txt.rstrip())
            # print('the hell if I know #####################################################')
            
            # input('what... ')
        else:
            alter_content_string = replace_strings(brackets_list, txt.rstrip())
            content_string = replace_strings(brackets_list, txt.rstrip())
            if debug_replacement == True:
                if current_server_ref.lower() in txt.lower():
                    print(brackets_list)
                    print('txt: ' + txt.rstrip())    
                    print('new_txt: ' + new_txt.rstrip()) 
                    print('content_string: ' + content_string.rstrip())
                    input('YO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!........')
                
        
        if 'CREATE PROC ' in alter_content:
            alter_content = alter_content.replace('CREATE PROC ', 'ALTER PROCEDURE ')
        if 'CREATE PROC ' in alter_content:
            alter_content = alter_content.replace('CREATE PROC ', 'CREATE PROCEDURE ')        
        
        alter_content += alter_content_string.rstrip() + "\n"
        create_content += create_content_string.rstrip() + "\n"
        replace_content += content_string.rstrip() + "\n"
        original_content += txt.rstrip() + "\n" 
        
        
        # if current_server_ref.lower() in txt.lower():    
        # print('replace_content: ' + replace_content)
        # print('original_content: ' + original_content)
        # input('stop........')

        
    # print(replace_content)        
    main.write_file_contents(replace_file_name, replace_content)
    main.write_file_contents(replace_file_name, go_content)

    # print(original_content)
    main.write_file_contents(original_file_name, original_content)
    main.write_file_contents(original_file_name, go_content)
        
    # print(replace_content)        
    main.write_file_contents(alter_file_name, alter_content)
    main.write_file_contents(alter_file_name, go_content)
    
    # print(replace_content)        
    main.write_file_contents(create_file_name, create_content)
    main.write_file_contents(create_file_name, go_content)            
    

    
   





if __name__ == '__main__':
    txt = 'CREATE PROCEDURE [dba].[DatabaseBackupDurations_Collect]'
    home_schema_name = 'dba'
    home_entity_name = 'DatabaseBackupDurations_Collect'
    home_entity_type = 'SQL_STORED_PROCEDURE'
    alter_entity = """ALTER PROCEDURE [dba].[DatabaseBackupDurations_Collect]"""
    
    create_entity_swapper(txt, home_schema_name, home_entity_name, home_entity_type, alter_entity)
    # swap_helper(create_type='PROCEDURE', home_schema_name='dbo', home_entity_name='spNameOfProcedure')
    # exec(open("main.py").read())
    # main(self)
    # print(1)
    
    
    
    