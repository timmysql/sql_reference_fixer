
import pandas as pd
# import itertools 
# import inspect

print_function = False
debug_print = False 


# def search_ref_brackets(*args):
#     ref = ''
#     # print(len(args))
#     for value in args:
#         # print(value)
#         ref += value
                                                
def bracket_reference_combos(refs, future_server_ref, desired_qualified_count):
    search_df = pd.DataFrame(columns=['Search', 'Replace'])
    # print('    REFERENCES')
    # print('    -------------------------------------------------------------------------')
    for ref in refs:
        # print('    ', ref.referenced_server_name, ref.referenced_database_name, ref.referenced_schema_name, ref.referenced_entity_name)
   
        ref_server = ref.referenced_server_name
        ref_db = ref.referenced_database_name
        ref_schema = ref.referenced_schema_name
        ref_entity = ref.referenced_entity_name

        if desired_qualified_count == 3:
            replace_string = (f"""[{ref_db}].[{ref_schema}].[{ref_entity}]""")
        if  desired_qualified_count == 4:
            replace_string = (f"""[{future_server_ref}].[{ref_db}].[{ref_schema}].[{ref_entity}]""")            
                        
        
        if ref.current_qualified_count == 4:
            search_df = search_df.append({'Search': (f"""{ref_server}.{ref_db}.{ref_schema}.{ref_entity} """),
                                        'Replace': replace_string}, ignore_index=True)      
                                          
            search_df = search_df.append({'Search': (f"""[{ref_server}].{ref_db}.{ref_schema}.{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f"""[{ref_server}].[{ref_db}].{ref_schema}.{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f"""[{ref_server}].[{ref_db}].[{ref_schema}].{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f"""[{ref_server}].[{ref_db}].[{ref_schema}].[{ref_entity}] """),                                                                                         
                                        'Replace': replace_string}, ignore_index=True)        
            
            search_df = search_df.append({'Search': (f"""{ref_server}.[{ref_db}].[{ref_schema}].[{ref_entity}] """),     
                                        'Replace': replace_string}, ignore_index=True)        
            
            search_df = search_df.append({'Search': (f"""{ref_server}.{ref_db}.[{ref_schema}].[{ref_entity}] """),                                              
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f"""{ref_server}.{ref_db}.{ref_schema}.[{ref_entity}] """),                                              
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f"""{ref_server}.[{ref_db}].[{ref_schema}].{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f"""{ref_server}.[{ref_db}].{ref_schema}.{ref_entity} """),                                                  
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f"""{ref_server}.{ref_db}.[{ref_schema}].{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
          
            search_df = search_df.append({'Search': (f"""[{ref_server}].{ref_db}.{ref_schema}.[{ref_entity}] """),
                                        'Replace': replace_string}, ignore_index=True)                              
          
            search_df = search_df.append({'Search': (f"""[{ref_server}].{ref_db}.[{ref_schema}].{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
          
            search_df = search_df.append({'Search': (f"""{ref_server}.[{ref_db}].{ref_schema}.[{ref_entity}] """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
          
            search_df = search_df.append({'Search': (f"""[{ref_server}].[{ref_db}].{ref_schema}.[{ref_entity}] """),   
                                        'Replace': replace_string}, ignore_index=True)
          
            search_df = search_df.append({'Search': (f"""[{ref_server}].{ref_db}.[{ref_schema}].[{ref_entity}] """),         
                                        'Replace': replace_string}, ignore_index=True)    
            
      
        
        if ref.current_qualified_count == 3:
            search_df = search_df.append({'Search': (f""" {ref_db}.{ref_schema}.{ref_entity} """), 
                                        'Replace': replace_string}, ignore_index=True)                                               

           
            search_df = search_df.append({'Search': (f""" [{ref_db}].{ref_schema}.{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)                    
            
            search_df = search_df.append({'Search': (f""" [{ref_db}].[{ref_schema}].{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)                   
           
            search_df = search_df.append({'Search': (f""" [{ref_db}].[{ref_schema}].[{ref_entity}] """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
            
            search_df = search_df.append({'Search': (f""" {ref_db}.[{ref_schema}].[{ref_entity}] """),                                                                                         
                                        'Replace': replace_string}, ignore_index=True)        
           
            search_df = search_df.append({'Search': (f""" {ref_db}.{ref_schema}.[{ref_entity}] """),     
                                        'Replace': replace_string}, ignore_index=True)        
            
            search_df = search_df.append({'Search': (f""" [{ref_db}].{ref_schema}.[{ref_entity}] """),                                              
                                        'Replace': replace_string}, ignore_index=True)        
            
            search_df = search_df.append({'Search': (f""" {ref_db}.[{ref_schema}].{ref_entity} """),                                              
                                        'Replace': replace_string}, ignore_index=True)          
            
        if ref.current_qualified_count == 2:
            search_df = search_df.append({'Search': (f""" {ref_schema}.{ref_entity} """),
                                        'Replace': replace_string}, ignore_index=True)                                    
            search_df = search_df.append({'Search': (f""" [{ref_schema}].{ref_entity} """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
            search_df = search_df.append({'Search': (f""" {ref_schema}.[{ref_entity}] """),                                      
                                        'Replace': replace_string}, ignore_index=True)        
            search_df = search_df.append({'Search': (f""" [{ref_schema}].[{ref_entity}] """),                                      
                                        'Replace': replace_string}, ignore_index=True)                                
             
        if ref.current_qualified_count == 1:
            search_df = search_df.append({'Search': (f""" {ref_entity} """),
                                        'Replace': replace_string}, ignore_index=True)                                    
            search_df = search_df.append({'Search': (f""" [{ref_entity}] """),                                      
                                        'Replace': replace_string}, ignore_index=True)            
                       

    return search_df


# def bracketizer(ref_server,ref_db,ref_schema,ref_entity):
#     if print_function == True: print(inspect.stack()[0][3])
#     brk_ref_server = f"""[{ref_server}]"""
#     f"""{ref_server}.{ref_db}.{ref_schema}.{ref_entity}"""

# def product_dict(**kwargs):
#     if print_function == True: print(inspect.stack()[0][3])
#     keys = kwargs.keys()
#     vals = kwargs.values()
#     for instance in itertools.product(*vals):
#         yield dict(zip(keys, instance))


if __name__ == '__main__': 
    pass
    # bracket_dictionary('server','database','schema','object',4)
    # product_dict(**kwargs) 
      
    # search_ref_brackets('server','database','schema','object')