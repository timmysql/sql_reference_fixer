
CREATE TABLE cms.ReferencedEntities  ( 
						id int not null identity(1,1) primary key,
						 SchemaObject nvarchar(255) 
					,[home_server_name] [nvarchar](255) NULL
					,[home_database_name] [nvarchar](255) NULL
					,[home_schema_name] [nvarchar](255) NULL
					,[home_entity_name] [nvarchar](255) NULL
					,[home_entity_type] [nvarchar](255) NULL

					,[home_entity_create] [nvarchar](255) NULL
					,[home_entity_alter] [nvarchar](255) NULL
						
						
						,referenced_server_name nvarchar(255)
						,referenced_database_name nvarchar(255)
						,referenced_schema_name nvarchar(255)
						,referenced_entity_name	 nvarchar(255)
						
						,desired_qualified_count int

						,current_qualified_count int 

						,qualified_server_name nvarchar(255)
						,qualified_database_name nvarchar(255)
						,qualified_schema_name nvarchar(255)
						,qualified_entity_name	 nvarchar(255)

						,four_part_qualified nvarchar(255)
						,three_part_qualified nvarchar(255)
						,two_part_qualified nvarchar(255)
						,one_part_qualified nvarchar(255)
						
						,error_retrieving_info nvarchar(max) 
						,sp_refresh_text nvarchar(255)
						,home_entity_name_conflict int
						,create_dt datetime default GETDATE()

)
