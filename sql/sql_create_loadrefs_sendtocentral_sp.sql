ALTER PROCEDURE cms.spReferencedEntities_SendToCentral
AS
BEGIN 

DELETE FROM [CMSSQL].[DBA_CMS].[dbo].[ReferencedEntities_Central]
WHERE home_server_name = @@SERVERNAME 


INSERT INTO [CMSSQL].[DBA_CMS].[dbo].[ReferencedEntities_Central]
(
      [SchemaObject]
      ,[home_server_name]
      ,[home_database_name]
      ,[home_schema_name]
      ,[home_entity_name]
      ,[home_entity_type]
      ,[home_entity_create]
      ,[home_entity_alter]
      ,[referenced_server_name]
      ,[referenced_database_name]
      ,[referenced_schema_name]
      ,[referenced_entity_name]
      ,[desired_qualified_count]
      ,[current_qualified_count]
      ,[qualified_server_name]
      ,[qualified_database_name]
      ,[qualified_schema_name]
      ,[qualified_entity_name]
      ,[four_part_qualified]
      ,[three_part_qualified]
      ,[two_part_qualified]
      ,[one_part_qualified]
      ,[error_retrieving_info]
      ,[sp_refresh_text]
      ,[home_entity_name_conflict]
	  ) 
SELECT 
       [SchemaObject]
      ,[home_server_name]
      ,[home_database_name]
      ,[home_schema_name]
      ,[home_entity_name]
      ,[home_entity_type]
      ,[home_entity_create]
      ,[home_entity_alter]
      ,[referenced_server_name]
      ,[referenced_database_name]
      ,[referenced_schema_name]
      ,[referenced_entity_name]
      ,[desired_qualified_count]
      ,[current_qualified_count]
      ,[qualified_server_name]
      ,[qualified_database_name]
      ,[qualified_schema_name]
      ,[qualified_entity_name]
      ,[four_part_qualified]
      ,[three_part_qualified]
      ,[two_part_qualified]
      ,[one_part_qualified]
      ,[error_retrieving_info]
      ,[sp_refresh_text]
      ,[home_entity_name_conflict]
  FROM [DBATools].[cms].[ReferencedEntities]

  END 