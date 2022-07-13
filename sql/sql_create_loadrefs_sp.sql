
ALTER PROCEDURE [cms].[usp_LoadReferencedEntities]
AS
BEGIN SET NOCOUNT ON;
--DECLARE @ObjectType VARCHAR(255) = 'SQL_STORED_PROCEDURE'
				
DECLARE @UseDatabase NVARCHAR(255) --= 'MBA_COMPANY_INFORMATION'				

		  DECLARE @ErrorMessage varchar(MAX), 
				@ErrorSeverity int,
				@ErrorState smallint

				
DECLARE @RunDate datetime = GETDATE()

DECLARE @SchemaName varchar(10)		
DECLARE @ObjectName varchar(255)
DECLARE @SchemaObject varchar(255)
DECLARE @TypeDesc varchar(255)

DECLARE @SQL NVARCHAR(MAX) 
DECLARE @SQL_OBJECTS NVARCHAR(MAX) 

IF OBJECT_ID('tempdb..#TBL') IS NOT NULL 
BEGIN 
    DROP TABLE #TBL 
END

IF OBJECT_ID('tempdb..#TBL_OBJECTS') IS NOT NULL 
BEGIN 
    DROP TABLE #TBL_OBJECTS
END


IF OBJECT_ID('cms.ReferencedEntities_Exceptions', 'u') IS NOT NULL 
DROP TABLE cms.ReferencedEntities_Exceptions;

--IF OBJECT_ID('cms.ReferencedEntities', 'u') IS NOT NULL 
--DROP TABLE cms.ReferencedEntities;

CREATE TABLE cms.ReferencedEntities_Exceptions (
		SchemaName varchar(100), 
		ObjectName varchar(100), 
		SchemaObject varchar(100), 
		UseDatabase varchar(100), 
		TypeDesc varchar(100), 
		SqlText nvarchar(max), 
		ErrorMessage nvarchar(max), 
		ErrorSeverity int,  
		ErrorState smallint, 
		InsertDate datetime default getdate(),
		RunDate datetime
		)




 
IF OBJECT_ID('cms.ReferencedEntities', 'u') IS NOT NULL 
DROP TABLE cms.ReferencedEntities;

--IF OBJECT_ID('cms.ReferencedEntities', 'u') IS NOT NULL 
--DROP TABLE cms.ReferencedEntities;

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



CREATE TABLE #TBL  ( 
						 SchemaObject nvarchar(255) 
						,DatabaseName nvarchar(255)
						,SchemaName   nvarchar(255)
						,ObjectName   nvarchar(255)
						,TypeDesc   nvarchar(255)
						,referenced_server_name nvarchar(255)
						,referenced_database_name nvarchar(255)
						,referenced_schema_name nvarchar(255)
						,referenced_entity_name	 nvarchar(255)
						,qualified_server_name nvarchar(255)
						,qualified_database_name nvarchar(255)
						,qualified_schema_name nvarchar(255)
						,qualified_entity_name	 nvarchar(255)
						,error_retrieving_info nvarchar(max) 
						,sp_refresh_text nvarchar(255)

)


CREATE TABLE #TBL_OBJECTS (
						 SchemaName nvarchar(255) 
						,ObjectName nvarchar(255)
						,SchemaObject  nvarchar(255)
						,UseDatabase  nvarchar(255)
						,TypeDesc nvarchar(100) 

)




DECLARE _OUTERCURSOR cursor
FOR 

				SELECT 
						name 
				FROM master.sys.databases 
				--WHERE name in ('MBA_COMPANY_INFORMATION')
				WHERE name not in ('master','tempdb','model','msdb'	,
				'SharePoint_AdminContent_a6c4d921-4781-41f1-8174-c705247be89c',
				'WSS_Content_8c0926a1b87c40eca65df9d3245eb88d'	,
				'Bdc_Service_DB_821bd1b844fa41e78b0ccd69f01e56dc',
				'ARCHIVE'
				)



OPEN _OUTERCURSOR

WHILE 0 = 0 
BEGIN FETCH NEXT FROM _OUTERCURSOR
INTO @UseDatabase
IF @@FETCH_STATUS <> 0 BREAK 
PRINT('UseDatabase: ' + @UseDatabase)
SET @SQL_OBJECTS = 
'
USE @UseDatabase

SELECT

		SCHEMA_NAME(o.schema_id) SchemaName
		, o.name ObjectName
		,SCHEMA_NAME(o.schema_id) + ''.'' + o.name  SchemaObject 
		,''@UseDatabase'' UseDatabase
		,type_desc

FROM 
		@UseDatabase.sys.objects o
		inner join @UseDatabase.sys.schemas s
		on o.schema_id = s.schema_id
WHERE 
		o.is_ms_shipped = 0
AND		o.type in (''U'', ''FN'', ''FS'', ''FT'', ''IF'', ''P'', ''PC'', ''TA'', ''TF'', ''TR'', ''V'')
AND		SCHEMA_NAME(o.schema_id) NOT IN (''zdl'')
AND		LEFT(o.name,1) NOT IN (''z'',''x'')

AND o.name not in (
					''sp_helpdiagrams''
					,''sp_helpdiagramdefinition''
					,''sp_creatediagram''
					,''sp_renamediagram''
					,''sp_alterdiagram''
					,''sp_dropdiagram''
					,''sp_upgraddiagrams''
					)'
SET @SQL_OBJECTS = REPLACE(@SQL_OBJECTS,'@UseDatabase', @UseDatabase)
--SET @SQL_OBJECTS = REPLACE(@SQL_OBJECTS ,'@ObjectType',@ObjectType )

--PRINT @SQL_OBJECTS
TRUNCATE TABLE #TBL_OBJECTS 
INSERT INTO #TBL_OBJECTS 
EXECUTE sp_executesql @SQL_OBJECTS
	



				DECLARE _INNERCURSOR CURSOR
				FOR 
			
				SELECT 
						 SchemaName
						,ObjectName
						,SchemaObject
						,UseDatabase 
						,TypeDesc 
				FROM #TBL_OBJECTS

				OPEN _INNERCURSOR



				WHILE 0 = 0
				BEGIN
				FETCH NEXT FROM _INNERCURSOR
				INTO @SchemaName, @ObjectName, @SchemaObject, @UseDatabase, @TypeDesc
				IF @@FETCH_STATUS <> 0 BREAK
				/*******************************************
				--do stuff 
				*********************************************/
				
		

				SET @SQL = '
				USE @UseDatabase
				SELECT  
						''@SchemaObject'' SchemaObject
						,''@UseDatabase'' UseDataBase
						,''@SchemaName'' SchemaName
						,''@ObjectName'' ObjectName 
						,''@TypeDesc'' TypeDesc 
						
						,ISNULL(referenced_server_name,'''')						
						,ISNULL(referenced_database_name,'''')						
						,ISNULL(referenced_schema_name,'''')
						,referenced_entity_name	

						,ISNULL(referenced_server_name,@@SERVERNAME)
						,ISNULL(referenced_database_name,''@UseDatabase'') referenced_database_name 
						,CASE WHEN ISNULL(referenced_schema_name,'''') = '''' THEN ''dbo'' ELSE referenced_schema_name END 
						,referenced_entity_name	

						,'''' error_retrieving_info
						,'''' sp_refresh_text
				FROM 
				@UseDatabase.sys.dm_sql_referenced_entities(''@SchemaObject'',''OBJECT'')
				--WHERE referenced_id IS NOT NULL

				GROUP BY 
						referenced_server_name
						,referenced_database_name
						,referenced_schema_name
						,referenced_entity_name
				'

				SET @SQL = REPLACE(@SQL,'@UseDatabase',  @UseDatabase)
				SET @SQL = REPLACE(@SQL,'@SchemaName',   @SchemaName)
				SET @SQL = REPLACE(@SQL,'@ObjectName',   @ObjectName)
				SET @SQL = REPLACE(@SQL,'@TypeDesc',   @TypeDesc)
				SET @SQL = REPLACE(@SQL,'@SchemaObject', @SchemaObject)

				--BEGIN TRANSACTION;  
				BEGIN TRY 
				--PRINT @SQL
				INSERT INTO #TBL
				EXECUTE sp_executesql @SQL


				END TRY
				BEGIN CATCH
					  SET @ErrorMessage = ERROR_MESSAGE()
					  SET @ErrorSeverity = ERROR_SEVERITY()
				      SET @ErrorState = ERROR_STATE()
					  PRINT @SQL
       
					   RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState)
					   WITH NOWAIT --RETURNS THE MESSAGE TO SCREEN IMMEDIATELY 
				END CATCH

			--BEGIN CATCH
		
	
			--		  --SELECT
			--			--ERROR_NUMBER() AS ErrorNumber  
			--			--,ERROR_SEVERITY() AS ErrorSeverity  
			--			--,ERROR_STATE() AS ErrorState  
			--			--,ERROR_PROCEDURE() AS ErrorProcedure  
			--			--,ERROR_LINE() AS ErrorLine  
			--			--,ERROR_MESSAGE() AS ErrorMessage;  

			--				INSERT INTO cms.ReferencedEntities_Exceptions 
			--				(
			--					SchemaName, 
			--					ObjectName , 
			--					SchemaObject, 
			--					UseDatabase, 
			--					TypeDesc , 
			--					SqlText , 
			--					ErrorMessage, 
			--					ErrorSeverity,  
			--					ErrorState,
			--					RunDate 
			--					) 
			--				VALUES (@SchemaName, @ObjectName, @SchemaObject, @UseDatabase, @TypeDesc, @SQL, ERROR_MESSAGE() , ERROR_SEVERITY(),ERROR_STATE(),@RunDate)		



			--			IF @@TRANCOUNT > 0  
			--			BEGIN
			--				PRINT('ATTEMPTING ROLLBACK')
			--				ROLLBACK TRANSACTION;	
			--			--	INSERT INTO cms.ReferencedEntities_Exceptions 
			--			--	(
			--			--		SchemaName, 
			--			--		ObjectName , 
			--			--		SchemaObject, 
			--			--		UseDatabase, 
			--			--		TypeDesc , 
			--			--		SqlText , 
			--			--		ErrorMessage, 
			--			--		ErrorSeverity,  
			--			--		ErrorState,
			--			--		RunDate 
			--			--		) 
			--			--	VALUES (@SchemaName, @ObjectName, @SchemaObject, @UseDatabase, @TypeDesc, @SQL, @ErrorMessage, @ErrorSeverity,@ErrorState,@RunDate)							
			--			END

			--	END CATCH 				
			--	IF @@TRANCOUNT > 0  
			--		COMMIT TRANSACTION;  



				END

				CLOSE _INNERCURSOR
				DEALLOCATE _INNERCURSOR


END

CLOSE _OUTERCURSOR
DEALLOCATE _OUTERCURSOR


INSERT INTO cms.ReferencedEntities
(
	  -- [SchemaObject]
	  --,[DatabaseName]
   --   ,[SchemaName]
   --   ,[ObjectName]
	  --,[TypeDesc]

		SchemaObject 
		,[home_server_name] 
		,[home_database_name] 
		,[home_schema_name] 
		,[home_entity_name] 
		,[home_entity_type] 
      ,[referenced_server_name]
      ,[referenced_database_name]
      ,[referenced_schema_name]
      ,[referenced_entity_name]

		,qualified_server_name 
		,qualified_database_name
		,qualified_schema_name 
		,qualified_entity_name

      ,[error_retrieving_info]
)
SELECT
	   [SchemaObject]
	  ,@@SERVERNAME
	  ,[DatabaseName]
      ,[SchemaName]
      ,[ObjectName]
	  ,[TypeDesc]
      ,[referenced_server_name]
      ,[referenced_database_name]
      ,[referenced_schema_name]
      ,[referenced_entity_name]

		,qualified_server_name 
		,qualified_database_name
		,qualified_schema_name 
		,qualified_entity_name

      ,[error_retrieving_info]

FROM 
		#TBL --WHERE referenced
WHERE ObjectName not in ('spADJUSTMENT_CREATE_MOVE') 



UPDATE cms.ReferencedEntities
SET 
		four_part_qualified   =   '[' + qualified_server_name + ']' + '.' + '[' + qualified_database_name + ']' + '.' + '[' + qualified_schema_name + ']' + '.' + '[' + qualified_entity_name + ']'
		,three_part_qualified =                                       '[' + qualified_database_name + ']' + '.' + '[' + qualified_schema_name + ']' + '.' + '[' + qualified_entity_name + ']'
		,two_part_qualified   =                                                                             '[' + qualified_schema_name + ']' + '.' + '[' + qualified_entity_name + ']'
		,one_part_qualified   =                                                                                                                 '[' + qualified_entity_name + ']'


UPDATE cms.ReferencedEntities
SET current_qualified_count = 
	  CASE 
			WHEN [referenced_server_name] = ''
				AND [referenced_database_name] = ''
				AND [referenced_schema_name] = ''
				AND [referenced_entity_name] <> ''
			THEN 1
			WHEN [referenced_server_name] = ''
				AND [referenced_database_name] = ''
				AND [referenced_schema_name] <> ''
				AND [referenced_entity_name] <> ''
			THEN 2
			WHEN [referenced_server_name] = ''
				AND [referenced_database_name] <> ''
				AND [referenced_schema_name] <> ''
				AND [referenced_entity_name] <> ''
			THEN 3
			
			WHEN [referenced_server_name] = ''
				AND [referenced_database_name] <> ''
				AND [referenced_schema_name] = ''
				AND [referenced_entity_name] <> ''
			THEN 5 -- Database..object

			WHEN [referenced_server_name] <> ''
				AND [referenced_database_name] <> ''
				AND [referenced_schema_name] <> ''
				AND [referenced_entity_name] <> ''
			THEN 4
			ELSE NULL END 


UPDATE cms.ReferencedEntities
SET desired_qualified_count = 1
WHERE home_database_name = qualified_database_name

UPDATE cms.ReferencedEntities
SET desired_qualified_count = 4
WHERE ISNULL(desired_qualified_count,0) <> 1



UPDATE cms.ReferencedEntities
SET sp_refresh_text = 
	  CASE WHEN [home_entity_type]  IN ('SQL_STORED_PROCEDURE','SQL_TABLE_VALUED_FUNCTION','SQL_SCALAR_FUNCTION') 

				THEN 'EXEC sys.sp_refreshsqlmodule @name = ''' + home_entity_name + ''''

			WHEN [home_entity_type]  = 'VIEW'  

				THEN 'EXEC sys.sp_refreshview @viewname = ''' + home_entity_name + ''''

			ELSE NULL END


UPDATE cms.ReferencedEntities
SET home_entity_create = 

	  CASE 
			WHEN [home_entity_type] = 'SQL_STORED_PROCEDURE'		THEN 'CREATE PROCEDURE ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			WHEN [home_entity_type] = 'SQL_TABLE_VALUED_FUNCTION'	THEN 'CREATE FUNCTION ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			WHEN [home_entity_type] = 'SQL_SCALAR_FUNCTION'			THEN 'CREATE FUNCTION ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			WHEN [home_entity_type]  = 'VIEW'						THEN 'CREATE VIEW ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			END 
,home_entity_alter = 
		CASE 
			WHEN [home_entity_type] = 'SQL_STORED_PROCEDURE'		THEN 'ALTER PROCEDURE ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			WHEN [home_entity_type] = 'SQL_TABLE_VALUED_FUNCTION'	THEN 'ALTER FUNCTION ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			WHEN [home_entity_type] = 'SQL_SCALAR_FUNCTION'			THEN 'ALTER FUNCTION ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			WHEN [home_entity_type]  = 'VIEW'						THEN 'ALTER VIEW ' + '[' + home_schema_name + '].[' + home_entity_name + ']'
			END
		FROM cms.ReferencedEntities



SELECT DISTINCT 
		a.home_entity_name, b.home_entity_name entity_name_in_other, a.home_server_name, a.home_database_name
INTO #CONFLICTS
FROM cms.ReferencedEntities a
CROSS JOIN cms.ReferencedEntities b

WHERE a.home_entity_name <> b.home_entity_name
AND a.home_entity_name like '%' + b.home_entity_name + '%'

AND a.home_server_name = b.home_server_name
and a.home_database_name = b.home_database_name


UPDATE cms.ReferencedEntities
SET home_entity_name_conflict = 0 

UPDATE cms.ReferencedEntities
SET home_entity_name_conflict = 1
FROM (SELECT DISTINCT entity_name_in_other, home_server_name, home_database_name FROM #CONFLICTS) x
WHERE 
	cms.ReferencedEntities.home_entity_name =  x.entity_name_in_other
AND cms.ReferencedEntities.home_server_name = x.home_server_name
AND cms.ReferencedEntities.home_database_name = x.home_database_name

----AND a.home_entity_name = b.home_entity_name 
DROP TABLE #CONFLICTS

END ;

