IF OBJECT_ID('cms.spReferencedEntities_SendToCentral') IS  NULL
    EXEC ('CREATE PROCEDURE cms.spReferencedEntities_SendToCentral AS RETURN 138;');
