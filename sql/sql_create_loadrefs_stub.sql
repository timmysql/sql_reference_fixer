IF OBJECT_ID('cms.usp_LoadReferencedEntities') IS  NULL
    EXEC ('CREATE PROCEDURE cms.usp_LoadReferencedEntities AS RETURN 138;');