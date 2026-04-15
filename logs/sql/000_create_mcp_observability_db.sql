/*
Create a dedicated SQL Server database for MCP observability.

Run as an account with CREATE DATABASE permissions (typically DBA / sysadmin).
*/
GO

-- Change this value only if you want a different observability DB name.
DECLARE @db_name SYSNAME = N'McpObservability';

IF DB_ID(@db_name) IS NULL
BEGIN
    BEGIN TRY
        DECLARE @sql_create NVARCHAR(MAX) = N'CREATE DATABASE [' + REPLACE(@db_name, N']', N']]') + N'];';
        EXEC(@sql_create);
    END TRY
    BEGIN CATCH
        DECLARE @create_err NVARCHAR(2048) =
            N'Failed to create observability DB [' + @db_name + N']: ' + ERROR_MESSAGE()
            + N'. Ask DBA to run this script or pre-create the database.';
        THROW 51010, @create_err, 1;
    END CATCH;
END
GO

DECLARE @db_name SYSNAME = N'McpObservability';
IF DB_ID(@db_name) IS NULL
    THROW 51011, 'Observability DB was not found after create step. Aborting.', 1;

BEGIN TRY
    DECLARE @sql NVARCHAR(MAX) = N'
    ALTER DATABASE [' + REPLACE(@db_name, N']', N']]') + N'] SET READ_COMMITTED_SNAPSHOT ON;
    ALTER DATABASE [' + REPLACE(@db_name, N']', N']]') + N'] SET AUTO_CLOSE OFF;
    ';
    EXEC(@sql);
END TRY
BEGIN CATCH
    -- Database exists; if ALTER permission is missing we continue with defaults.
    PRINT 'Warning: ALTER DATABASE options were skipped: ' + ERROR_MESSAGE();
END CATCH;
GO

PRINT 'Database ready: McpObservability';
