/*
Create least-privilege access roles for MCP logging and a retention procedure.

Run after:
    logs/sql/001_create_mcp_logging.sql
*/

/*
Edit these values before running:
    Writer login/user:  mcp_loger / mcp_loger
    Reader login/user:  mcp_analytics_login / mcp_analytics_user
*/

USE [McpObservability];
GO

IF OBJECT_ID(N'[mcp_logging].[events]', N'U') IS NULL
    THROW 51000, 'Table [mcp_logging].[events] was not found. Run 001_create_mcp_logging.sql first.', 1;
GO

IF DATABASE_PRINCIPAL_ID(N'mcp_logging_writer') IS NULL
    CREATE ROLE [mcp_logging_writer];
GO

IF DATABASE_PRINCIPAL_ID(N'mcp_logging_reader') IS NULL
    CREATE ROLE [mcp_logging_reader];
GO

GRANT INSERT ON [mcp_logging].[events] TO [mcp_logging_writer];
GRANT SELECT ON [mcp_logging].[v_trace_summary] TO [mcp_logging_reader];
GRANT SELECT ON [mcp_logging].[v_session_summary] TO [mcp_logging_reader];
GRANT SELECT ON [mcp_logging].[events] TO [mcp_logging_reader];
GO

IF SUSER_ID(N'mcp_loger') IS NOT NULL AND DATABASE_PRINCIPAL_ID(N'mcp_loger') IS NULL
BEGIN
    DECLARE @sql_create_writer_user NVARCHAR(MAX) =
        N'CREATE USER [' + REPLACE(N'mcp_loger', N']', N']]') + N'] FOR LOGIN [' + REPLACE(N'mcp_loger', N']', N']]') + N'];';
    EXEC(@sql_create_writer_user);
END
GO

IF DATABASE_PRINCIPAL_ID(N'mcp_loger') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.database_role_members drm
       INNER JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
       INNER JOIN sys.database_principals m ON m.principal_id = drm.member_principal_id
       WHERE r.name = N'mcp_logging_writer'
         AND m.name = N'mcp_loger'
   )
BEGIN
    DECLARE @sql_add_writer_role NVARCHAR(MAX) =
        N'ALTER ROLE [mcp_logging_writer] ADD MEMBER [' + REPLACE(N'mcp_loger', N']', N']]') + N'];';
    EXEC(@sql_add_writer_role);
END
GO

IF SUSER_ID(N'mcp_analytics_login') IS NOT NULL AND DATABASE_PRINCIPAL_ID(N'mcp_analytics_user') IS NULL
BEGIN
    DECLARE @sql_create_reader_user NVARCHAR(MAX) =
        N'CREATE USER [' + REPLACE(N'mcp_analytics_user', N']', N']]') + N'] FOR LOGIN [' + REPLACE(N'mcp_analytics_login', N']', N']]') + N'];';
    EXEC(@sql_create_reader_user);
END
GO

IF DATABASE_PRINCIPAL_ID(N'mcp_analytics_user') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.database_role_members drm
       INNER JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
       INNER JOIN sys.database_principals m ON m.principal_id = drm.member_principal_id
       WHERE r.name = N'mcp_logging_reader'
         AND m.name = N'mcp_analytics_user'
   )
BEGIN
    DECLARE @sql_add_reader_role NVARCHAR(MAX) =
        N'ALTER ROLE [mcp_logging_reader] ADD MEMBER [' + REPLACE(N'mcp_analytics_user', N']', N']]') + N'];';
    EXEC(@sql_add_reader_role);
END
GO

CREATE OR ALTER PROCEDURE [mcp_logging].[usp_purge_events_older_than_days]
    @retention_days INT = 90,
    @batch_size INT = 5000
AS
BEGIN
    SET NOCOUNT ON;

    IF @retention_days < 1
        THROW 51001, '@retention_days must be >= 1.', 1;
    IF @batch_size < 100
        THROW 51002, '@batch_size must be >= 100.', 1;

    DECLARE @cutoff DATETIME2(3) = DATEADD(DAY, -@retention_days, SYSUTCDATETIME());
    DECLARE @rows INT = 1;

    WHILE @rows > 0
    BEGIN
        DELETE TOP (@batch_size)
        FROM [mcp_logging].[events]
        WHERE [ts_utc] < @cutoff;

        SET @rows = @@ROWCOUNT;
    END
END
GO

GRANT EXECUTE ON [mcp_logging].[usp_purge_events_older_than_days] TO [mcp_logging_writer];
GO

PRINT 'Security roles and retention procedure created in McpObservability.';
