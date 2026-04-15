/*
Create MCP logging objects inside the dedicated observability database.

Run after:
    logs/sql/000_create_mcp_observability_db.sql
*/
USE [McpObservability];
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'mcp_logging') IS NULL
    EXEC('CREATE SCHEMA [mcp_logging]');
GO

IF OBJECT_ID(N'[mcp_logging].[events]', N'U') IS NULL
BEGIN
    CREATE TABLE [mcp_logging].[events]
    (
        [event_id] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_mcp_logging_events] PRIMARY KEY,
        [ts_utc] DATETIME2(3) NOT NULL,
        [trace_id] NVARCHAR(64) NULL,
        [session_id] NVARCHAR(128) NULL,
        [request_id] NVARCHAR(128) NULL,
        [event_type] NVARCHAR(100) NOT NULL,
        [transport] NVARCHAR(50) NULL,
        [method] NVARCHAR(120) NULL,
        [tool_name] NVARCHAR(120) NULL,
        [download_mode] NVARCHAR(20) NULL,
        [row_count] INT NULL,
        [agent_think_ms] INT NULL,
        [sql_exec_ms] INT NULL,
        [total_ms] INT NULL,
        [sql_preview] NVARCHAR(400) NULL,
        [user_prompt] NVARCHAR(1000) NULL,
        [error] NVARCHAR(2000) NULL,
        [payload_json] NVARCHAR(MAX) NOT NULL,
        [inserted_at_utc] DATETIME2(3) NOT NULL CONSTRAINT [DF_mcp_logging_events_inserted_at_utc] DEFAULT (SYSUTCDATETIME())
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_mcp_logging_events_ts_utc' AND object_id = OBJECT_ID(N'[mcp_logging].[events]'))
    CREATE INDEX [IX_mcp_logging_events_ts_utc] ON [mcp_logging].[events]([ts_utc] DESC);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_mcp_logging_events_trace_ts' AND object_id = OBJECT_ID(N'[mcp_logging].[events]'))
    CREATE INDEX [IX_mcp_logging_events_trace_ts] ON [mcp_logging].[events]([trace_id], [ts_utc] DESC);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_mcp_logging_events_session_ts' AND object_id = OBJECT_ID(N'[mcp_logging].[events]'))
    CREATE INDEX [IX_mcp_logging_events_session_ts] ON [mcp_logging].[events]([session_id], [ts_utc] DESC);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_mcp_logging_events_event_type_ts' AND object_id = OBJECT_ID(N'[mcp_logging].[events]'))
    CREATE INDEX [IX_mcp_logging_events_event_type_ts] ON [mcp_logging].[events]([event_type], [ts_utc] DESC);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_mcp_logging_events_tool_ts' AND object_id = OBJECT_ID(N'[mcp_logging].[events]'))
    CREATE INDEX [IX_mcp_logging_events_tool_ts] ON [mcp_logging].[events]([tool_name], [ts_utc] DESC);
GO

CREATE OR ALTER VIEW [mcp_logging].[v_trace_summary]
AS
SELECT
    [trace_id],
    MIN([ts_utc]) AS [trace_started_utc],
    MAX([ts_utc]) AS [trace_ended_utc],
    COUNT_BIG(*) AS [event_count],
    SUM(CASE WHEN [event_type] LIKE '%error%' THEN 1 ELSE 0 END) AS [error_count],
    MAX([total_ms]) AS [max_total_ms]
FROM [mcp_logging].[events]
WHERE [trace_id] IS NOT NULL AND LTRIM(RTRIM([trace_id])) <> ''
GROUP BY [trace_id];
GO

CREATE OR ALTER VIEW [mcp_logging].[v_session_summary]
AS
SELECT
    [session_id],
    MIN([ts_utc]) AS [session_started_utc],
    MAX([ts_utc]) AS [session_last_seen_utc],
    COUNT_BIG(*) AS [event_count],
    SUM(CASE WHEN [event_type] = 'mcp_sse_session_opened' THEN 1 ELSE 0 END) AS [open_events],
    SUM(CASE WHEN [event_type] = 'mcp_sse_session_closed' THEN 1 ELSE 0 END) AS [close_events]
FROM [mcp_logging].[events]
WHERE [session_id] IS NOT NULL AND LTRIM(RTRIM([session_id])) <> ''
GROUP BY [session_id];
GO
