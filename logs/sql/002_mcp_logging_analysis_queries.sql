/* Quick analysis queries for MCP logs in SQL Server */

USE [McpObservability];
GO

/* Recent events */
SELECT TOP (200)
    event_id,
    ts_utc,
    trace_id,
    session_id,
    event_type,
    tool_name,
    total_ms,
    sql_exec_ms,
    row_count,
    error
FROM mcp_logging.events
ORDER BY ts_utc DESC;

/* Error rate by tool (last 24h) */
SELECT
    tool_name,
    COUNT(*) AS total_events,
    SUM(CASE WHEN event_type LIKE '%error%' THEN 1 ELSE 0 END) AS error_events,
    CAST(100.0 * SUM(CASE WHEN event_type LIKE '%error%' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS error_rate_pct
FROM mcp_logging.events
WHERE ts_utc >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY tool_name
ORDER BY error_rate_pct DESC, total_events DESC;

/* Slow traces */
SELECT TOP (100)
    trace_id,
    trace_started_utc,
    trace_ended_utc,
    event_count,
    error_count,
    max_total_ms
FROM mcp_logging.v_trace_summary
ORDER BY max_total_ms DESC, trace_ended_utc DESC;

/* Active/abnormal sessions */
SELECT TOP (200)
    session_id,
    session_started_utc,
    session_last_seen_utc,
    event_count,
    open_events,
    close_events
FROM mcp_logging.v_session_summary
ORDER BY session_last_seen_utc DESC;
