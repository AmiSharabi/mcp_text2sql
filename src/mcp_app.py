import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable

import uvicorn
from dotenv import load_dotenv
from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from src.mcp_models import (
    BuildChartRequest,
    BuildDashboardRequest,
    DownloadResultRequest,
    ExecuteReadonlySqlRequest,
    ExplainReasoningRequest,
    PreviewTableRequest,
)
from src.mcp_runtime import (
    SSE_SESSIONS,
    STREAMABLE_HTTP_SESSIONS,
    enforce_auth,
    log_request_end,
    log_request_start,
    mcp_error_response,
    post_only_hint,
    run_mcp_payload,
    sse_event,
    trace_id_from_header,
)
from src.tools import (
    build_chart,
    build_dashboard,
    download_readonly_sql_result,
    execute_readonly_sql,
    explain_reasoning,
    get_schema,
    list_databases,
    preview_table,
)

load_dotenv()

app = FastAPI(title='MCP Text2SQL Server', version='0.1.0')
DOWNLOADS_DIR = Path(os.getenv('DOWNLOADS_DIR', 'logs/downloads'))
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
app.mount('/downloads', StaticFiles(directory=str(DOWNLOADS_DIR)), name='downloads')


def _json_safe(value: Any) -> Any:
    # Convert nested payloads into JSON-serializable structures (Decimal/datetime/etc.).
    return jsonable_encoder(value)


def _run_http_tool_endpoint(
    request: Request,
    x_trace_id: str | None,
    tool_name: str,
    user_prompt: str,
    runner: Callable[[str, str], dict[str, Any]],
) -> dict[str, Any]:
    # Run a tool endpoint with shared auth, timing, logging, and HTTP error mapping.
    enforce_auth(request)
    trace_id = trace_id_from_header(x_trace_id)
    started = time.perf_counter()
    log_request_start(trace_id, tool_name, user_prompt=user_prompt, transport='http_tool')

    try:
        return runner(trace_id, user_prompt)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail='Internal server error') from exc
    finally:
        total_ms = int((time.perf_counter() - started) * 1000)
        log_request_end(
            trace_id,
            tool_name,
            total_ms,
            user_prompt=user_prompt,
            transport='http_tool',
        )


@app.get('/')
def root() -> dict[str, str]:
    # Return basic service status and endpoint usage hints.
    return {
        'status': 'ok',
        'message': (
            'Server is running. Use POST /mcp for MCP tools/list and tools/call '
            'or GET /sse + POST /messages for SSE transport '
            'or POST on /tools/get_schema, /tools/execute_readonly_sql, /tools/explain_reasoning, '
            '/tools/preview_table, /tools/download_result, /tools/build_chart, /tools/build_dashboard, '
            '/tools/list_databases'
        ),
    }


@app.get('/health')
def health() -> dict[str, str]:
    # Return lightweight liveness status for health checks.
    return {'status': 'ok'}


@app.post('/mcp')
@app.post('/mcp/')
async def mcp_rpc(
    request: Request,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Handle JSON-RPC over HTTP on /mcp.
    enforce_auth(request)

    try:
        payload = await request.json()
    except Exception:
        return mcp_error_response(None, -32700, 'Invalid JSON')

    if not isinstance(payload, dict):
        return mcp_error_response(None, -32600, 'Invalid request')

    return run_mcp_payload(
        payload=payload,
        request=request,
        x_trace_id=x_trace_id,
        session_id=None,
        transport='jsonrpc_http',
    )


@app.get('/mcp')
@app.get('/mcp/')
def get_mcp_hint() -> dict[str, str]:
    # Explain that /mcp must be called with POST.
    return {'status': 'error', 'message': 'Use POST /mcp with method tools/list or tools/call'}


@app.get('/sse')
@app.get('/sse/')
async def sse_endpoint(request: Request) -> StreamingResponse:
    # Open an SSE session and stream queued MCP responses to the client.
    enforce_auth(request)
    session_id = str(uuid.uuid4())
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    SSE_SESSIONS[session_id] = queue

    from src.logger import log_event

    log_event(
        {
            'event_type': 'mcp_sse_session_opened',
            'session_id': session_id,
            'transport': 'sse',
        }
    )

    async def event_stream():
        endpoint_url = str(request.url_for('post_sse_message'))
        yield sse_event('endpoint', f'{endpoint_url}?session_id={session_id}')

        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield sse_event('message', json.dumps(_json_safe(message), ensure_ascii=True))
                except asyncio.TimeoutError:
                    yield ': keepalive\n\n'
        finally:
            SSE_SESSIONS.pop(session_id, None)
            log_event(
                {
                    'event_type': 'mcp_sse_session_closed',
                    'session_id': session_id,
                    'transport': 'sse',
                }
            )

    return StreamingResponse(event_stream(), media_type='text/event-stream')


@app.post('/sse')
@app.post('/sse/')
async def post_sse_rpc(
    request: Request,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> JSONResponse:
    # Handle streamable HTTP MCP requests on /sse and return JSON-RPC response.
    enforce_auth(request)
    try:
        payload = await request.json()
    except Exception:
        response = mcp_error_response(None, -32700, 'Invalid JSON')
        return JSONResponse(_json_safe(response), status_code=200)

    if not isinstance(payload, dict):
        response = mcp_error_response(None, -32600, 'Invalid request')
        return JSONResponse(_json_safe(response), status_code=200)

    method = payload.get('method')
    session_id_header = request.headers.get('mcp-session-id')

    response_headers: dict[str, str] = {}
    session_id_for_log: str | None = session_id_header
    if method == 'initialize':
        session_id_for_log = str(uuid.uuid4())
        STREAMABLE_HTTP_SESSIONS[session_id_for_log] = time.time()
        response_headers['Mcp-Session-Id'] = session_id_for_log

    response = run_mcp_payload(
        payload=payload,
        request=request,
        x_trace_id=x_trace_id,
        session_id=session_id_for_log,
        transport='streamable_http',
    )

    if isinstance(method, str) and method.startswith('notifications/'):
        return Response(status_code=202, headers=response_headers)

    return JSONResponse(_json_safe(response), status_code=200, headers=response_headers)


@app.post('/messages')
@app.post('/messages/')
async def post_sse_message(
    request: Request,
    session_id: str,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Accept MCP payloads for a session and enqueue results for SSE delivery.
    enforce_auth(request)
    queue = SSE_SESSIONS.get(session_id)
    if queue is None:
        raise HTTPException(status_code=404, detail='Unknown session_id')

    try:
        payload = await request.json()
    except Exception:
        payload = None

    if not isinstance(payload, dict):
        response = mcp_error_response(None, -32700, 'Invalid JSON')
    else:
        response = run_mcp_payload(
            payload=payload,
            request=request,
            x_trace_id=x_trace_id,
            session_id=session_id,
            transport='sse',
        )

    await queue.put(_json_safe(response))
    return {'ok': True}


@app.post('/tools/get_schema')
@app.post('/tools/get_schema/')
def post_get_schema(
    request: Request,
    database: str | None = None,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the get_schema tool over plain HTTP endpoint.
    req_prompt = 'get schema'
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='get_schema',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: get_schema(
            trace_id,
            user_prompt=user_prompt,
            database=database,
        ),
    )


@app.get('/tools/get_schema')
@app.get('/tools/get_schema/')
def get_get_schema_hint() -> dict[str, str]:
    # Explain that the get_schema tool endpoint requires POST.
    return post_only_hint('get_schema')


@app.post('/tools/execute_readonly_sql')
@app.post('/tools/execute_readonly_sql/')
def post_execute_readonly_sql(
    request: Request,
    body: ExecuteReadonlySqlRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the execute_readonly_sql tool over plain HTTP endpoint.
    req_prompt = body.sql
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='execute_readonly_sql',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: execute_readonly_sql(
            trace_id=trace_id,
            sql=body.sql,
            user_prompt=user_prompt,
            database=body.database,
        ),
    )


@app.get('/tools/execute_readonly_sql')
@app.get('/tools/execute_readonly_sql/')
def get_execute_readonly_sql_hint() -> dict[str, str]:
    # Explain that the execute_readonly_sql endpoint requires POST.
    return post_only_hint('execute_readonly_sql')


@app.post('/tools/explain_reasoning')
@app.post('/tools/explain_reasoning/')
def post_explain_reasoning(
    request: Request,
    body: ExplainReasoningRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the explain_reasoning tool over plain HTTP endpoint.
    req_prompt = body.question
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='explain_reasoning',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: explain_reasoning(
            trace_id=trace_id,
            question=body.question,
            chosen_tables=body.chosen_tables,
            sql=body.sql,
            user_prompt=user_prompt,
        ),
    )


@app.get('/tools/explain_reasoning')
@app.get('/tools/explain_reasoning/')
def get_explain_reasoning_hint() -> dict[str, str]:
    # Explain that the explain_reasoning endpoint requires POST.
    return post_only_hint('explain_reasoning')


@app.post('/tools/preview_table')
@app.post('/tools/preview_table/')
def post_preview_table(
    request: Request,
    body: PreviewTableRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the preview_table tool over plain HTTP endpoint.
    req_prompt = f'preview {body.schema_name}.{body.table_name}'
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='preview_table',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: preview_table(
            trace_id=trace_id,
            table_name=body.table_name,
            schema_name=body.schema_name,
            user_prompt=user_prompt,
            database=body.database,
        ),
    )


@app.get('/tools/preview_table')
@app.get('/tools/preview_table/')
def get_preview_table_hint() -> dict[str, str]:
    # Explain that the preview_table endpoint requires POST.
    return post_only_hint('preview_table')


@app.post('/tools/download_result')
@app.post('/tools/download_result/')
def post_download_result(
    request: Request,
    body: DownloadResultRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the download_result tool over plain HTTP endpoint.
    req_prompt = body.sql
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='download_result',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: download_readonly_sql_result(
            trace_id=trace_id,
            sql=body.sql,
            file_name=body.file_name,
            download_mode=body.download_mode,
            user_prompt=user_prompt,
            database=body.database,
        ),
    )


@app.get('/tools/download_result')
@app.get('/tools/download_result/')
def get_download_result_hint() -> dict[str, str]:
    # Explain that the download_result endpoint requires POST.
    return post_only_hint('download_result')


@app.post('/tools/build_chart')
@app.post('/tools/build_chart/')
def post_build_chart(
    request: Request,
    body: BuildChartRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the build_chart tool over plain HTTP endpoint.
    req_prompt = body.title or body.sql
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='build_chart',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: build_chart(
            trace_id=trace_id,
            sql=body.sql,
            chart_type=body.chart_type,
            x_field=body.x_field,
            y_field=body.y_field,
            series_field=body.series_field,
            title=body.title,
            user_prompt=user_prompt,
            database=body.database,
        ),
    )


@app.get('/tools/build_chart')
@app.get('/tools/build_chart/')
def get_build_chart_hint() -> dict[str, str]:
    # Explain that the build_chart endpoint requires POST.
    return post_only_hint('build_chart')


@app.post('/tools/build_dashboard')
@app.post('/tools/build_dashboard/')
def post_build_dashboard(
    request: Request,
    body: BuildDashboardRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the build_dashboard tool over plain HTTP endpoint.
    req_prompt = body.title or 'build dashboard'
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='build_dashboard',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: build_dashboard(
            trace_id=trace_id,
            widgets=body.widgets,
            title=body.title,
            user_prompt=user_prompt,
            database=body.database,
        ),
    )


@app.get('/tools/build_dashboard')
@app.get('/tools/build_dashboard/')
def get_build_dashboard_hint() -> dict[str, str]:
    # Explain that the build_dashboard endpoint requires POST.
    return post_only_hint('build_dashboard')


@app.post('/tools/list_databases')
@app.post('/tools/list_databases/')
def post_list_databases(
    request: Request,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    # Execute the list_databases tool over plain HTTP endpoint.
    req_prompt = 'list databases'
    return _run_http_tool_endpoint(
        request=request,
        x_trace_id=x_trace_id,
        tool_name='list_databases',
        user_prompt=req_prompt,
        runner=lambda trace_id, user_prompt: list_databases(
            trace_id=trace_id,
            user_prompt=user_prompt,
        ),
    )


@app.get('/tools/list_databases')
@app.get('/tools/list_databases/')
def get_list_databases_hint() -> dict[str, str]:
    # Explain that the list_databases endpoint requires POST.
    return post_only_hint('list_databases')


def main() -> None:
    # Start the FastAPI server using host/port from environment.
    host = os.getenv('MCP_HOST', '0.0.0.0')
    port = int(os.getenv('MCP_PORT', '8000'))
    uvicorn.run(app, host=host, port=port)
