import os
import time
import uuid
import json
from pathlib import Path
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from logger import log_event
from tools import execute_readonly_sql, explain_reasoning, get_schema, preview_table


load_dotenv()

app = FastAPI(title='MCP Text2SQL Server', version='0.1.0')


class ExecuteReadonlySqlRequest(BaseModel):
    sql: str = Field(..., min_length=1)


class ExplainReasoningRequest(BaseModel):
    question: str = Field(..., min_length=1)
    chosen_tables: list[str]
    sql: str = Field(..., min_length=1)


class PreviewTableRequest(BaseModel):
    table_name: str = Field(..., min_length=1)
    schema_name: str = Field(default='dbo', min_length=1)


_DEFAULT_TOOLS_PATH = Path(__file__).with_name('mcp_tools.json')


def _load_mcp_tool_definitions() -> list[dict[str, Any]]:
    tools_path_raw = os.getenv('MCP_TOOLS_PATH', str(_DEFAULT_TOOLS_PATH))
    tools_path = Path(tools_path_raw).expanduser()
    if not tools_path.is_absolute():
        tools_path = Path(__file__).resolve().parent / tools_path

    try:
        parsed = json.loads(tools_path.read_text(encoding='utf-8'))
    except FileNotFoundError as exc:
        raise RuntimeError(f'MCP tools file not found: {tools_path}') from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'Invalid JSON in MCP tools file: {tools_path}') from exc

    tools = parsed.get('tools')
    if not isinstance(tools, list):
        raise RuntimeError(f'MCP tools file must contain "tools" as a list: {tools_path}')

    for idx, tool_def in enumerate(tools):
        if not isinstance(tool_def, dict):
            raise RuntimeError(f'MCP tools item at index {idx} must be an object')
        if not isinstance(tool_def.get('name'), str) or not tool_def['name'].strip():
            raise RuntimeError(f'MCP tools item at index {idx} missing non-empty "name"')
        if not isinstance(tool_def.get('description'), str):
            raise RuntimeError(f'MCP tools item at index {idx} missing "description"')
        if not isinstance(tool_def.get('inputSchema'), dict):
            raise RuntimeError(f'MCP tools item at index {idx} missing "inputSchema"')

    return tools


MCP_TOOL_DEFINITIONS = _load_mcp_tool_definitions()


def _trace_id_from_header(trace_id_header: str | None) -> str:
    if trace_id_header and trace_id_header.strip():
        return trace_id_header.strip()
    return str(uuid.uuid4())


def _log_request_start(trace_id: str, tool_name: str) -> None:
    log_event({'trace_id': trace_id, 'event_type': 'request_start', 'tool_name': tool_name})


def _log_request_end(trace_id: str, tool_name: str, total_ms: int) -> None:
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'request_end',
            'tool_name': tool_name,
            'total_ms': total_ms,
        }
    )


@app.get('/')
def root() -> dict[str, str]:
    return {
        'status': 'ok',
        'message': (
            'Server is running. Use POST /mcp for MCP tools/list and tools/call '
            'or POST on /tools/get_schema, /tools/execute_readonly_sql, /tools/explain_reasoning, /tools/preview_table'
        ),
    }


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}


def _post_only_hint(tool_name: str) -> dict[str, str]:
    return {
        'status': 'error',
        'message': f'Use POST for /tools/{tool_name}',
    }


def _is_authorized(request: Request) -> bool:
    expected_key = os.getenv('MCP_API_KEY', '').strip()
    if not expected_key:
        # If no key configured, auth is disabled by design for local dev.
        return True

    provided_header = request.headers.get('x-api-key', '').strip()
    if provided_header and provided_header == expected_key:
        return True

    auth_header = request.headers.get('authorization', '').strip()
    if auth_header.lower().startswith('bearer '):
        token = auth_header[7:].strip()
        if token == expected_key:
            return True

    return False


def _enforce_auth(request: Request) -> None:
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail='Unauthorized')


def _mcp_error_response(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {
        'jsonrpc': '2.0',
        'id': request_id,
        'error': {
            'code': code,
            'message': message,
        },
    }


def _call_mcp_tool(name: str, arguments: dict[str, Any], trace_id: str) -> dict[str, Any]:
    if name == 'get_schema':
        return get_schema(trace_id)

    if name == 'execute_readonly_sql':
        sql = arguments.get('sql')
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError('execute_readonly_sql requires non-empty "sql"')
        return execute_readonly_sql(trace_id=trace_id, sql=sql)

    if name == 'explain_reasoning':
        question = arguments.get('question')
        chosen_tables = arguments.get('chosen_tables')
        sql = arguments.get('sql')

        if not isinstance(question, str) or not question.strip():
            raise ValueError('explain_reasoning requires non-empty "question"')
        if not isinstance(chosen_tables, list) or not all(isinstance(t, str) for t in chosen_tables):
            raise ValueError('explain_reasoning requires "chosen_tables" as list[str]')
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError('explain_reasoning requires non-empty "sql"')

        return explain_reasoning(
            trace_id=trace_id,
            question=question,
            chosen_tables=chosen_tables,
            sql=sql,
        )

    if name == 'preview_table':
        table_name = arguments.get('table_name')
        schema_name = arguments.get('schema_name', 'dbo')
        if not isinstance(table_name, str) or not table_name.strip():
            raise ValueError('preview_table requires non-empty "table_name"')
        if not isinstance(schema_name, str) or not schema_name.strip():
            raise ValueError('preview_table requires non-empty "schema_name"')
        return preview_table(trace_id=trace_id, table_name=table_name, schema_name=schema_name)

    raise ValueError(f'Unknown tool: {name}')


@app.post('/mcp')
@app.post('/mcp/')
async def mcp_rpc(
    request: Request,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    _enforce_auth(request)

    try:
        payload = await request.json()
    except Exception:
        return _mcp_error_response(None, -32700, 'Invalid JSON')

    request_id = payload.get('id')
    method = payload.get('method')

    if method == 'tools/list':
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {'tools': MCP_TOOL_DEFINITIONS},
        }

    if method == 'tools/call':
        params = payload.get('params')
        if not isinstance(params, dict):
            return _mcp_error_response(request_id, -32602, 'Invalid params')

        tool_name = params.get('name')
        if not isinstance(tool_name, str) or not tool_name.strip():
            return _mcp_error_response(request_id, -32602, 'Missing tool name')

        arguments = params.get('arguments', {})
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            return _mcp_error_response(request_id, -32602, 'Invalid tool arguments')

        trace_id = _trace_id_from_header(x_trace_id)
        started = time.perf_counter()
        _log_request_start(trace_id, tool_name)
        try:
            tool_result = _call_mcp_tool(tool_name, arguments, trace_id)
        except ValueError as exc:
            return _mcp_error_response(request_id, -32602, str(exc))
        except Exception:
            return _mcp_error_response(request_id, -32603, 'Internal server error')
        finally:
            total_ms = int((time.perf_counter() - started) * 1000)
            _log_request_end(trace_id, tool_name, total_ms)

        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {
                'content': [{'type': 'json', 'json': tool_result}],
            },
        }

    return _mcp_error_response(request_id, -32601, f'Unknown method: {method}')


@app.get('/mcp')
@app.get('/mcp/')
def get_mcp_hint() -> dict[str, str]:
    return {'status': 'error', 'message': 'Use POST /mcp with method tools/list or tools/call'}


@app.post('/tools/get_schema')
@app.post('/tools/get_schema/')
def post_get_schema(
    request: Request,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    _enforce_auth(request)
    trace_id = _trace_id_from_header(x_trace_id)
    started = time.perf_counter()
    _log_request_start(trace_id, 'get_schema')

    try:
        return get_schema(trace_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail='Internal server error') from exc
    finally:
        total_ms = int((time.perf_counter() - started) * 1000)
        _log_request_end(trace_id, 'get_schema', total_ms)


@app.get('/tools/get_schema')
@app.get('/tools/get_schema/')
def get_get_schema_hint() -> dict[str, str]:
    return _post_only_hint('get_schema')


@app.post('/tools/execute_readonly_sql')
@app.post('/tools/execute_readonly_sql/')
def post_execute_readonly_sql(
    request: Request,
    body: ExecuteReadonlySqlRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    _enforce_auth(request)
    trace_id = _trace_id_from_header(x_trace_id)
    started = time.perf_counter()
    _log_request_start(trace_id, 'execute_readonly_sql')

    try:
        return execute_readonly_sql(trace_id=trace_id, sql=body.sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail='Internal server error') from exc
    finally:
        total_ms = int((time.perf_counter() - started) * 1000)
        _log_request_end(trace_id, 'execute_readonly_sql', total_ms)


@app.get('/tools/execute_readonly_sql')
@app.get('/tools/execute_readonly_sql/')
def get_execute_readonly_sql_hint() -> dict[str, str]:
    return _post_only_hint('execute_readonly_sql')


@app.post('/tools/explain_reasoning')
@app.post('/tools/explain_reasoning/')
def post_explain_reasoning(
    request: Request,
    body: ExplainReasoningRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    _enforce_auth(request)
    trace_id = _trace_id_from_header(x_trace_id)
    started = time.perf_counter()
    _log_request_start(trace_id, 'explain_reasoning')

    try:
        return explain_reasoning(
            trace_id=trace_id,
            question=body.question,
            chosen_tables=body.chosen_tables,
            sql=body.sql,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail='Internal server error') from exc
    finally:
        total_ms = int((time.perf_counter() - started) * 1000)
        _log_request_end(trace_id, 'explain_reasoning', total_ms)


@app.get('/tools/explain_reasoning')
@app.get('/tools/explain_reasoning/')
def get_explain_reasoning_hint() -> dict[str, str]:
    return _post_only_hint('explain_reasoning')


@app.post('/tools/preview_table')
@app.post('/tools/preview_table/')
def post_preview_table(
    request: Request,
    body: PreviewTableRequest,
    x_trace_id: str | None = Header(default=None, alias='x-trace-id'),
) -> dict[str, Any]:
    _enforce_auth(request)
    trace_id = _trace_id_from_header(x_trace_id)
    started = time.perf_counter()
    _log_request_start(trace_id, 'preview_table')

    try:
        return preview_table(
            trace_id=trace_id,
            table_name=body.table_name,
            schema_name=body.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail='Internal server error') from exc
    finally:
        total_ms = int((time.perf_counter() - started) * 1000)
        _log_request_end(trace_id, 'preview_table', total_ms)


@app.get('/tools/preview_table')
@app.get('/tools/preview_table/')
def get_preview_table_hint() -> dict[str, str]:
    return _post_only_hint('preview_table')


def main() -> None:
    host = os.getenv('MCP_HOST', '0.0.0.0')
    port = int(os.getenv('MCP_PORT', '8000'))
    uvicorn.run(app, host=host, port=port)


if __name__ == '__main__':
    main()
