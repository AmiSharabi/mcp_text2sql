import os
import time
import uuid
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from logger import log_event
from tools import execute_readonly_sql, explain_reasoning, get_schema


load_dotenv()

app = FastAPI(title='MCP Text2SQL Server', version='0.1.0')


class ExecuteReadonlySqlRequest(BaseModel):
    sql: str = Field(..., min_length=1)


class ExplainReasoningRequest(BaseModel):
    question: str = Field(..., min_length=1)
    chosen_tables: list[str]
    sql: str = Field(..., min_length=1)


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
        'message': 'Server is running. Use POST on /tools/get_schema, /tools/execute_readonly_sql, /tools/explain_reasoning',
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


def main() -> None:
    host = os.getenv('MCP_HOST', '0.0.0.0')
    port = int(os.getenv('MCP_PORT', '8000'))
    uvicorn.run(app, host=host, port=port)


if __name__ == '__main__':
    main()
