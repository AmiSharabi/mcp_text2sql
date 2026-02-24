import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request

from src.logger import log_event
from src.tools import (
    download_readonly_sql_result,
    execute_readonly_sql,
    explain_reasoning,
    get_schema,
    preview_table,
)

_DEFAULT_TOOLS_PATH = Path(__file__).resolve().parent.parent / 'mcp_tools.json'
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_mcp_tool_definitions() -> list[dict[str, Any]]:
    # Load and validate MCP tool definitions from JSON configuration.
    tools_path_raw = os.getenv('MCP_TOOLS_PATH', str(_DEFAULT_TOOLS_PATH))
    tools_path = Path(tools_path_raw).expanduser()
    if not tools_path.is_absolute():
        tools_path = _PROJECT_ROOT / tools_path

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
SSE_SESSIONS: dict[str, asyncio.Queue[dict[str, Any]]] = {}
STREAMABLE_HTTP_SESSIONS: dict[str, float] = {}


def trace_id_from_header(trace_id_header: str | None) -> str:
    # Return client trace id when present, otherwise generate a new UUID.
    if trace_id_header and trace_id_header.strip():
        return trace_id_header.strip()
    return str(uuid.uuid4())


def log_request_start(
    trace_id: str,
    tool_name: str,
    user_prompt: str | None = None,
    session_id: str | None = None,
    transport: str = 'http_tool',
) -> None:
    # Write a standardized "request_start" log event for tool calls.
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'request_start',
            'tool_name': tool_name,
            'user_prompt': user_prompt,
            'session_id': session_id,
            'transport': transport,
        }
    )


def log_request_end(
    trace_id: str,
    tool_name: str,
    total_ms: int,
    user_prompt: str | None = None,
    session_id: str | None = None,
    transport: str = 'http_tool',
) -> None:
    # Write a standardized "request_end" log event for tool calls.
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'request_end',
            'tool_name': tool_name,
            'total_ms': total_ms,
            'user_prompt': user_prompt,
            'session_id': session_id,
            'transport': transport,
        }
    )


def post_only_hint(tool_name: str) -> dict[str, str]:
    # Build a common hint response for endpoints that only support POST.
    return {
        'status': 'error',
        'message': f'Use POST for /tools/{tool_name}',
    }


def is_authorized(request: Request) -> bool:
    # Validate API key from x-api-key or Bearer token headers.
    expected_key = os.getenv('MCP_API_KEY', '').strip()
    if not expected_key:
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


def enforce_auth(request: Request) -> None:
    # Raise 401 when the incoming request is not authorized.
    if not is_authorized(request):
        raise HTTPException(status_code=401, detail='Unauthorized')


def mcp_error_response(request_id: Any, code: int, message: str) -> dict[str, Any]:
    # Build a JSON-RPC error payload with the provided id/code/message.
    return {
        'jsonrpc': '2.0',
        'id': request_id,
        'error': {
            'code': code,
            'message': message,
        },
    }


def _finalize_tool_result(tool_result: Any, total_ms: int) -> dict[str, Any]:
    # Normalize tool result to dict and ensure total duration is included.
    if isinstance(tool_result, dict):
        result = dict(tool_result)
    else:
        result = {'value': tool_result}
    result['total_ms'] = total_ms
    return result


def _format_tool_output_text(tool_name: str, tool_result: dict[str, Any]) -> str:
    # Build a compact human-readable summary for MCP TextContent output.
    parts = []
    for key in ('total_ms', 'agent_think_ms', 'sql_exec_ms', 'row_count'):
        value = tool_result.get(key)
        if isinstance(value, int):
            parts.append(f'{key}={value}')
    summary = f'{tool_name} completed'
    if parts:
        summary += ' | ' + ', '.join(parts)

    return summary + '\n' + json.dumps(tool_result, ensure_ascii=True)


def call_mcp_tool(name: str, arguments: dict[str, Any], trace_id: str) -> dict[str, Any]:
    # Dispatch supported MCP tool names to internal tool implementations.
    user_prompt = arguments.get('user_prompt')
    if user_prompt is not None and not isinstance(user_prompt, str):
        raise ValueError('"user_prompt" must be a string when provided')

    if name == 'get_schema':
        return get_schema(trace_id, user_prompt=user_prompt or 'get schema')

    if name == 'execute_readonly_sql':
        sql = arguments.get('sql')
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError('execute_readonly_sql requires non-empty "sql"')
        return execute_readonly_sql(trace_id=trace_id, sql=sql, user_prompt=user_prompt or sql)

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
            user_prompt=user_prompt or question,
        )

    if name == 'preview_table':
        table_name = arguments.get('table_name')
        schema_name = arguments.get('schema_name', 'dbo')
        if not isinstance(table_name, str) or not table_name.strip():
            raise ValueError('preview_table requires non-empty "table_name"')
        if not isinstance(schema_name, str) or not schema_name.strip():
            raise ValueError('preview_table requires non-empty "schema_name"')
        return preview_table(
            trace_id=trace_id,
            table_name=table_name,
            schema_name=schema_name,
            user_prompt=user_prompt or f'preview {schema_name}.{table_name}',
        )

    if name == 'download_result':
        sql = arguments.get('sql')
        file_name = arguments.get('file_name')
        download_mode = arguments.get('download_mode', 'link')
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError('download_result requires non-empty "sql"')
        if file_name is not None and not isinstance(file_name, str):
            raise ValueError('download_result "file_name" must be string when provided')
        if not isinstance(download_mode, str) or download_mode.strip().lower() not in {'link', 'base64'}:
            raise ValueError('download_result "download_mode" must be "link" or "base64"')
        return download_readonly_sql_result(
            trace_id=trace_id,
            sql=sql,
            file_name=file_name,
            download_mode=download_mode,
            user_prompt=user_prompt or sql,
        )

    raise ValueError(f'Unknown tool: {name}')


def run_mcp_payload(
    payload: dict[str, Any],
    request: Request,
    x_trace_id: str | None,
    session_id: str | None = None,
    transport: str = 'jsonrpc_http',
) -> dict[str, Any]:
    # Handle a JSON-RPC payload and return the correct MCP response object.
    request_id = payload.get('id')
    method = payload.get('method')
    header_user_prompt = request.headers.get('x-user-prompt')

    log_event(
        {
            'event_type': 'mcp_request_received',
            'method': method,
            'request_id': request_id,
            'session_id': session_id,
            'transport': transport,
        }
    )

    if method == 'initialize':
        params = payload.get('params')
        client_protocol_version = None
        if isinstance(params, dict):
            cpv = params.get('protocolVersion')
            if isinstance(cpv, str) and cpv.strip():
                client_protocol_version = cpv.strip()

        server_protocol_version = os.getenv('MCP_PROTOCOL_VERSION', '2024-11-05')
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {
                'protocolVersion': client_protocol_version or server_protocol_version,
                'capabilities': {
                    'tools': {
                        'listChanged': False,
                    }
                },
                'serverInfo': {
                    'name': 'mcp_text2sql',
                    'version': '0.1.0',
                },
                'instructions': 'Use tools/list to inspect tools and tools/call to execute a tool.',
            },
        }

    if method == 'notifications/initialized':
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {},
        }

    if method == 'ping':
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {},
        }

    if method == 'resources/list':
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {'resources': []},
        }

    if method == 'resources/templates/list':
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {'resourceTemplates': []},
        }

    if method == 'prompts/list':
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {'prompts': []},
        }

    if method == 'tools/list':
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {'tools': MCP_TOOL_DEFINITIONS},
        }

    if method == 'tools/call':
        params = payload.get('params')
        if not isinstance(params, dict):
            return mcp_error_response(request_id, -32602, 'Invalid params')

        tool_name = params.get('name')
        if not isinstance(tool_name, str) or not tool_name.strip():
            return mcp_error_response(request_id, -32602, 'Missing tool name')

        arguments = params.get('arguments', {})
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            return mcp_error_response(request_id, -32602, 'Invalid tool arguments')
        user_prompt = arguments.get('user_prompt')
        if (not isinstance(user_prompt, str) or not user_prompt.strip()) and isinstance(header_user_prompt, str):
            user_prompt = header_user_prompt
        if user_prompt is not None and not isinstance(user_prompt, str):
            return mcp_error_response(request_id, -32602, '"user_prompt" must be string')

        trace_id = trace_id_from_header(x_trace_id)
        started = time.perf_counter()
        log_request_start(
            trace_id,
            tool_name,
            user_prompt=user_prompt,
            session_id=session_id,
            transport=transport,
        )
        try:
            tool_result = call_mcp_tool(tool_name, arguments, trace_id)
        except ValueError as exc:
            return mcp_error_response(request_id, -32602, str(exc))
        except Exception:
            return mcp_error_response(request_id, -32603, 'Internal server error')
        finally:
            total_ms = int((time.perf_counter() - started) * 1000)
            log_request_end(
                trace_id,
                tool_name,
                total_ms,
                user_prompt=user_prompt,
                session_id=session_id,
                transport=transport,
            )

        client_tool_result = _finalize_tool_result(tool_result, total_ms)
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': {
                'content': [
                    {
                        'type': 'text',
                        'text': _format_tool_output_text(tool_name, client_tool_result),
                    }
                ],
                'structuredContent': client_tool_result,
            },
        }

    return mcp_error_response(request_id, -32601, f'Unknown method: {method}')


def sse_event(name: str, data: str) -> str:
    # Format a Server-Sent Events frame from event name and data.
    return f'event: {name}\ndata: {data}\n\n'
