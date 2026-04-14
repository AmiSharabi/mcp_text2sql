import json
import os
import tempfile
import unittest
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from fastapi.testclient import TestClient

import mcp_server


class TestSseSession(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.log_path = os.path.join(self.tmp_dir.name, 'events.jsonl')
        os.environ['LOG_PATH'] = self.log_path
        os.environ['MCP_API_KEY'] = ''
        self.client = TestClient(mcp_server.app)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_sse_session_tools_list_and_logs(self) -> None:
        session_id = 'unit-test-session'
        q: asyncio.Queue[dict] = asyncio.Queue()
        mcp_server.SSE_SESSIONS[session_id] = q

        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'tools/list',
            'params': {},
        }
        post_resp = self.client.post(f'/messages?session_id={session_id}', json=payload)
        self.assertEqual(post_resp.status_code, 200)
        self.assertEqual(post_resp.json().get('ok'), True)

        queued = q.get_nowait()
        self.assertEqual(queued.get('id'), 1)
        self.assertIn('result', queued)
        self.assertIn('tools', queued['result'])
        self.assertGreater(len(queued['result']['tools']), 0)

        mcp_server.SSE_SESSIONS.pop(session_id, None)

        with open(self.log_path, 'r', encoding='utf-8') as f:
            logs = [json.loads(line) for line in f if line.strip()]

        self.assertTrue(
            any(
                e.get('event_type') == 'mcp_request_received'
                and e.get('transport') == 'sse'
                and e.get('session_id') == session_id
                for e in logs
            )
        )

    def test_post_sse_tools_list(self) -> None:
        payload = {
            'jsonrpc': '2.0',
            'id': 2,
            'method': 'tools/list',
            'params': {},
        }
        resp = self.client.post('/sse', json=payload)
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get('id'), 2)
        self.assertIn('result', body)
        self.assertIn('tools', body['result'])

    def test_post_sse_initialize(self) -> None:
        payload = {
            'jsonrpc': '2.0',
            'id': 3,
            'method': 'initialize',
            'params': {
                'protocolVersion': '2024-11-05',
                'capabilities': {},
                'clientInfo': {'name': 'unit_test_client', 'version': '1.0.0'},
            },
        }
        resp = self.client.post('/sse', json=payload)
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get('id'), 3)
        self.assertIn('result', body)
        self.assertEqual(body['result'].get('protocolVersion'), '2024-11-05')
        self.assertIn('serverInfo', body['result'])
        self.assertIn('capabilities', body['result'])
        self.assertTrue(resp.headers.get('Mcp-Session-Id'))

    def test_post_sse_resources_list(self) -> None:
        payload = {
            'jsonrpc': '2.0',
            'id': 4,
            'method': 'resources/list',
            'params': {},
        }
        resp = self.client.post('/sse', json=payload)
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get('id'), 4)
        self.assertIn('result', body)
        self.assertEqual(body['result'].get('resources'), [])

    def test_post_sse_notifications_initialized_returns_202(self) -> None:
        payload = {
            'jsonrpc': '2.0',
            'method': 'notifications/initialized',
            'params': {},
        }
        resp = self.client.post('/sse', json=payload)
        self.assertEqual(resp.status_code, 202)

    def test_post_sse_tools_call_returns_text_content_and_structured_content(self) -> None:
        payload = {
            'jsonrpc': '2.0',
            'id': 5,
            'method': 'tools/call',
            'params': {
                'name': 'explain_reasoning',
                'arguments': {
                    'question': 'Why this SQL?',
                    'chosen_tables': ['dbo.Fact_Sales'],
                    'sql': 'SELECT TOP 1 * FROM dbo.Fact_Sales',
                },
            },
        }
        resp = self.client.post('/sse', json=payload)
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get('id'), 5)
        self.assertIn('result', body)
        self.assertIn('content', body['result'])
        self.assertGreater(len(body['result']['content']), 0)
        first = body['result']['content'][0]
        self.assertEqual(first.get('type'), 'text')
        self.assertIsInstance(first.get('text'), str)
        self.assertIn('total_ms=', first.get('text'))
        self.assertIn('structuredContent', body['result'])
        self.assertIsInstance(body['result']['structuredContent'], dict)
        self.assertIsInstance(body['result']['structuredContent'].get('total_ms'), int)

    def test_post_sse_tools_call_decimal_payload_is_serializable(self) -> None:
        payload = {
            'jsonrpc': '2.0',
            'id': 6,
            'method': 'tools/call',
            'params': {
                'name': 'execute_readonly_sql',
                'arguments': {
                    'sql': 'SELECT TOP 1 1 AS amount',
                },
            },
        }
        fake_tool_result = {
            'sql': 'SELECT TOP 1 1 AS amount',
            'rows': [{'amount': Decimal('12.34')}],
            'row_count': 1,
            'agent_think_ms': 1,
            'sql_exec_ms': 2,
        }
        with patch('src.mcp_runtime.call_mcp_tool', return_value=fake_tool_result):
            resp = self.client.post('/sse', json=payload)
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get('id'), 6)
        self.assertIn('result', body)
        self.assertIn('structuredContent', body['result'])
        rows = body['result']['structuredContent'].get('rows')
        self.assertIsInstance(rows, list)
        self.assertEqual(rows[0].get('amount'), 12.34)

    def test_post_sse_logs_full_tool_result_linked_to_user_prompt(self) -> None:
        payload = {
            'jsonrpc': '2.0',
            'id': 7,
            'method': 'tools/call',
            'params': {
                'name': 'execute_readonly_sql',
                'arguments': {
                    'sql': 'SELECT TOP 1 1 AS amount',
                    'user_prompt': 'show me first amount',
                    'password': 'should_not_be_logged',
                },
            },
        }
        fake_tool_result = {
            'sql': 'SELECT TOP 1 1 AS amount',
            'rows': [{'amount': Decimal('12.34'), 'ts': datetime(2026, 1, 1, tzinfo=timezone.utc)}],
            'row_count': 1,
            'agent_think_ms': 3,
            'sql_exec_ms': 4,
        }
        with patch('src.mcp_runtime.call_mcp_tool', return_value=fake_tool_result):
            resp = self.client.post('/sse', json=payload)

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get('id'), 7)
        self.assertIn('result', body)

        with open(self.log_path, 'r', encoding='utf-8') as f:
            logs = [json.loads(line) for line in f if line.strip()]

        detailed = [
            e
            for e in logs
            if e.get('event_type') == 'tool_result' and e.get('request_id') == 7 and e.get('tool_name') == 'execute_readonly_sql'
        ]
        self.assertTrue(detailed, 'Expected a tool_result event in logs')
        event = detailed[-1]
        self.assertEqual(event.get('user_prompt'), 'show me first amount')
        self.assertIn('trace_id', event)

        args = event.get('tool_arguments', {})
        self.assertEqual(args.get('sql'), 'SELECT TOP 1 1 AS amount')
        self.assertNotIn('password', args)

        result = event.get('tool_result', {})
        rows = result.get('rows')
        self.assertIsInstance(rows, list)
        self.assertEqual(rows[0].get('amount'), 12.34)
        self.assertEqual(rows[0].get('ts'), '2026-01-01T00:00:00Z')

        trace_id = event.get('trace_id')
        self.assertTrue(any(e.get('event_type') == 'request_start' and e.get('trace_id') == trace_id for e in logs))
        self.assertTrue(any(e.get('event_type') == 'request_end' and e.get('trace_id') == trace_id for e in logs))


if __name__ == '__main__':
    unittest.main()
