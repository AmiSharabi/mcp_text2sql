import json
import os
import tempfile
import unittest
import asyncio

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


if __name__ == '__main__':
    unittest.main()
