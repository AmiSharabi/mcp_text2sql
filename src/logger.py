import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _truncate_sql_preview(value: str, max_len: int = 200) -> str:
    text = ' '.join(value.split())
    if len(text) <= max_len:
        return text
    return text[:max_len] + '...'


def _truncate_text(value: str, max_len: int = 400) -> str:
    text = ' '.join(value.split())
    if len(text) <= max_len:
        return text
    return text[:max_len] + '...'


def _sanitize_event(event: dict[str, Any]) -> dict[str, Any]:
    clean = dict(event)

    if 'sql_preview' in clean and isinstance(clean['sql_preview'], str):
        clean['sql_preview'] = _truncate_sql_preview(clean['sql_preview'])
    if 'user_prompt' in clean and isinstance(clean['user_prompt'], str):
        clean['user_prompt'] = _truncate_text(clean['user_prompt'])

    for key in list(clean.keys()):
        key_l = key.lower()
        if 'password' in key_l or 'secret' in key_l:
            clean.pop(key, None)

    return clean


def log_event(event: dict[str, Any]) -> None:
    log_path = os.getenv('LOG_PATH', 'logs/events.jsonl')
    safe_event = _sanitize_event(event)

    if 'ts_iso' not in safe_event:
        safe_event['ts_iso'] = _now_iso()

    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(safe_event, ensure_ascii=True) + '\n')
