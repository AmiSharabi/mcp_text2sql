import argparse
import csv
import json
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile
from xml.sax.saxutils import escape


CANONICAL_ORDER = [
    "ts_iso",
    "_line",
    "trace_id",
    "session_id",
    "request_id",
    "event_type",
    "transport",
    "method",
    "tool_name",
    "user_prompt",
    "sql_preview",
    "download_mode",
    "row_count",
    "agent_think_ms",
    "sql_exec_ms",
    "total_ms",
    "error",
]


COLUMN_GUIDE: dict[str, tuple[str, str]] = {
    "ts_iso": ("UTC timestamp in ISO-8601.", "Added by logger automatically if missing in the event."),
    "_line": ("Original line number in the JSONL log file.", "Added by export script while reading JSONL."),
    "trace_id": (
        "Correlation id for a full request flow.",
        "Generated once per request and reused across request/tool events.",
    ),
    "session_id": ("Session id for SSE transport.", "Set when the request is tied to an active SSE session."),
    "request_id": ("JSON-RPC request id sent by MCP client.", "Comes from incoming MCP payload field id."),
    "event_type": (
        "Type of event logged.",
        "Set by server/tool logger (request_start, tool_ok, mcp_request_received, etc.).",
    ),
    "transport": (
        "How request reached server.",
        "Set by route handler (http_tool, jsonrpc_http, streamable_http, sse).",
    ),
    "method": ("MCP method name.", "Comes from incoming MCP payload field method."),
    "tool_name": ("Tool that was called.", "Set when tools/call or direct tool route executes."),
    "user_prompt": (
        "Prompt/context tied to request.",
        "Taken from tool args/header and backfilled by trace_id during export.",
    ),
    "sql_preview": ("Sanitized SQL snippet preview.", "Logged by SQL tools for execute/download operations."),
    "download_mode": ("Download behavior for CSV export tool.", "From download_result arguments (link/base64)."),
    "row_count": ("Number of rows returned by SQL execution.", "Logged by tool_ok events after query execution."),
    "agent_think_ms": ("Server-side preprocessing time in ms.", "Logged by tool implementation timing."),
    "sql_exec_ms": ("Database execution time in ms.", "Logged by tool implementation timing."),
    "total_ms": ("End-to-end request duration in ms.", "Logged at request_end by route handler."),
    "error": ("Error details if request/tool failed.", "Logged by tool_error or error response flow."),
}


def load_jsonl(path: Path) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    bad_lines = 0

    with path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                bad_lines += 1
                continue

            if not isinstance(obj, dict):
                bad_lines += 1
                continue

            obj["_line"] = idx
            records.append(obj)

    return records, bad_lines


def _fill_user_prompt(records: list[dict[str, Any]]) -> None:
    prompt_by_trace: dict[str, str] = {}

    for rec in records:
        trace_id = rec.get("trace_id")
        user_prompt = rec.get("user_prompt")
        if isinstance(trace_id, str) and trace_id and isinstance(user_prompt, str) and user_prompt.strip():
            prompt_by_trace[trace_id] = user_prompt

    for rec in records:
        current = rec.get("user_prompt")
        if isinstance(current, str) and current.strip():
            continue
        trace_id = rec.get("trace_id")
        if isinstance(trace_id, str) and trace_id in prompt_by_trace:
            rec["user_prompt"] = prompt_by_trace[trace_id]
        else:
            rec["user_prompt"] = ""


def _ordered_columns(records: list[dict[str, Any]]) -> list[str]:
    columns = set()
    for row in records:
        columns.update(row.keys())

    ordered = list(CANONICAL_ORDER)
    for col in sorted(columns):
        if col not in ordered:
            ordered.append(col)
    return ordered


def _next_available_path(path: Path) -> Path:
    candidate = path
    idx = 1
    while candidate.exists():
        candidate = path.with_name(f"{path.stem}_new{idx}{path.suffix}")
        idx += 1
    return candidate


def export_events_csv(records: list[dict[str, Any]], ordered: list[str], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    target = output_path
    while True:
        try:
            with target.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=ordered, extrasaction="ignore")
                writer.writeheader()
                for row in records:
                    writer.writerow(row)
            return target
        except PermissionError:
            target = _next_available_path(target)


def _collect_examples(records: list[dict[str, Any]], col: str, max_examples: int = 3) -> list[str]:
    examples: list[str] = []
    seen: set[str] = set()

    for row in records:
        value = row.get(col)
        if value is None:
            continue

        text = str(value).strip()
        if not text:
            continue
        if text in seen:
            continue

        seen.add(text)
        examples.append(text)
        if len(examples) >= max_examples:
            break

    return examples


def build_column_guide_rows(records: list[dict[str, Any]], ordered: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for idx, col in enumerate(ordered, start=1):
        description, recorded_when = COLUMN_GUIDE.get(
            col,
            ("Field captured in event payload.", "Captured directly from the logged JSON event."),
        )
        examples = _collect_examples(records, col)
        rows.append(
            {
                "column_name": col,
                "position": idx,
                "description": description,
                "recorded_when": recorded_when,
                "example_1": examples[0] if len(examples) > 0 else "",
                "example_2": examples[1] if len(examples) > 1 else "",
                "example_3": examples[2] if len(examples) > 2 else "",
            }
        )

    return rows


def export_column_guide_csv(rows: list[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["column_name", "position", "description", "recorded_when", "example_1", "example_2", "example_3"]
    target = output_path
    while True:
        try:
            with target.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
            return target
        except PermissionError:
            target = _next_available_path(target)


def _xlsx_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    escaped = escape(text)
    return f"<c t=\"inlineStr\"><is><t>{escaped}</t></is></c>"


def _xlsx_sheet_xml(headers: list[str], rows: list[dict[str, Any]]) -> str:
    body: list[str] = []
    row_idx = 1

    header_cells = "".join(_xlsx_cell(h) for h in headers)
    body.append(f"<row r=\"{row_idx}\">{header_cells}</row>")
    row_idx += 1

    for row in rows:
        cells = "".join(_xlsx_cell(row.get(h, "")) for h in headers)
        body.append(f"<row r=\"{row_idx}\">{cells}</row>")
        row_idx += 1

    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
        "<sheetData>"
        + "".join(body)
        + "</sheetData></worksheet>"
    )


def export_column_guide_xlsx(rows: list[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["column_name", "position", "description", "recorded_when", "example_1", "example_2", "example_3"]

    content_types = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
        "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
        "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
        "<Override PartName=\"/xl/workbook.xml\" "
        "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
        "<Override PartName=\"/xl/worksheets/sheet1.xml\" "
        "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
        "</Types>"
    )

    rels = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" "
        "Target=\"xl/workbook.xml\"/>"
        "</Relationships>"
    )

    workbook = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        "<sheets><sheet name=\"columns_guide\" sheetId=\"1\" r:id=\"rId1\"/></sheets>"
        "</workbook>"
    )

    workbook_rels = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" "
        "Target=\"worksheets/sheet1.xml\"/>"
        "</Relationships>"
    )

    sheet1 = _xlsx_sheet_xml(headers, rows)

    target = output_path
    while True:
        try:
            with ZipFile(target, "w", compression=ZIP_DEFLATED) as zf:
                zf.writestr("[Content_Types].xml", content_types)
                zf.writestr("_rels/.rels", rels)
                zf.writestr("xl/workbook.xml", workbook)
                zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
                zf.writestr("xl/worksheets/sheet1.xml", sheet1)
            return target
        except PermissionError:
            target = _next_available_path(target)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export JSONL logs to CSV.")
    parser.add_argument("--input", default="logs/events.jsonl", help="Input JSONL path")
    parser.add_argument("--output", default="logs/events.csv", help="Output CSV path")
    parser.add_argument(
        "--columns-guide-output",
        default="logs/events_columns_guide.csv",
        help="Output CSV path for column guide (Excel-friendly).",
    )
    parser.add_argument(
        "--columns-guide-xlsx-output",
        default="logs/events_columns_guide.xlsx",
        help="Output XLSX path for column guide.",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    guide_csv_path = Path(args.columns_guide_output)
    guide_xlsx_path = Path(args.columns_guide_xlsx_output)

    if not in_path.exists():
        print(f"Input log file not found: {in_path}")
        return 1

    records, bad_lines = load_jsonl(in_path)
    _fill_user_prompt(records)

    ordered = _ordered_columns(records)
    events_out = export_events_csv(records, ordered, out_path)

    guide_rows = build_column_guide_rows(records, ordered)
    guide_csv_out = export_column_guide_csv(guide_rows, guide_csv_path)
    guide_xlsx_out = export_column_guide_xlsx(guide_rows, guide_xlsx_path)

    print(f"Exported {len(records)} records to {events_out}")
    print(f"Exported column guide CSV to {guide_csv_out}")
    print(f"Exported column guide XLSX to {guide_xlsx_out}")
    if bad_lines:
        print(f"Skipped {bad_lines} invalid line(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
