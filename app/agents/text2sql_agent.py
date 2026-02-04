import re
import json
import clickhouse_connect

from app.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    CH_MAX_ROWS, SCHEMA_DICT_PATH
)
from app.core.llm import LLMClient
from app.core.schema_registry import SchemaRegistry

llm = LLMClient()

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()

def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

def _is_safe_sql(sql: str) -> bool:
    if not sql:
        return False
    s = sql.strip().lower()

    # must start with select
    if not s.startswith("select"):
        return False

    # block dangerous keywords
    banned = ["insert", "update", "delete", "drop", "truncate", "alter", "create", "attach", "detach"]
    if any(b in s for b in banned):
        return False

    return True

def _enforce_limit(sql: str, max_rows: int) -> str:
    s = sql.strip().rstrip(";")
    if re.search(r"\blimit\b", s, flags=re.IGNORECASE):
        return s
    return f"{s}\nLIMIT {max_rows}"

def _tables_in_sql(sql: str) -> set[str]:
    found = set()
    for m in re.finditer(r"\b(from|join)\s+([a-zA-Z0-9_\.]+)", sql, flags=re.IGNORECASE):
        found.add(m.group(2))
    return found

async def text2sql_answer(query: str) -> str:

    candidates = _registry.search(query, top_k=8)
    if not candidates:
        return "Text2SQL: Dictionary дээрээс тохирох хүснэгт олдсонгүй. (Table/Column нэр эсвэл бизнес түлхүүр үг нэмээд асуугаарай.)"

    schema_ctx = []
    for t in candidates[:5]:
        cols = [{"name": c.name, "type": c.dtype, "desc": c.attr} for c in t.columns[:40]]
        schema_ctx.append({
            "db": t.db,
            "table": t.table,
            "entity": t.entity,
            "description": t.description[:200],
            "columns": cols
        })

    prompt = [
        {"role": "system", "content":
            "You generate SAFE ClickHouse SELECT queries. Reply ONLY JSON.\n"
            "Rules:\n"
            "- Only SELECT (no DDL/DML)\n"
            "- Prefer the provided tables/columns\n"
            "- Always include a LIMIT\n"
            "- If the question is unclear, generate a small exploratory SELECT with LIMIT 20 (e.g., show recent rows).\n"
            "Output JSON: {\"sql\":\"...\",\"notes\":\"...\"}"
        },
        {"role": "user", "content": json.dumps({
            "question": query,
            "database_hint": CLICKHOUSE_DATABASE,
            "candidates": schema_ctx
        }, ensure_ascii=False)}
    ]

    try:
        out = await llm.chat(prompt, temperature=0.0, max_tokens=500)
        m = re.search(r"\{.*\}", out, re.DOTALL)
        data = json.loads(m.group(0) if m else out)
        sql = (data.get("sql") or "").strip()
        notes = (data.get("notes") or "").strip()
    except Exception as e:
        return f"Text2SQL: SQL үүсгэх үед LLM алдаа: {e}"

    # 4) safety checks
    if not _is_safe_sql(sql):
        return "Text2SQL: Үүссэн SQL аюулгүй биш байна (SELECT-only дүрэм зөрчсөн). Асуултаа илүү тодорхой болгоод дахин асуугаарай."

    sql = _enforce_limit(sql, max(10, min(CH_MAX_ROWS, 1000)))

    allowed_tables = set()
    for t in candidates[:8]:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")

    used = _tables_in_sql(sql)
    if used and not all(u in allowed_tables for u in used):
        return (
            "Text2SQL: SQL дотор dictionary-д байхгүй хүснэгт ашигласан байна.\n"
            f"Used: {sorted(list(used))}\n"
            f"Allowed: {sorted(list(allowed_tables))[:10]} ..."
        )

    # 5) execute
    try:
        client = _ch_client()
        res = client.query(sql)
        rows = res.result_rows
        cols = res.column_names
    except Exception as e:
        return f"Text2SQL: ClickHouse query error: {e}\n\nSQL:\n{sql}"

    if not rows:
        return f"Text2SQL: Үр дүн хоосон.\n\nSQL:\n{sql}\n\nNotes:\n{notes}"

    # render first 50 rows
    show_n = min(len(rows), 50)
    header = " | ".join(cols)
    lines = [header]
    for r in rows[:show_n]:
        lines.append(" | ".join([str(x) for x in r]))

    return f"Text2SQL (ClickHouse) үр дүн:\n{notes}\n\nSQL:\n{sql}\n\nDATA (top {show_n}):\n" + "\n".join(lines)
