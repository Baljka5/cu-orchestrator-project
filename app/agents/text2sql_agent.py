# /app/app/agents/text2sql_agent.py
import re
import json
from typing import Any, Dict, List, Set

import clickhouse_connect

from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
    SCHEMA_DICT_PATH,
)

from app.core.llm import LLMClient
from app.core.schema_registry import SchemaRegistry, TableInfo

# -------------------------------------------------
# Init
# -------------------------------------------------
llm = LLMClient()

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()
_all_relationships = _registry.build_relationships()


# -------------------------------------------------
# ClickHouse client
# -------------------------------------------------
def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _normalize_table_ref(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return t
    if "." not in t:
        return f"{CLICKHOUSE_DATABASE}.{t}"
    return t


def _safe_table(t: str, allowed: Set[str]) -> bool:
    t = _normalize_table_ref(t)
    base = t.split(".", 1)[-1]
    return (t in allowed) or (base in allowed)


def _extract_json(out: str) -> Dict[str, Any]:
    m = re.search(r"\{.*\}", out, re.DOTALL)
    raw = m.group(0) if m else out
    return json.loads(raw)


# -------------------------------------------------
# HARD RULE: Top sold product name by year
# -------------------------------------------------
def _hard_rule_top_product_name(query: str) -> Dict[str, Any] | None:
    """
    2025 онд хамгийн их зарагдсан бүтээгдэхүүний нэр
    гэх мэт асуултад LLM-гүйгээр шууд JOIN SQL хийж гүйцэтгэнэ.
    """
    ql = (query or "").lower()

    wants_name = any(k in ql for k in [
        "бүтээгдэхүүний нэр",
        "барааны нэр",
        "product name",
        "item name",
        "нэр"
    ])
    most_sold = any(k in ql for k in [
        "хамгийн их",
        "их зарагдсан",
        "most sold"
    ])

    m_year = re.search(r"\b(20\d{2})\b", query or "")
    year = int(m_year.group(1)) if m_year else None

    if not (wants_name and most_sold and year):
        return None

    sql = f"""
SELECT
  d1.GDS_NM AS item_name,
  sum(f.SoldQty) AS total_qty
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
LEFT JOIN {CLICKHOUSE_DATABASE}.Dimension_IM d1
  ON f.GDS_CD = d1.GDS_CD
WHERE toYear(f.SalesDate) = {year}
GROUP BY d1.GDS_NM
ORDER BY total_qty DESC
LIMIT 1
""".strip()

    client = _ch_client()
    res = client.query(sql)

    item_name = ""
    if res.result_rows:
        item_name = str(res.result_rows[0][0])

    return {
        "final_answer": item_name or "Илэрц олдсонгүй.",
        "meta": {
            "agent": "text2sql",
            "sql": sql,
            "data": {
                "columns": res.column_names,
                "rows": res.result_rows[:10]
            }
        }
    }


# -------------------------------------------------
# Main entry
# -------------------------------------------------
async def text2sql_answer(query: str) -> Dict[str, Any]:
    # 1️⃣ HARD RULE first
    hard = _hard_rule_top_product_name(query)
    if hard:
        return hard

    # 2️⃣ Registry search
    candidates = _registry.search(query, top_k=12)
    if not candidates:
        return {"final_answer": "Schema олдсонгүй.", "meta": {"agent": "text2sql"}}

    table_cards = [_registry.to_table_card(t, max_cols=80) for t in candidates[:8]]

    allowed_tables: Set[str] = set()
    for t in candidates:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")
    allowed_tables.add("Dimension_IM")
    allowed_tables.add(f"{CLICKHOUSE_DATABASE}.Dimension_IM")

    system = """
You are a ClickHouse Text-to-SQL planner.
Return ONLY valid JSON.

Rules:
- Use only tables in allowed_tables.
- Fact alias must be 'f'
- Joined tables must be 'd1','d2',...
- Use proper GROUP BY when using SUM.
Return JSON:
{
  "fact_table": "DB.TABLE",
  "select": [{"expr":"...", "as":"..."}],
  "joins": [{"type":"LEFT","table":"DB.TABLE","alias":"d1","on":"f.col = d1.col"}],
  "where": ["..."],
  "group_by": ["..."],
  "order_by": ["..."],
  "limit": 50
}
"""

    user = {
        "question": query,
        "table_cards": table_cards,
        "allowed_tables": sorted(list(allowed_tables))
    }

    out = await llm.chat(
        [
            {"role": "system", "content": system.strip()},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        temperature=0.0,
        max_tokens=900,
    )

    try:
        plan = _extract_json(out)
    except Exception:
        return {
            "final_answer": "LLM JSON parse error",
            "meta": {"raw": out}
        }

    fact = _normalize_table_ref(
        plan.get("fact_table") or f"{candidates[0].db}.{candidates[0].table}"
    )

    if not _safe_table(fact, allowed_tables):
        return {"final_answer": "Unsafe table", "meta": {"plan": plan}}

    select_items = plan.get("select") or [{"expr": "count()", "as": "cnt"}]
    select_clause = ", ".join(
        f"{x['expr']} AS {x['as']}" if x.get("as") else x["expr"]
        for x in select_items
    )

    sql = f"SELECT {select_clause}\nFROM {fact} f"

    # joins
    for j in plan.get("joins", []) or []:
        jtype = (j.get("type") or "LEFT").upper()
        tbl = _normalize_table_ref(j.get("table") or "")
        alias = j.get("alias") or "d1"
        on = j.get("on") or ""
        if tbl and on and _safe_table(tbl, allowed_tables):
            sql += f"\n{jtype} JOIN {tbl} {alias} ON {on}"

    # where
    wh = [x for x in (plan.get("where") or []) if isinstance(x, str) and x.strip()]
    if wh:
        sql += "\nWHERE " + " AND ".join(wh)

    # group by
    gb = [x for x in (plan.get("group_by") or []) if isinstance(x, str) and x.strip()]
    if gb:
        sql += "\nGROUP BY " + ", ".join(gb)

    # order by
    ob = [x for x in (plan.get("order_by") or []) if isinstance(x, str) and x.strip()]
    if ob:
        sql += "\nORDER BY " + ", ".join(ob)

    # limit
    try:
        lim = int(plan.get("limit") or 50)
    except Exception:
        lim = 50
    lim = max(1, min(lim, 500))
    sql += f"\nLIMIT {lim}"

    # execute
    client = _ch_client()
    try:
        res = client.query(sql)
    except Exception as e:
        return {
            "final_answer": "ClickHouse query error",
            "meta": {"sql": sql, "error": str(e)}
        }

    return {
        "final_answer": "Text2SQL",
        "meta": {
            "agent": "text2sql",
            "sql": sql,
            "data": {
                "columns": res.column_names,
                "rows": res.result_rows[:lim]
            }
        }
    }
