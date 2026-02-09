import re
import json
import clickhouse_connect

from app.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    CH_MAX_ROWS, SCHEMA_DICT_PATH
)
from app.core.llm import LLMClient
from app.core.schema_registry import SchemaRegistry

from datetime import date, timedelta
from app.config import CH_DEFAULT_TABLE, CH_DEFAULT_STORE_COL, CH_DEFAULT_DATE_COL, CH_DEFAULT_METRIC_COL

from datetime import date, timedelta
from app.config import CH_FALLBACK_TABLES
from typing import Any, Dict, List, Optional




llm = LLMClient()

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()

def _try_query(client, sql: str, params: dict | None = None):
    return client.query(sql, parameters=params or {})

def _extract_store_any(q: str):
    m = re.search(r"(CU\d{3,4})", q.upper())
    return m.group(1) if m else None

def _extract_days_any(q: str) -> int:
    ql = q.lower()
    if "өнөөдөр" in ql: return 1
    if "өчигдөр" in ql: return 2
    m = re.search(r"(\d+)\s*(хоног|өдөр)", ql)
    if m: return max(1, min(int(m.group(1)), 90))
    if "7 хоног" in ql or "7хоног" in ql: return 7
    if "30 хоног" in ql or "30хоног" in ql: return 30
    return 7

def _pick_metric_any(q: str) -> str:
    ql = q.lower()
    if any(k in ql for k in ["gross", "grosssale", "нийт"]): return "GrossSale"
    if any(k in ql for k in ["татвар", "vat", "tax"]): return "Tax_VAT"
    if any(k in ql for k in ["хөнгөлөлт", "discount"]): return "Discount"
    if any(k in ql for k in ["өртөг", "cost", "actualcost"]): return "ActualCost"
    return "NetSale"


def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
def _extract_store(q: str):
    m = re.search(r"(CU\d{3,4})", q.upper())
    return m.group(1) if m else None

def _extract_days(q: str) -> int:
    ql = q.lower()
    m = re.search(r"(\d+)\s*(хоног|өдөр)", ql)
    if m:
        return max(1, min(int(m.group(1)), 90))
    if "7 хоног" in ql: return 7
    if "30 хоног" in ql: return 30
    if "өнөөдөр" in ql: return 1
    return 7

def _rule_sql(query: str) -> tuple[str, str]:
    store = _extract_store(query)
    days = _extract_days(query)
    end_d = date.today()
    start_d = end_d - timedelta(days=days-1)

    # metric select
    ql = query.lower()
    metric = CH_DEFAULT_METRIC_COL
    if "gross" in ql or "grosssale" in ql or "нийт" in ql:
        metric = "GrossSale"
    if "татвар" in ql or "vat" in ql or "tax" in ql:
        metric = "Tax_VAT"
    if "хөнгөлөлт" in ql or "discount" in ql:
        metric = "Discount"
    if "өртөг" in ql or "cost" in ql:
        metric = "ActualCost"

    table = CH_DEFAULT_TABLE
    store_col = CH_DEFAULT_STORE_COL
    date_col = CH_DEFAULT_DATE_COL

    if store:
        sql = f"""
        SELECT
          {date_col} AS day,
          sum({metric}) AS value
        FROM {table}
        WHERE {store_col} = %(store)s
          AND {date_col} >= %(start)s
          AND {date_col} <= %(end)s
        GROUP BY day
        ORDER BY day DESC
        LIMIT {min(200, CH_MAX_ROWS)}
        """
        notes = f"RULE fallback: {store} / {metric} / {start_d}→{end_d} (daily sum)"
        return sql, notes

    sql = f"""
    SELECT *
    FROM {table}
    ORDER BY {date_col} DESC
    LIMIT 20
    """
    notes = "RULE fallback: store код олдсонгүй, default table дээрээс recent 20 мөр харууллаа."
    return sql, notes

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

async def text2sql_execute(query: str) -> Dict[str, Any]:
    """
    SQL үүсгээд ClickHouse дээр ажиллуулаад structured үр дүн буцаана.
    Return:
      {
        "sql": "...",
        "notes": "...",
        "columns": [...],
        "rows": [...],
        "row_count": int,
        "mode": "sql_result"
      }
    """
    # existing logic-г ашиглаж: дээрх text2sql_answer() шиг LLM + fallback-ууд
    candidates = _registry.search(query, top_k=8)
    if not candidates:
        return {
            "sql": "",
            "notes": "Dictionary дээрээс тохирох хүснэгт олдсонгүй.",
            "columns": [],
            "rows": [],
            "row_count": 0,
            "mode": "sql_result",
        }

    schema_ctx = []
    for t in candidates[:5]:
        cols = [{"name": c.name, "type": c.dtype, "desc": c.attr} for c in t.columns[:60]]
        schema_ctx.append({
            "db": t.db,
            "table": t.table,
            "entity": t.entity,
            "description": (t.description or "")[:200],
            "columns": cols
        })

    allowed_tables = set()
    for t in candidates[:8]:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")

    client = _ch_client()

    # --- 1) LLM SQL try
    try:
        prompt = [
            {"role": "system", "content":
                "You generate SAFE ClickHouse SELECT queries. Reply ONLY JSON.\n"
                "Rules:\n"
                "- Only SELECT (no DDL/DML)\n"
                "- Prefer the provided tables/columns\n"
                "- Always include a LIMIT\n"
                "- If unclear, return exploratory SELECT * LIMIT 20 on the best candidate table.\n"
                "Output JSON: {\"sql\":\"...\",\"notes\":\"...\"}"
            },
            {"role": "user", "content": json.dumps({
                "question": query,
                "database_hint": CLICKHOUSE_DATABASE,
                "candidates": schema_ctx
            }, ensure_ascii=False)}
        ]

        out = await llm.chat(prompt, temperature=0.0, max_tokens=550)
        m = re.search(r"\{.*\}", out, re.DOTALL)
        data = json.loads(m.group(0) if m else out)

        sql = (data.get("sql") or "").strip()
        notes = (data.get("notes") or "").strip()

        if not _is_safe_sql(sql):
            raise ValueError("unsafe_sql")

        sql = _enforce_limit(sql, max(10, min(CH_MAX_ROWS, 1000)))

        used = _tables_in_sql(sql)
        if used and not all(u in allowed_tables for u in used):
            raise ValueError(f"table_not_allowed: {sorted(list(used))}")

        res = client.query(sql)
        return {
            "sql": sql,
            "notes": notes,
            "columns": res.column_names,
            "rows": res.result_rows[:min(len(res.result_rows), CH_MAX_ROWS)],
            "row_count": len(res.result_rows),
            "mode": "sql_result",
        }

    except Exception as llm_err:
        # --- 2) fallback: RULE aggregation (store + metric)
        store = _extract_store_any(query)
        days = _extract_days_any(query)
        metric = _pick_metric_any(query)

        end_d = date.today()
        start_d = end_d - timedelta(days=days - 1)

        candidate_tables = []
        for t in candidates[:5]:
            candidate_tables.append(t.table)
            candidate_tables.append(f"{t.db}.{t.table}")
        candidate_tables += CH_FALLBACK_TABLES

        if store:
            for tbl in candidate_tables:
                sql_a = f"""
                SELECT
                  SalesDate AS day,
                  sum({metric}) AS value
                FROM {tbl}
                WHERE StoreID = %(store)s
                  AND SalesDate >= %(start)s
                  AND SalesDate <= %(end)s
                GROUP BY day
                ORDER BY day DESC
                LIMIT {min(200, CH_MAX_ROWS)}
                """
                try:
                    res = client.query(sql_a, parameters={
                        "store": store,
                        "start": str(start_d),
                        "end": str(end_d),
                    })
                    if res.result_rows:
                        return {
                            "sql": sql_a.strip(),
                            "notes": f"Fallback RULE aggregation (LLM failed: {type(llm_err).__name__})",
                            "columns": res.column_names,
                            "rows": res.result_rows[:min(len(res.result_rows), CH_MAX_ROWS)],
                            "row_count": len(res.result_rows),
                            "mode": "sql_result",
                        }
                except Exception:
                    continue

        # --- 3) fallback: sample rows
        for tbl in candidate_tables:
            sql_b = f"SELECT * FROM {tbl} LIMIT 20"
            try:
                res = client.query(sql_b)
                if res.result_rows:
                    return {
                        "sql": sql_b,
                        "notes": f"Fallback SAMPLE rows (LLM failed: {type(llm_err).__name__})",
                        "columns": res.column_names,
                        "rows": res.result_rows,
                        "row_count": len(res.result_rows),
                        "mode": "sql_result",
                    }
            except Exception:
                continue

        return {
            "sql": "",
            "notes": f"Text2SQL failed. LLM error: {str(llm_err)}",
            "columns": [],
            "rows": [],
            "row_count": 0,
            "mode": "sql_result",
        }

async def text2sql_answer(query: str) -> str:

    # ----------------------------
    # 1) Find candidate tables from dictionary
    # ----------------------------
    candidates = _registry.search(query, top_k=8)
    if not candidates:
        return (
            "Text2SQL: Dictionary дээрээс тохирох хүснэгт олдсонгүй.\n"
            "Table/Column нэр, эсвэл бизнес түлхүүр үг (StoreID, SalesDate, NetSale гэх мэт) нэмээд асуугаарай."
        )

    # Build compact schema context for LLM
    schema_ctx = []
    for t in candidates[:5]:
        cols = [{"name": c.name, "type": c.dtype, "desc": c.attr} for c in t.columns[:60]]
        schema_ctx.append({
            "db": t.db,
            "table": t.table,
            "entity": t.entity,
            "description": (t.description or "")[:200],
            "columns": cols
        })

    # Allowed tables list (strict allowlist)
    allowed_tables = set()
    for t in candidates[:8]:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")

    # ----------------------------
    # 2) Try LLM SQL (best effort)
    # ----------------------------
    sql = ""
    notes = ""
    try:
        prompt = [
            {"role": "system", "content":
                "You generate SAFE ClickHouse SELECT queries. Reply ONLY JSON.\n"
                "Rules:\n"
                "- Only SELECT (no DDL/DML)\n"
                "- Prefer the provided tables/columns\n"
                "- Always include a LIMIT\n"
                "- If unclear, return exploratory SELECT * LIMIT 20 on the best candidate table.\n"
                "Output JSON: {\"sql\":\"...\",\"notes\":\"...\"}"
            },
            {"role": "user", "content": json.dumps({
                "question": query,
                "database_hint": CLICKHOUSE_DATABASE,
                "candidates": schema_ctx
            }, ensure_ascii=False)}
        ]

        out = await llm.chat(prompt, temperature=0.0, max_tokens=550)
        m = re.search(r"\{.*\}", out, re.DOTALL)
        data = json.loads(m.group(0) if m else out)

        sql = (data.get("sql") or "").strip()
        notes = (data.get("notes") or "").strip()

        # Safety checks
        if not _is_safe_sql(sql):
            raise ValueError("unsafe_sql")

        sql = _enforce_limit(sql, max(10, min(CH_MAX_ROWS, 1000)))

        used = _tables_in_sql(sql)
        if used and not all(u in allowed_tables for u in used):
            raise ValueError(f"table_not_allowed: {sorted(list(used))}")

        # Execute LLM SQL
        client = _ch_client()
        res = client.query(sql)
        rows = res.result_rows
        cols = res.column_names

        if not rows:
            return f"Text2SQL (ClickHouse) \n{notes}\n\nSQL:\n{sql}\n\nDATA: (хоосон үр дүн)"

        show_n = min(len(rows), 50)
        lines = [" | ".join(cols)]
        for r in rows[:show_n]:
            lines.append(" | ".join([str(x) for x in r]))

        return f"Text2SQL (ClickHouse) \n{notes}\n\nSQL:\n{sql}\n\nDATA (top {show_n}):\n" + "\n".join(lines)

    except Exception as llm_err:
        # ----------------------------
        # 3) LLM failed -> RULE fallback aggregation
        # ----------------------------
        try:
            client = _ch_client()

            store = _extract_store_any(query)
            days = _extract_days_any(query)
            metric = _pick_metric_any(query)

            end_d = date.today()
            start_d = end_d - timedelta(days=days - 1)

            candidate_tables = []
            for t in candidates[:5]:
                candidate_tables.append(t.table)
                candidate_tables.append(f"{t.db}.{t.table}")
            candidate_tables += CH_FALLBACK_TABLES  # from env

            if store:
                for tbl in candidate_tables:
                    sql_a = f"""
                    SELECT
                      SalesDate AS day,
                      sum({metric}) AS value
                    FROM {tbl}
                    WHERE StoreID = %(store)s
                      AND SalesDate >= %(start)s
                      AND SalesDate <= %(end)s
                    GROUP BY day
                    ORDER BY day DESC
                    LIMIT {min(200, CH_MAX_ROWS)}
                    """
                    try:
                        res = client.query(sql_a, parameters={
                            "store": store,
                            "start": str(start_d),
                            "end": str(end_d),
                        })
                        if res.result_rows:
                            cols = res.column_names
                            rows = res.result_rows[:50]
                            lines = [" | ".join(cols)] + [" | ".join(map(str, r)) for r in rows]
                            return (
                                "Text2SQL fallback (RULE aggregation) \n"
                                f"Reason: LLM/SQL failed ({type(llm_err).__name__})\n"
                                f"SQL:\n{sql_a}\n\nDATA:\n" + "\n".join(lines)
                            )
                    except Exception:
                        continue

            # ----------------------------
            # 4) If aggregation fails -> SAMPLE ROWS fallback
            # ----------------------------
            for tbl in candidate_tables:
                sql_b = f"SELECT * FROM {tbl} LIMIT 20"
                try:
                    res = client.query(sql_b)
                    if res.result_rows:
                        cols = res.column_names
                        rows = res.result_rows[:20]
                        lines = [" | ".join(cols)] + [" | ".join(map(str, r)) for r in rows]
                        return (
                            "Text2SQL fallback (SAMPLE rows) \n"
                            f"Reason: LLM/aggregation failed ({type(llm_err).__name__})\n"
                            f"SQL:\n{sql_b}\n\nDATA:\n" + "\n".join(lines)
                        )
                except Exception:
                    continue

            # ----------------------------
            # 5) Last resort -> dictionary schema result
            # ----------------------------
            out = (
                "ClickHouse дээр query ажиллуулах боломжгүй/үр дүн олдсонгүй.\n"
                "Гэхдээ dictionary дээрээс хамгийн тохирох schema-ууд:\n"
            )
            for t in candidates[:3]:
                out += f"\n- {t.db}.{t.table}: {(t.description or '')}\n"
                out += "  cols: " + ", ".join([c.name for c in t.columns[:20]]) + "\n"
            return out

        except Exception as ch_err:
            out = (
                "Text2SQL: ClickHouse connection/query асуудалтай байна.\n"
                f"LLM error: {str(llm_err)}\n"
                f"CH error: {str(ch_err)}\n\n"
                "Dictionary top matches:\n"
            )
            for t in candidates[:3]:
                out += f"- {t.db}.{t.table}\n"
            return out


