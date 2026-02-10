# app/agents/text2sql_agent.py
# -*- coding: utf-8 -*-

import re
import json
from datetime import date, timedelta
from typing import Optional, Dict, Any, List, Tuple

import clickhouse_connect

from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
    CH_MAX_ROWS,
    SCHEMA_DICT_PATH,
    CH_FALLBACK_TABLES,
)

from app.core.llm_client import chat_completion
from app.core.schema_registry import SchemaRegistry

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
    if not s.startswith("select"):
        return False
    banned = ["insert", "update", "delete", "drop", "truncate", "alter", "create", "attach", "detach"]
    return not any(b in s for b in banned)


def _enforce_limit(sql: str, max_rows: int) -> str:
    s = (sql or "").strip().rstrip(";")
    if not s:
        return s
    if re.search(r"\blimit\b", s, flags=re.IGNORECASE):
        return s
    return f"{s}\nLIMIT {max_rows}"


def _tables_in_sql(sql: str) -> set[str]:
    found = set()
    for m in re.finditer(r"\b(from|join)\s+([a-zA-Z0-9_\.]+)", sql, flags=re.IGNORECASE):
        found.add(m.group(2))
    return found


def _extract_store_any(q: str) -> Optional[str]:
    m = re.search(r"(CU\d{3,4})", (q or "").upper())
    return m.group(1) if m else None


def _extract_days_any(q: str) -> int:
    ql = (q or "").lower()
    if "өнөөдөр" in ql:
        return 1
    if "өчигдөр" in ql:
        return 2
    if "7 хоног" in ql or "7хоног" in ql:
        return 7
    if "30 хоног" in ql or "30хоног" in ql:
        return 30
    m = re.search(r"(\d+)\s*(хоног|өдөр)", ql)
    if m:
        return max(1, min(int(m.group(1)), 90))
    return 7


def _extract_year(q: str) -> Optional[int]:
    m = re.search(r"\b(20\d{2})\b", q or "")
    if not m:
        return None
    y = int(m.group(1))
    if 2000 <= y <= 2100:
        return y
    return None


def _pick_metric_any(q: str) -> str:
    ql = (q or "").lower()
    if any(k in ql for k in ["qty", "quantity", "тоо", "ширхэг", "soldqty"]):
        return "SoldQty"
    if any(k in ql for k in ["gross", "grosssale", "нийт"]):
        return "GrossSale"
    if any(k in ql for k in ["татвар", "vat", "tax"]):
        return "Tax_VAT"
    if any(k in ql for k in ["хөнгөлөлт", "discount"]):
        return "Discount"
    if any(k in ql for k in ["өртөг", "cost", "actualcost"]):
        return "ActualCost"
    # default
    return "NetSale"


def _has_col(t, name: str) -> bool:
    name_l = (name or "").lower()
    return any(((c.name or "").lower() == name_l) for c in (t.columns or []))


def _pick_first_existing(t, options: List[str]) -> Optional[str]:
    for o in options:
        if _has_col(t, o):
            return o
    return None


def _format_table(cols: List[str], rows: List[List[Any]], max_rows: int = 50) -> str:
    show = rows[:max_rows]
    lines = [" | ".join(cols)]
    for r in show:
        lines.append(" | ".join([str(x) for x in r]))
    return "\n".join(lines)


def _safe_notes(txt: str) -> str:
    return (txt or "").strip()


async def text2sql_answer(query: str) -> str:
    q = (query or "").strip()
    if not q:
        return "Text2SQL: хоосон асуулт байна."

    ql = q.lower()
    year = _extract_year(q)
    store = _extract_store_any(q)

    if ("хамгийн" in ql and ("их зарагдсан" in ql or "их борлуулсан" in ql)) and (year is not None):
        tbl = "Cluster_Main_Sales"
        sql_top = f"""
        SELECT
          GDS_CD AS item,
          sum(SoldQty) AS total_qty
        FROM {tbl}
        WHERE SalesDate >= toDate('{year}-01-01')
          AND SalesDate <  toDate('{year + 1}-01-01')
        GROUP BY item
        ORDER BY total_qty DESC
        LIMIT 10
        """.strip()

        try:
            client = _ch_client()
            res = client.query(sql_top)
            cols = list(res.column_names or [])
            rows = [list(r) for r in (res.result_rows or [])]
            return (
                "Text2SQL (RULE top sold)\n"
                f"Total sold qty top-10 for {year}\n\n"
                f"SQL:\n{sql_top}\n\n"
                f"DATA (top {min(50, len(rows))}):\n{_format_table(cols, rows, 50) if rows else '(хоосон үр дүн)'}"
            )
        except Exception as e:
            pass

    if ("нийт борлуулалт" in ql or "total sales" in ql) and (year is not None):
        metric = "GrossSale" if ("gross" in ql or "нийт" in ql) else "NetSale"
        tbl = "Cluster_Main_Sales"
        sql_sum = f"""
        SELECT
          sum({metric}) AS total_sales
        FROM {tbl}
        WHERE SalesDate >= toDate('{year}-01-01')
          AND SalesDate <  toDate('{year + 1}-01-01')
        LIMIT 1
        """.strip()

        try:
            client = _ch_client()
            res = client.query(sql_sum)
            cols = list(res.column_names or [])
            rows = [list(r) for r in (res.result_rows or [])]
            return (
                "Text2SQL (RULE year total)\n"
                f"Total sales for {year}\n\n"
                f"SQL:\n{sql_sum}\n\n"
                f"DATA (top {min(50, len(rows))}):\n{_format_table(cols, rows, 50) if rows else '(хоосон үр дүн)'}"
            )
        except Exception:
            pass

    candidates = _registry.search(q, top_k=8)
    if not candidates:
        return (
            "Text2SQL: Dictionary дээрээс тохирох хүснэгт олдсонгүй.\n"
            "Table/Column нэр, эсвэл түлхүүр үг (StoreID, SalesDate, NetSale гэх мэт) нэмээд асуугаарай."
        )

    MAX_TABLES = 3
    MAX_COLS = 25

    schema_ctx: List[Dict[str, Any]] = []
    for t in candidates[:MAX_TABLES]:
        cols = []
        for c in t.columns[:MAX_COLS]:
            cols.append(
                {
                    "name": c.name,
                    "type": (c.dtype or "")[:24],
                    "desc": (c.attr or "")[:60],
                }
            )

        schema_ctx.append(
            {
                "db": t.db,
                "table": t.table,
                "entity": (t.entity or "")[:60],
                "description": (t.description or "")[:120],
                "columns": cols,
            }
        )

    allowed_tables = set()
    for t in candidates[:8]:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")

    sql = ""
    notes = ""
    try:
        user_payload = {
            "q": q,
            "db": CLICKHOUSE_DATABASE,
            "tables": schema_ctx,
        }

        system_msg = (
            "You generate SAFE ClickHouse SELECT queries. Reply ONLY JSON.\n"
            "Rules:\n"
            "- Only SELECT (no DDL/DML)\n"
            "- Use only provided tables/columns\n"
            "- Keep SQL short\n"
            "- Always include LIMIT (10~200)\n"
            "- If unclear, return: SELECT * FROM <best_table> LIMIT 20\n"
            "Output JSON: {\"sql\":\"...\",\"notes\":\"...\"}"
        )

        out = await chat_completion(
            user_message=json.dumps(user_payload, ensure_ascii=False),
            system=system_msg,
            temperature=0.0,
            max_tokens=256,
        )

        m = re.search(r"\{.*\}", out or "", re.DOTALL)
        data = json.loads(m.group(0) if m else out)

        sql = (data.get("sql") or "").strip()
        notes = _safe_notes(data.get("notes") or "")

        if not _is_safe_sql(sql):
            raise ValueError("unsafe_sql")

        sql = _enforce_limit(sql, max(10, min(int(CH_MAX_ROWS), 200)))

        used = _tables_in_sql(sql)
        if used and not all(u in allowed_tables for u in used):
            raise ValueError(f"table_not_allowed: {sorted(list(used))}")

        client = _ch_client()
        res = client.query(sql)
        cols = list(res.column_names or [])
        rows = [list(r) for r in (res.result_rows or [])]

        return (
            "Text2SQL (ClickHouse)\n"
            f"{notes}\n\n"
            f"SQL:\n{sql}\n\n"
            f"DATA (top {min(50, len(rows))}):\n"
            f"{_format_table(cols, rows, 50) if rows else '(хоосон үр дүн)'}"
        )

    except Exception as llm_err:
        try:
            client = _ch_client()

            days = _extract_days_any(q)
            metric_pref = _pick_metric_any(q)
            end_d = date.today()
            start_d = end_d - timedelta(days=days - 1)

            candidate_tables: List[str] = []
            for t in candidates[:5]:
                candidate_tables.append(f"{t.db}.{t.table}")
            candidate_tables += (CH_FALLBACK_TABLES or [])

            if store:
                for t in candidates[:5]:
                    tbl = f"{t.db}.{t.table}"

                    date_col = _pick_first_existing(t, ["SalesDate", "sale_date", "tr_date", "stock_date", "CRT_YMD"])
                    store_col = _pick_first_existing(t, ["StoreID", "Store", "BIZLOC_CD", "STORE_ID"])
                    metric_col = _pick_first_existing(
                        t, [metric_pref, "NetSale", "NetSales", "GrossSale", "GrossSales", "Qty", "SoldQty"]
                    )

                    if not (date_col and store_col and metric_col):
                        continue

                    sql_a = f"""
                    SELECT
                      {date_col} AS day,
                      sum({metric_col}) AS value
                    FROM {tbl}
                    WHERE {store_col} = %(store)s
                      AND {date_col} >= %(start)s
                      AND {date_col} <= %(end)s
                    GROUP BY day
                    ORDER BY day DESC
                    LIMIT {min(200, int(CH_MAX_ROWS))}
                    """.strip()

                    try:
                        res = client.query(
                            sql_a,
                            parameters={"store": store, "start": str(start_d), "end": str(end_d)},
                        )
                        if res.result_rows:
                            cols = list(res.column_names or [])
                            rows = [list(r) for r in (res.result_rows or [])]
                            return (
                                "Text2SQL fallback (RULE aggregation)\n"
                                f"Reason: LLM failed ({type(llm_err).__name__})\n\n"
                                f"SQL:\n{sql_a}\n\n"
                                f"DATA (top {min(50, len(rows))}):\n{_format_table(cols, rows, 50)}"
                            )
                    except Exception:
                        continue

            for t in candidates[:5]:
                tbl = f"{t.db}.{t.table}"
                date_col = _pick_first_existing(t, ["SalesDate", "sale_date", "tr_date", "stock_date", "CRT_YMD"])
                metric_col = _pick_first_existing(
                    t, [metric_pref, "NetSale", "NetSales", "GrossSale", "GrossSales", "Qty", "SoldQty"]
                )
                if not (date_col and metric_col):
                    continue

                sql_a2 = f"""
                SELECT
                  sum({metric_col}) AS value
                FROM {tbl}
                WHERE {date_col} >= %(start)s
                  AND {date_col} <= %(end)s
                LIMIT 1
                """.strip()

                try:
                    res = client.query(sql_a2, parameters={"start": str(start_d), "end": str(end_d)})
                    if res.result_rows:
                        cols = list(res.column_names or [])
                        rows = [list(r) for r in (res.result_rows or [])]
                        return (
                            "Text2SQL fallback (RULE sum window)\n"
                            f"Reason: LLM failed ({type(llm_err).__name__})\n\n"
                            f"SQL:\n{sql_a2}\n\n"
                            f"DATA:\n{_format_table(cols, rows, 50)}"
                        )
                except Exception:
                    continue

            for tbl in candidate_tables:
                sql_b = f"SELECT * FROM {tbl} LIMIT 20"
                try:
                    res = client.query(sql_b)
                    if res.result_rows:
                        cols = list(res.column_names or [])
                        rows = [list(r) for r in (res.result_rows or [])]
                        return (
                            "Text2SQL fallback (SAMPLE rows)\n"
                            f"Reason: LLM/aggregation failed ({type(llm_err).__name__})\n\n"
                            f"SQL:\n{sql_b}\n\n"
                            f"DATA (top {min(20, len(rows))}):\n{_format_table(cols, rows, 20)}"
                        )
                except Exception:
                    continue

            out_msg = (
                "ClickHouse дээр query ажиллуулах боломжгүй/үр дүн олдсонгүй.\n"
                "Гэхдээ dictionary дээрээс хамгийн тохирох schema-ууд:\n"
            )
            for t in candidates[:3]:
                out_msg += f"\n- {t.db}.{t.table}: {(t.description or '')}\n"
                out_msg += "  cols: " + ", ".join([c.name for c in t.columns[:20]]) + "\n"
            return out_msg

        except Exception as ch_err:
            out_msg = (
                "Text2SQL: ClickHouse connection/query асуудалтай байна.\n"
                f"LLM error: {str(llm_err)}\n"
                f"CH error: {str(ch_err)}\n\n"
                "Dictionary top matches:\n"
            )
            for t in candidates[:3]:
                out_msg += f"- {t.db}.{t.table}\n"
            return out_msg
