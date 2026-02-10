# app/agents/text2sql_agent.py
import re
import json
import clickhouse_connect
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    CH_MAX_ROWS, SCHEMA_DICT_PATH, CH_FALLBACK_TABLES,
    CH_DEFAULT_TABLE, CH_DEFAULT_STORE_COL, CH_DEFAULT_DATE_COL, CH_DEFAULT_METRIC_COL,
)

from app.core.llm import LLMClient
from app.core.schema_registry import SchemaRegistry, TableInfo, build_relationships


llm = LLMClient()

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()


# -------------------------------
# ClickHouse client
# -------------------------------
def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


# -------------------------------
# Simple extractors (mongolian-friendly)
# -------------------------------
def _extract_store_any(q: str) -> Optional[str]:
    m = re.search(r"(CU\d{3,4})", (q or "").upper())
    return m.group(1) if m else None


def _extract_days_any(q: str) -> int:
    ql = (q or "").lower()
    if "өнөөдөр" in ql:
        return 1
    if "өчигдөр" in ql:
        return 2
    m = re.search(r"(\d+)\s*(хоног|өдөр)", ql)
    if m:
        return max(1, min(int(m.group(1)), 90))
    if "7 хоног" in ql or "7хоног" in ql:
        return 7
    if "30 хоног" in ql or "30хоног" in ql:
        return 30
    return 7


def _is_most_sold_intent(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["их зарагдсан", "хамгийн их", "most sold", "best seller", "их борлогдсон"])


def _wants_amount(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["дүн", "орлого", "борлуулалтын дүн", "sales amount", "amount", "нет", "gross"])


def _pick_metric_default(q: str) -> str:
    """
    If 'most sold' and not asking amount => prefer SoldQty.
    Otherwise default NetSale.
    """
    ql = (q or "").lower()

    # explicit
    if any(k in ql for k in ["gross", "grosssale", "нийт"]):
        return "GrossSale"
    if any(k in ql for k in ["татвар", "vat", "tax"]):
        return "Tax_VAT"
    if any(k in ql for k in ["хөнгөлөлт", "discount"]):
        return "Discount"
    if any(k in ql for k in ["өртөг", "cost", "actualcost"]):
        return "ActualCost"

    # intent-based
    if _is_most_sold_intent(q) and not _wants_amount(q):
        return "SoldQty"

    return "NetSale"


# -------------------------------
# SQL safety
# -------------------------------
def _is_safe_sql(sql: str) -> bool:
    if not sql:
        return False
    s = sql.strip().lower()
    if not s.startswith("select"):
        return False
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


# -------------------------------
# Helpers: pick columns from schema
# -------------------------------
def _find_col(table: TableInfo, prefer: List[str]) -> Optional[str]:
    cols = [c.name for c in table.columns]
    lower_map = {c.lower(): c for c in cols}
    for p in prefer:
        if p.lower() in lower_map:
            return lower_map[p.lower()]
    return None


def _guess_name_column(dim: TableInfo) -> Optional[str]:
    # Typical item name columns
    prefer = ["ITEM_NM", "GDS_NM", "ITEM_NAME", "NAME", "ITEMNAME", "GDS_NAME"]
    return _find_col(dim, prefer)


def _build_join_hints(candidates: List[TableInfo]) -> List[Dict[str, str]]:
    """
    Auto infer join hints: if two tables share same column name (case-insensitive),
    we propose join edge. Prioritize keys like *_CD, *_ID, StoreID, GDS_CD, ITEM_CD, EVT_CD.
    """
    if not candidates:
        return []

    # map table -> columns set lower
    tcols = {}
    for t in candidates[:6]:
        tcols[t.table] = {c.name.lower(): c.name for c in t.columns}

    priority = ["gds_cd", "item_cd", "storeid", "store_id", "bizloc_cd", "evt_cd", "promotionid", "receiptno"]

    hints = []
    names = list(tcols.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = names[i], names[j]
            common = set(tcols[a].keys()).intersection(set(tcols[b].keys()))
            if not common:
                continue

            # choose best key
            key = None
            for p in priority:
                if p in common:
                    key = p
                    break
            if not key:
                # fallback: *_cd or *_id
                cd = [x for x in common if x.endswith("_cd")]
                if cd:
                    key = sorted(cd)[0]
                else:
                    _id = [x for x in common if x.endswith("_id")]
                    if _id:
                        key = sorted(_id)[0]
                    else:
                        continue

            hints.append({
                "left": f"{a}.{tcols[a][key]}",
                "right": f"{b}.{tcols[b][key]}",
                "why": f"shared key {tcols[a][key]}"
            })

    # keep small
    return hints[:12]


# -------------------------------
# PLAN -> SQL builder
# -------------------------------
def _normalize_table_name(name: str) -> str:
    # allow db.table or table
    return (name or "").strip()


def _table_allowed(name: str, allowed: set[str]) -> bool:
    if not name:
        return False
    n = name.strip()
    if n in allowed:
        return True
    # allow alias form "db.table alias"
    base = n.split()[0]
    return base in allowed


def _build_sql_from_plan(
    plan: Dict[str, Any],
    allowed_tables: set[str],
    candidates: List[TableInfo],
    query: str,
) -> Tuple[str, str]:
    """
    Build ClickHouse SQL deterministically.
    Expected plan shape:
    {
      "fact_table": "Cluster_Main_Sales",
      "select": [{"expr":"...","as":"..."}],
      "joins": [{"type":"LEFT","table":"Dimension_IM","on":"f.GDS_CD = d1.GDS_CD"}],
      "filters": ["toYear(f.SalesDate)=2025", ...],
      "group_by": ["..."],
      "order_by": [{"expr":"total_qty","dir":"DESC"}],
      "limit": 20,
      "notes": "..."
    }
    """
    notes = (plan.get("notes") or "").strip()

    fact = _normalize_table_name(plan.get("fact_table") or "")
    if not _table_allowed(fact, allowed_tables):
        # fallback: best candidate
        fact = f"{candidates[0].db}.{candidates[0].table}" if candidates else CH_DEFAULT_TABLE

    # alias
    fact_from = f"{fact} f"

    joins = plan.get("joins") or []
    join_sql_parts = []
    for idx, j in enumerate(joins[:4], start=1):
        jtype = (j.get("type") or "LEFT").upper()
        jtable = _normalize_table_name(j.get("table") or "")
        if not _table_allowed(jtable, allowed_tables):
            continue
        alias = f"d{idx}"
        # if table already has alias in text, keep it
        base = jtable.split()[0]
        join_tbl = f"{base} {alias}"
        on = (j.get("on") or "").strip()
        # enforce that on uses f. and d{idx}.
        if "f." not in on or alias + "." not in on:
            # attempt to map if user provided "im." etc -> replace with alias
            # but keep it simple: if invalid, skip join
            continue
        join_sql_parts.append(f"{jtype} JOIN {join_tbl} ON {on}")

    # SELECT
    select_items = plan.get("select") or []
    select_sql = []
    for it in select_items[:10]:
        expr = (it.get("expr") or "").strip()
        alias = (it.get("as") or "").strip()
        if not expr:
            continue
        if alias:
            select_sql.append(f"{expr} AS {alias}")
        else:
            select_sql.append(expr)

    if not select_sql:
        # smart default for common questions
        metric = _pick_metric_default(query)
        select_sql = [f"sum(f.{metric}) AS value"]

    # WHERE / PREWHERE
    filters = plan.get("filters") or []
    filters = [x.strip() for x in filters if isinstance(x, str) and x.strip()]
    where_sql = ""
    if filters:
        where_sql = "WHERE " + "\n  AND ".join(filters)

    group_by = plan.get("group_by") or []
    group_by = [x.strip() for x in group_by if isinstance(x, str) and x.strip()]
    group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""

    order_by = plan.get("order_by") or []
    order_items = []
    for ob in order_by[:3]:
        expr = (ob.get("expr") or "").strip()
        direction = (ob.get("dir") or "DESC").upper()
        if expr:
            order_items.append(f"{expr} {direction}")
    order_sql = f"ORDER BY {', '.join(order_items)}" if order_items else ""

    limit = int(plan.get("limit") or 50)
    limit = max(1, min(limit, min(CH_MAX_ROWS, 200)))

    sql = (
        "SELECT\n  " + ",\n  ".join(select_sql) + "\n"
        f"FROM {fact_from}\n"
        + ("\n".join(join_sql_parts) + "\n" if join_sql_parts else "")
        + (where_sql + "\n" if where_sql else "")
        + (group_sql + "\n" if group_sql else "")
        + (order_sql + "\n" if order_sql else "")
        + f"LIMIT {limit}"
    )

    return sql, notes


async def _llm_generate_plan(query: str, schema_ctx: List[Dict[str, Any]], join_hints: List[Dict[str, str]]) -> Dict[str, Any]:
    system = (
        "You are a ClickHouse analyst.\n"
        "Return ONLY JSON.\n"
        "Task: produce a QUERY PLAN (not SQL) from given schema candidates.\n"
        "Rules:\n"
        "- Use only the provided candidate tables.\n"
        "- If question asks 'хамгийн их зарагдсан/most sold' use SUM(quantity) not COUNT(*).\n"
        "- Prefer joining dimension tables to get names (e.g., item name) when requested.\n"
        "- Always include a limit (<=200).\n"
        "Output JSON shape:\n"
        "{\n"
        '  "fact_table":"db.table or table",\n'
        '  "select":[{"expr":"...","as":"..."}],\n'
        '  "joins":[{"type":"LEFT","table":"db.table or table","on":"f.KEY = d1.KEY"}],\n'
        '  "filters":["..."],\n'
        '  "group_by":["..."],\n'
        '  "order_by":[{"expr":"...","dir":"DESC"}],\n'
        '  "limit": 20,\n'
        '  "notes":"short"\n'
        "}\n"
        "Important: in joins.on always use aliases: fact is f, joined tables are d1, d2, ...\n"
    )

    user = {
        "question": query,
        "database_hint": CLICKHOUSE_DATABASE,
        "candidates": schema_ctx,
        "join_hints": join_hints,
        "relationships": relationships
    }

    prompt = [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
    ]

    out = await llm.chat(prompt, temperature=0.0, max_tokens=650)

    m = re.search(r"\{.*\}", out, re.DOTALL)
    raw = m.group(0) if m else out
    return json.loads(raw)


async def _llm_repair_plan(query: str, schema_ctx: List[Dict[str, Any]], join_hints: List[Dict[str, str]], error: str, prev_plan: Dict[str, Any]) -> Dict[str, Any]:
    system = (
        "Return ONLY JSON.\n"
        "You will REPAIR the plan based on the error.\n"
        "Use only candidate tables/columns. Keep joins aliases (f,d1,d2..).\n"
        "Fix the error and return a corrected plan JSON with same shape."
    )
    user = {
        "question": query,
        "error": error,
        "previous_plan": prev_plan,
        "candidates": schema_ctx,
        "join_hints": join_hints,
    }
    prompt = [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(user, ensure_ascii=False)}]
    out = await llm.chat(prompt, temperature=0.0, max_tokens=650)
    m = re.search(r"\{.*\}", out, re.DOTALL)
    raw = m.group(0) if m else out
    return json.loads(raw)


# -------------------------------
# MAIN: agent entry
# -------------------------------
async def text2sql_answer(query: str) -> Dict[str, Any]:
    """
    Return dict for orchestrator:
      {
        "final_answer": "...",
        "meta": {"sql": "...", "notes": "...", "data": {"columns":[...],"rows":[...]} }
      }
    """

    candidates = _registry.search(query, top_k=8)
    if not candidates:
        return {
            "final_answer": (
                "Text2SQL: Dictionary дээрээс тохирох хүснэгт олдсонгүй.\n"
                "Table/Column нэр, эсвэл бизнес түлхүүр үг (StoreID, SalesDate, NetSale гэх мэт) нэмээд асуугаарай."
            ),
            "meta": {"agent": "text2sql"}
        }

    # Build schema ctx with highlights
    schema_ctx = []
    for t in candidates[:5]:
        cols = [{"name": c.name, "type": c.dtype, "desc": c.attr} for c in t.columns[:70]]
        schema_ctx.append({
            "db": t.db,
            "table": t.table,
            "entity": t.entity,
            "description": (t.description or "")[:220],
            "highlights": _registry.highlights(t),
            "columns": cols
        })

    allowed_tables = set()
    for t in candidates[:8]:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")

    join_hints = _build_join_hints(candidates[:6])

    client = _ch_client()

    # ---- 1) PLAN from LLM
    plan = {}
    sql = ""
    notes = ""
    try:
        plan = await _llm_generate_plan(query, schema_ctx, join_hints)
        sql, notes = _build_sql_from_plan(plan, allowed_tables, candidates, query)

        if not _is_safe_sql(sql):
            raise ValueError("unsafe_sql")

        sql = _enforce_limit(sql, max(10, min(CH_MAX_ROWS, 200)))

        used = _tables_in_sql(sql)
        if used and not all(u in allowed_tables for u in used):
            raise ValueError(f"table_not_allowed: {sorted(list(used))}")

        res = client.query(sql)
        cols = res.column_names
        rows = res.result_rows

        if not rows:
            # ---- 2) refine once: expand date window if it's "most sold" yearly queries etc.
            # try repair plan
            plan2 = await _llm_repair_plan(query, schema_ctx, join_hints, "empty_result", plan)
            sql2, notes2 = _build_sql_from_plan(plan2, allowed_tables, candidates, query)
            if _is_safe_sql(sql2):
                sql2 = _enforce_limit(sql2, max(10, min(CH_MAX_ROWS, 200)))
                try:
                    res2 = client.query(sql2)
                    if res2.result_rows:
                        return {
                            "final_answer": f"Text2SQL\n{notes2}",
                            "meta": {
                                "agent": "text2sql",
                                "notes": notes2,
                                "sql": sql2,
                                "data": {"columns": res2.column_names, "rows": res2.result_rows[:50]},
                            }
                        }
                except Exception:
                    pass

            return {
                "final_answer": f"Text2SQL\n{notes}\n(хоосон үр дүн)",
                "meta": {"agent": "text2sql", "notes": notes, "sql": sql, "data": {"columns": cols, "rows": []}}
            }

        return {
            "final_answer": f"Text2SQL\n{notes}",
            "meta": {
                "agent": "text2sql",
                "notes": notes,
                "sql": sql,
                "data": {"columns": cols, "rows": rows[:50]},
            }
        }

    except Exception as llm_err:
        # ---- 3) RULE fallback (existing idea, but return meta)
        try:
            store = _extract_store_any(query)
            days = _extract_days_any(query)
            metric = _pick_metric_default(query)

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
""".strip()
                    try:
                        res = client.query(sql_a, parameters={
                            "store": store,
                            "start": str(start_d),
                            "end": str(end_d),
                        })
                        if res.result_rows:
                            return {
                                "final_answer": f"Text2SQL fallback (RULE)\nReason: {type(llm_err).__name__}",
                                "meta": {
                                    "agent": "text2sql",
                                    "notes": f"fallback RULE {store} {metric} {start_d}->{end_d}",
                                    "sql": sql_a,
                                    "data": {"columns": res.column_names, "rows": res.result_rows[:50]},
                                }
                            }
                    except Exception:
                        continue

            for tbl in candidate_tables:
                sql_b = f"SELECT * FROM {tbl} LIMIT 20"
                try:
                    res = client.query(sql_b)
                    if res.result_rows:
                        return {
                            "final_answer": f"Text2SQL fallback (SAMPLE)\nReason: {type(llm_err).__name__}",
                            "meta": {
                                "agent": "text2sql",
                                "notes": "fallback SAMPLE rows",
                                "sql": sql_b,
                                "data": {"columns": res.column_names, "rows": res.result_rows[:20]},
                            }
                        }
                except Exception:
                    continue

            out = (
                "ClickHouse дээр query ажиллуулах боломжгүй/үр дүн олдсонгүй.\n"
                "Гэхдээ dictionary дээрээс хамгийн тохирох schema-ууд:\n"
            )
            for t in candidates[:3]:
                out += f"\n- {t.db}.{t.table}: {(t.description or '')}\n"
                out += "  cols: " + ", ".join([c.name for c in t.columns[:20]]) + "\n"

            return {"final_answer": out, "meta": {"agent": "text2sql"}}

        except Exception as ch_err:
            return {
                "final_answer": (
                    "Text2SQL: ClickHouse connection/query асуудалтай байна.\n"
                    f"LLM error: {str(llm_err)}\n"
                    f"CH error: {str(ch_err)}"
                ),
                "meta": {"agent": "text2sql"}
            }
