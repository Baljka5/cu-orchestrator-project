# /app/app/agents/text2sql_agent.py
import re
import json
import clickhouse_connect
from typing import Any, Dict, List, Set

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

llm = LLMClient()

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()
_all_relationships = _registry.build_relationships()


# -------------------------------
def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def _wants_group_store(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "дэлгүүрээр", "салбараар", "салбар тус бүр", "store by", "per store"
    ])


def _run_sql_preview(sql: str, max_rows: int = 50) -> Dict[str, Any]:
    """
    Run SQL and return a small preview for UI rendering.
    """
    try:
        client = _ch_client()
        res = client.query(sql)
        cols = res.column_names or []
        rows = (res.result_rows or [])[:max_rows]
        return {"columns": cols, "rows": rows}
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}


# -------------------------------
def _wants_name(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "нэр", "name", "product name", "item name",
        "барааны нэр", "бүтээгдэхүүний нэр"
    ])


def _is_most_sold_intent(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["хамгийн их", "most sold", "их зарагдсан"])


# -------------------------------
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


# -------------------------------
def _filter_relationships(candidates: List[TableInfo]) -> List[Dict[str, Any]]:
    cand_tables = {t.table for t in candidates[:8]}
    cand_tables.add("Dimension_IM")  # force include

    rel_filtered = []
    for r in _all_relationships:
        if r.get("type") == "join_key":
            lt = r["left"].split(".", 1)[0]
            rt = r["right"].split(".", 1)[0]
            if lt in cand_tables and rt in cand_tables:
                rel_filtered.append(r)
        elif r.get("type") == "name_column":
            if r.get("table") in cand_tables:
                rel_filtered.append(r)

    rel_filtered.sort(key=lambda x: x.get("score", 0), reverse=True)
    return rel_filtered[:80]


# -------------------------------
def _inject_name_join(plan: Dict[str, Any],
                      candidates: List[TableInfo],
                      rel_filtered: List[Dict[str, Any]],
                      query: str) -> Dict[str, Any]:
    """
    General "name wants" injection based on registry relationships.
    (kept for backward compatibility)
    """
    if not _wants_name(query):
        return plan

    fact_full = (plan.get("fact_table") or "").strip()
    fact = fact_full.split()[0].split(".")[-1] if fact_full else candidates[0].table

    name_cols = [r for r in rel_filtered if r.get("type") == "name_column"]
    join_keys = [r for r in rel_filtered if r.get("type") == "join_key"]

    if not name_cols:
        return plan

    target = name_cols[0]
    dim_tbl = target["table"]
    name_col = target["name_column"]

    jk = None
    for r in join_keys:
        lt, lc = r["left"].split(".", 1)
        rt, rc = r["right"].split(".", 1)
        if (lt == fact and rt == dim_tbl) or (rt == fact and lt == dim_tbl):
            jk = r
            break

    if not jk:
        return plan

    plan.setdefault("joins", [])
    if not plan["joins"]:
        alias = "d1"
        lt, lc = jk["left"].split(".", 1)
        rt, rc = jk["right"].split(".", 1)
        if lt == fact:
            on = f"f.{lc} = {alias}.{rc}"
        else:
            on = f"f.{rc} = {alias}.{lc}"

        plan["joins"].append({
            "type": "LEFT",
            "table": _normalize_table_ref(dim_tbl),
            "alias": alias,
            "on": on
        })

    plan.setdefault("select", [])
    plan["select"].insert(0, {"expr": f"d1.{name_col}", "as": "item_name"})

    plan.setdefault("group_by", [])
    if plan.get("group_by") is not None:
        plan["group_by"].insert(0, f"d1.{name_col}")

    return plan


# -------------------------------
def _force_product_name_join(plan: Dict[str, Any], query: str) -> Dict[str, Any]:
    """
    Strong rule: if question asks for product name, force Dimension_IM join via GDS_CD.
    Works even if LLM forgets to join.
    """
    ql = (query or "").lower()
    wants_name = _wants_name(query)
    if not wants_name:
        return plan

    plan.setdefault("select", [])
    plan.setdefault("joins", [])
    plan.setdefault("where", [])
    plan.setdefault("group_by", [])
    plan.setdefault("order_by", [])
    plan.setdefault("limit", 50)

    # Ensure Dimension_IM join exists
    dim_join = None
    for j in plan["joins"]:
        if j.get("table", "").split(".")[-1] == "Dimension_IM":
            dim_join = j
            break

    if not dim_join:
        alias = f"d{len(plan['joins']) + 1}"
        dim_join = {
            "type": "LEFT",
            "table": f"{CLICKHOUSE_DATABASE}.Dimension_IM",
            "alias": alias,
            "on": f"f.GDS_CD = {alias}.GDS_CD"
        }
        plan["joins"].append(dim_join)

    alias = dim_join.get("alias") or "d1"

    # Ensure name select
    has_name = any(
        isinstance(x, dict) and ("GDS_NM" in (x.get("expr") or "") or x.get("as") in ("product_name", "item_name"))
        for x in plan["select"]
    )
    if not has_name:
        plan["select"].insert(0, {"expr": f"{alias}.GDS_NM", "as": "product_name"})

    # If "most sold" intent, enforce correct aggregate
    if _is_most_sold_intent(query):
        has_qty = any(isinstance(x, dict) and "SoldQty" in (x.get("expr") or "") for x in plan["select"])
        if not has_qty:
            plan["select"].append({"expr": "sum(f.SoldQty)", "as": "total_qty"})
        if f"{alias}.GDS_NM" not in plan["group_by"]:
            plan["group_by"].insert(0, f"{alias}.GDS_NM")
        plan["order_by"] = ["total_qty DESC"]
        plan["limit"] = 1

    return plan


# -------------------------------
def _extract_json(out: str) -> Dict[str, Any]:
    m = re.search(r"\{.*\}", out, re.DOTALL)
    raw = m.group(0) if m else out
    return json.loads(raw)


# -------------------------------
def _hard_rule_sql_only(query: str) -> str | None:
    """
    API is in SQL mode: return SQL string only for certain high-confidence intents.
    2025 онд хамгийн их зарагдсан бүтээгдэхүүний нэр -> JOIN Dimension_IM and return name.
    """
    ql = (query or "").lower()

    wants_name = _wants_name(query)
    most_sold = _is_most_sold_intent(query)

    m_year = re.search(r"\b(20\d{2})\b", query or "")
    year = int(m_year.group(1)) if m_year else None

    if not (wants_name and most_sold and year):
        return None

    return f"""
SELECT
  d1.GDS_NM AS product_name,
  sum(f.SoldQty) AS total_qty
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
LEFT JOIN {CLICKHOUSE_DATABASE}.Dimension_IM d1
  ON f.GDS_CD = d1.GDS_CD
WHERE toYear(f.SalesDate) = {year}
GROUP BY d1.GDS_NM
ORDER BY total_qty DESC
LIMIT 1
""".strip()


def _hard_rule_total_sales_sql_only(query: str) -> str | None:
    ql = (query or "").lower()

    # year must exist
    m_year = re.search(r"\b(20\d{2})\b", query or "")
    year = int(m_year.group(1)) if m_year else None
    if not year:
        return None

    # total sales intent
    wants_total = any(k in ql for k in ["нийт", "total", "sum"])
    wants_sales = any(k in ql for k in ["борлуулалт", "sales", "netsale", "grosssale", "орлого"])

    if not (wants_total and wants_sales):
        return None

    # If user explicitly asks per store/branch, do group by store.
    if _wants_group_store(query):
        return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales DESC
LIMIT 50
""".strip()

    # Otherwise: ONE NUMBER (no join, no group by)
    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
""".strip()


# -------------------------------
async def text2sql_answer(query: str) -> Dict[str, Any]:
    # 1) hard rule (SQL mode)
    hard_total = _hard_rule_total_sales_sql_only(query)
    if hard_total:
        return {"answer": hard_total, "meta": {"agent": "text2sql", "mode": "sql", "rule": "total_sales"}}

    hard_prod = _hard_rule_sql_only(query)
    if hard_prod:
        return {"answer": hard_prod, "meta": {"agent": "text2sql", "mode": "sql", "rule": "top_sold_product_name"}}

    hard_sql = _hard_rule_sql_only(query)
    if hard_sql:
        data = _run_sql_preview(hard_sql, max_rows=50)
        meta = {"agent": "text2sql", "mode": "sql", "rule": "top_sold_product_name", "data": data}
        if data.get("error"):
            meta["error"] = data["error"]

        return {"answer": hard_sql, "meta": meta}

    # 2) normal flow: candidate schema + LLM plan -> build SQL
    candidates = _registry.search(query, top_k=12)
    if not candidates:
        return {"answer": "Schema олдсонгүй.", "meta": {"agent": "text2sql", "mode": "sql"}}

    table_cards = [_registry.to_table_card(t, max_cols=80) for t in candidates[:8]]

    allowed_tables: Set[str] = set()
    for t in candidates:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")
    allowed_tables.add("Dimension_IM")
    allowed_tables.add(f"{CLICKHOUSE_DATABASE}.Dimension_IM")

    rel_filtered = _filter_relationships(candidates)

    system = """
You are a ClickHouse Text-to-SQL planner.
Return ONLY valid JSON (no markdown).

Hard rules:
- Use only tables in allowed_tables
- Fact alias must be f
- Joined tables must be d1, d2, ...
- Use sum(...) with GROUP BY for totals/top.
Return JSON schema:
{
  "fact_table": "DB.TABLE",
  "select": [{"expr":"...", "as":"..."}],
  "joins": [{"type":"LEFT","table":"DB.TABLE","alias":"d1","on":"f.col = d1.col"}],
  "where": ["..."],
  "group_by": ["..."],
  "order_by": ["..."],
  "limit": 50
}
""".strip()

    user = {
        "question": query,
        "table_cards": table_cards,
        "allowed_tables": sorted(list(allowed_tables))[:120],
        "relationships": rel_filtered[:60],
    }

    out = await llm.chat(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        temperature=0.0,
        max_tokens=900,
    )

    try:
        plan = _extract_json(out)
    except Exception:
        # fallback: just return raw
        return {"answer": out, "meta": {"agent": "text2sql", "mode": "sql", "error": "json_parse"}}

    # post-injections
    plan = _inject_name_join(plan, candidates, rel_filtered, query)
    plan = _force_product_name_join(plan, query)

    # build SQL
    fact = _normalize_table_ref(plan.get("fact_table") or f"{candidates[0].db}.{candidates[0].table}")
    if not _safe_table(fact, allowed_tables):
        return {"answer": "Unsafe fact_table", "meta": {"agent": "text2sql", "mode": "sql"}}

    select_items = plan.get("select") or [{"expr": "count()", "as": "cnt"}]
    select_clause = ", ".join(
        f"{x['expr']} AS {x['as']}" if x.get("as") else x["expr"]
        for x in select_items
    )

    sql = f"SELECT {select_clause}\nFROM {fact} f"

    for j in plan.get("joins", []) or []:
        jtype = (j.get("type") or "LEFT").upper()
        tbl = _normalize_table_ref(j.get("table") or "")
        alias = (j.get("alias") or "").strip() or "d1"
        on = (j.get("on") or "").strip()
        if not tbl or not on:
            continue
        if not _safe_table(tbl, allowed_tables):
            continue
        sql += f"\n{jtype} JOIN {tbl} {alias} ON {on}"

    wh = [x.strip() for x in (plan.get("where") or []) if isinstance(x, str) and x.strip()]
    if wh:
        sql += "\nWHERE " + " AND ".join(wh)

    gb = [x.strip() for x in (plan.get("group_by") or []) if isinstance(x, str) and x.strip()]
    if gb:
        sql += "\nGROUP BY " + ", ".join(gb)

    ob = [x.strip() for x in (plan.get("order_by") or []) if isinstance(x, str) and x.strip()]
    if ob:
        sql += "\nORDER BY " + ", ".join(ob)

    try:
        lim = int(plan.get("limit") or 50)
    except Exception:
        lim = 50
    lim = max(1, min(lim, 500))
    sql += f"\nLIMIT {lim}"

    data = _run_sql_preview(sql, max_rows=50)
    meta = {"agent": "text2sql", "mode": "sql", "data": data}
    if data.get("error"):
        meta["error"] = data["error"]

    return {"answer": sql, "meta": meta}
