# /app/app/agents/text2sql_agent.py
import re
import json
import clickhouse_connect
from typing import Any, Dict, List, Set, Optional

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
def _wants_group_store(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "дэлгүүрээр", "салбараар", "салбар тус бүр", "store by", "per store"
    ])


def _wants_name(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "нэр", "name", "product name", "item name",
        "барааны нэр", "бүтээгдэхүүний нэр"
    ])


def _is_most_sold_intent(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["хамгийн их", "most sold", "их зарагдсан"])


def _is_top_store_intent(q: str) -> bool:
    ql = (q or "").lower()
    return ("салбар" in ql or "дэлгүүр" in ql or "store" in ql) and any(
        k in ql for k in ["хамгийн их", "top", "их"]
    )


def _is_bottom_store_intent(q: str) -> bool:
    ql = (q or "").lower()
    return ("салбар" in ql or "дэлгүүр" in ql or "store" in ql) and any(
        k in ql for k in ["хамгийн бага", "bottom", "бага"]
    )


def _is_sales_intent(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["борлуулалт", "орлого", "sales", "netsale", "grosssale"])


def _is_monthly_intent(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["сар", "сар бүр", "monthly", "month", "тренд", "trend"])


def _is_quarter_intent(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["улирал", "quarter", "q1", "q2", "q3", "q4"])


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
    if not _wants_name(query):
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
        if (j.get("table", "") or "").split(".")[-1] == "Dimension_IM":
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
        isinstance(x, dict)
        and ("GDS_NM" in (x.get("expr") or "") or x.get("as") in ("product_name", "item_name"))
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
def _force_fact_sales_table(plan: Dict[str, Any], query: str) -> Dict[str, Any]:
    """
    Force sales-related questions to use Cluster_Main_Sales as the fact table.
    This prevents LLM from jumping to agg_sales_2025 / war_stock_* tables.
    """
    if not _is_sales_intent(query):
        return plan
    plan["fact_table"] = f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales"
    return plan


# -------------------------------
def _extract_json(out: str) -> Dict[str, Any]:
    m = re.search(r"\{.*\}", out, re.DOTALL)
    raw = m.group(0) if m else out
    return json.loads(raw)


def _extract_year(query: str) -> Optional[int]:
    m_year = re.search(r"\b(20\d{2})\b", query or "")
    return int(m_year.group(1)) if m_year else None


def _extract_quarter(query: str) -> Optional[int]:
    ql = (query or "").lower()
    # Mongolian: "1-р улирал" etc.
    m = re.search(r"(\d)\s*[-]?\s*р\s*улирал", ql)
    if m:
        return int(m.group(1))
    # English: Q1..Q4
    m2 = re.search(r"\bq([1-4])\b", ql)
    if m2:
        return int(m2.group(1))
    # generic "1 улирал"
    m3 = re.search(r"\b([1-4])\b.*улирал", ql)
    if m3:
        return int(m3.group(1))
    return None


# -------------------------------
def _hard_rule_top_sold_product_name_sql(query: str) -> Optional[str]:
    """
    2025 онд хамгийн их зарагдсан бүтээгдэхүүний нэр -> JOIN Dimension_IM and return name.
    """
    year = _extract_year(query)
    if not year:
        return None

    if not (_wants_name(query) and _is_most_sold_intent(query)):
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


def _hard_rule_total_sales_sql(query: str) -> Optional[str]:
    """
    Total NetSale for a year. If 'per store/branch' then group by store.
    Also supports top/bottom store in that year.
    """
    year = _extract_year(query)
    if not year:
        return None

    ql = (query or "").lower()
    wants_total = any(k in ql for k in ["нийт", "total", "sum"])
    wants_sales = _is_sales_intent(query)

    if not (wants_total and wants_sales):
        return None

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

    # Top/bottom store (single row)
    if _is_top_store_intent(query):
        return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales DESC
LIMIT 1
""".strip()

    if _is_bottom_store_intent(query):
        return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales ASC
LIMIT 1
""".strip()

    # One number
    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
""".strip()


def _hard_rule_total_qty_sql(query: str) -> Optional[str]:
    """
    2025 оны нийт зарагдсан ширхэг хэд вэ? -> sum(SoldQty)
    """
    year = _extract_year(query)
    if not year:
        return None

    ql = (query or "").lower()
    wants_total = any(k in ql for k in ["нийт", "total", "sum"])
    wants_qty = any(k in ql for k in ["ширхэг", "тоо", "quantity", "soldqty", "борлуулсан тоо", "зарагдсан ширхэг"])

    if not (wants_total and wants_qty):
        return None

    return f"""
SELECT
  sum(f.SoldQty) AS total_qty
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
""".strip()


def _hard_rule_monthly_sales_sql(query: str) -> Optional[str]:
    """
    2025 оны нийт орлого сард хэд байсан бэ? / 2025 онд сар бүрийн борлуулалтын тренд
    """
    year = _extract_year(query)
    if not year:
        return None

    if not (_is_sales_intent(query) and _is_monthly_intent(query)):
        return None

    return f"""
SELECT
  toYYYYMM(f.SalesDate) AS ym,
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
GROUP BY ym
ORDER BY ym
""".strip()


def _hard_rule_quarter_sales_sql(query: str) -> Optional[str]:
    """
    2025 оны 1-р улирлын нийт борлуулалт хэд вэ?
    """
    year = _extract_year(query)
    if not year:
        return None

    if not (_is_sales_intent(query) and _is_quarter_intent(query)):
        return None

    q = _extract_quarter(query)
    if not q or q not in (1, 2, 3, 4):
        return None

    # Quarter months mapping
    start_month = (q - 1) * 3 + 1
    end_month = start_month + 2

    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
  AND toMonth(f.SalesDate) BETWEEN {start_month} AND {end_month}
""".strip()


def _hard_rule_top_n_sales_store_sql(query: str) -> Optional[str]:
    """
    2025 онд ТОП 10 борлуулалттай салбар
    """
    year = _extract_year(query)
    if not year:
        return None

    ql = (query or "").lower()
    is_top10 = ("топ" in ql or "top" in ql) and ("10" in ql)
    is_store = ("салбар" in ql or "дэлгүүр" in ql or "store" in ql)
    wants_sales = _is_sales_intent(query)

    if not (is_top10 and is_store and wants_sales):
        return None

    return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales DESC
LIMIT 10
""".strip()


def _hard_rule_yoy_growth_sql(query: str) -> str | None:
    ql = (query or "").lower()
    wants_compare = any(k in ql for k in ["харьцуулах", "vs", "өнгөрсөн", "өссөн", "өсөлт", "how much increase"])
    wants_percent = any(k in ql for k in ["хувь", "%", "percent"])
    wants_sales = any(k in ql for k in ["борлуулалт", "орлого", "netsale", "sales"])

    # must contain 2 years
    years = re.findall(r"\b(20\d{2})\b", query or "")
    years = sorted({int(y) for y in years})
    if len(years) < 2:
        return None

    y1, y2 = years[0], years[1]  # older, newer
    if not (wants_compare and wants_percent and wants_sales):
        return None

    return f"""
SELECT
  sumIf(f.NetSale, toYear(f.SalesDate) = {y2}) AS net_{y2},
  sumIf(f.NetSale, toYear(f.SalesDate) = {y1}) AS net_{y1},
  if(net_{y1} = 0, NULL,
     round((net_{y2} - net_{y1}) / net_{y1} * 100, 2)
  ) AS growth_pct
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) IN ({y1}, {y2})
""".strip()


def _hard_rule_dataset_help_text(query: str) -> str | None:
    ql = (query or "").lower()

    asks_where = any(k in ql for k in [
        "хаана", "ямар table", "аль table", "ямар хүснэгт", "аль хүснэгт",
        "where", "which table", "table name"
    ])
    asks_sales = any(k in ql for k in ["sales", "борлуул", "орлого", "netsale", "grosssale", "soldqty"])

    if not (asks_where and asks_sales):
        return None

    return (
        "Sales-ийн үндсэн detail дата **BI_DB.Cluster_Main_Sales** хүснэгт дээр байна.\n"
        "Түгээмэл хэмжигдэхүүнүүд: NetSale, GrossSale, SoldQty, Discount, Tax_VAT.\n"
        "Огноо: SalesDate, Дэлгүүр: StoreID, Бараа: GDS_CD.\n"
        "Бүтээгдэхүүний нэр хэрэгтэй бол **BI_DB.Dimension_IM**-тэй GDS_CD дээр join хийж GDS_NM авна."
    )


# -------------------------------
def _sql_response(sql: str, rule: str) -> Dict[str, Any]:
    data = _run_sql_preview(sql, max_rows=50)
    meta = {"agent": "text2sql", "mode": "sql", "rule": rule, "data": data}
    if data.get("error"):
        meta["error"] = data["error"]
    return {"answer": sql, "meta": meta}


# -------------------------------
async def text2sql_answer(query: str) -> Dict[str, Any]:
    # 1) Hard rules first (always include preview data for UI)

    txt = _hard_rule_dataset_help_text(query)
    if txt:
        return {"answer": txt, "meta": {"agent": "text2sql", "mode": "text", "rule": "sales_dataset_help"}}

    sql = _hard_rule_yoy_growth_sql(query)
    if sql:
        return _sql_response(sql, "yoy_sales_growth_pct")

    sql = _hard_rule_top_sold_product_name_sql(query)
    if sql:
        return _sql_response(sql, "top_sold_product_name")

    sql = _hard_rule_total_qty_sql(query)
    if sql:
        return _sql_response(sql, "total_qty")

    sql = _hard_rule_monthly_sales_sql(query)
    if sql:
        return _sql_response(sql, "monthly_sales_trend")

    sql = _hard_rule_quarter_sales_sql(query)
    if sql:
        return _sql_response(sql, "quarter_sales_total")

    sql = _hard_rule_top_n_sales_store_sql(query)
    if sql:
        return _sql_response(sql, "top10_store_sales")

    sql = _hard_rule_total_sales_sql(query)
    if sql:
        return _sql_response(sql, "total_sales")

    # 2) normal flow: candidate schema + LLM plan -> build SQL
    candidates = _registry.search(query, top_k=8)
    if not candidates:
        return {"answer": "Schema олдсонгүй.", "meta": {"agent": "text2sql", "mode": "sql"}}

    # Prompt slimming to avoid 2048-token overflow
    # (reduce tables/cols/relationships sent to vLLM)
    # NOTE: SchemaRegistry in your project already has to_table_card(...)
    table_cards = [_registry.to_table_card(t, max_cols=25) for t in candidates[:4]]
    rel_filtered = _filter_relationships(candidates)[:20]

    allowed_tables: Set[str] = set()
    for t in candidates:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")
    allowed_tables.add("Dimension_IM")
    allowed_tables.add(f"{CLICKHOUSE_DATABASE}.Dimension_IM")
    allowed_tables.add("Cluster_Main_Sales")
    allowed_tables.add(f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales")

    system = """
You are a ClickHouse Text-to-SQL planner.
Return ONLY valid JSON (no markdown).

Hard rules:
- Use only tables in allowed_tables
- Fact alias must be f
- Joined tables must be d1, d2, ...
- Use sum(...) with GROUP BY for totals/top.
- For sales-related questions, prefer BI_DB.Cluster_Main_Sales as fact_table.
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
        "allowed_tables": sorted(list(allowed_tables))[:60],
        "relationships": rel_filtered,
    }

    out = await llm.chat(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        temperature=0.0,
        max_tokens=800,
    )

    try:
        plan = _extract_json(out)
    except Exception:
        return {"answer": out, "meta": {"agent": "text2sql", "mode": "sql", "error": "json_parse"}}

    # Post-injections / guards
    plan = _force_fact_sales_table(plan, query)
    plan = _inject_name_join(plan, candidates, rel_filtered, query)
    plan = _force_product_name_join(plan, query)

    # Build SQL
    fact = _normalize_table_ref(plan.get("fact_table") or f"{candidates[0].db}.{candidates[0].table}")
    if not _safe_table(fact, allowed_tables):
        return {"answer": "Unsafe fact_table", "meta": {"agent": "text2sql", "mode": "sql"}}

    select_items = plan.get("select") or [{"expr": "count()", "as": "cnt"}]
    select_clause = ", ".join(
        f"{x['expr']} AS {x['as']}" if x.get("as") else x["expr"]
        for x in select_items
        if isinstance(x, dict) and x.get("expr")
    )

    sql = f"SELECT {select_clause}\nFROM {fact} f"

    for j in plan.get("joins", []) or []:
        if not isinstance(j, dict):
            continue
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
