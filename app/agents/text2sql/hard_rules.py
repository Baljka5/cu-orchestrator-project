# app/agents/text2sql/hard_rules.py
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.config import CLICKHOUSE_DATABASE
from app.agents.text2sql.intents import Intent, extract_year, extract_years, extract_quarter


def sales_fact() -> str:
    return f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales"


def hard_rule_dataset_help_text(query: str) -> Optional[str]:
    q = (query or "").lower()

    asks_where = any(k in q for k in [
        "хаана", "ямар table", "аль table", "ямар хүснэгт", "аль хүснэгт",
        "where", "which table", "table name"
    ])
    asks_sales = any(k in q for k in ["sales", "борлуул", "орлого", "netsale", "grosssale", "soldqty"])

    if not (asks_where and asks_sales):
        return None

    return (
        "Sales-ийн үндсэн detail дата **BI_DB.Cluster_Main_Sales** хүснэгт дээр байна.\n"
        "Түгээмэл хэмжигдэхүүнүүд: NetSale, GrossSale, SoldQty, Discount, Tax_VAT.\n"
        "Огноо: SalesDate, Дэлгүүр: StoreID, Бараа: GDS_CD.\n"
        "Бүтээгдэхүүний нэр хэрэгтэй бол **BI_DB.Dimension_IM**-тэй GDS_CD дээр join хийж GDS_NM авна."
    )


def hard_rule_table_about_text(query: str, registry: Any) -> Optional[str]:
    q = (query or "").strip()
    ql = q.lower()

    asks_about = any(k in ql for k in [
        "ямар дата", "ямар мэдээлэл", "юу байдаг", "ямар багана",
        "тайлбар", "about", "what data", "what is in", "columns"
    ])
    if not asks_about:
        return None

    m = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", q)
    if not m:
        return None

    tname = m.group(1)

    try:
        hits = registry.search(tname, top_k=3)
        if not hits:
            return None

        hits = sorted(hits, key=lambda x: 0 if x.table.lower() == tname.lower() else 1)
        t = hits[0]
        cols = [c.name for c in t.columns[:30]]
        more = "" if len(t.columns) <= 30 else f" … (+{len(t.columns) - 30} cols)"
        highlights = registry.highlights(t)

        return (
            f"**{t.db}.{t.table}** хүснэгт:\n"
            f"- Entity: {t.entity or '-'}\n"
            f"- Description: {t.description or '-'}\n"
            f"- Гол баганууд: {', '.join(cols)}{more}\n"
            f"- Date төрлийн магадлалтай: {', '.join(highlights.get('date_cols', [])) or '-'}\n"
            f"- Key/ID: {', '.join(highlights.get('key_cols', [])) or '-'}\n"
            f"- Metric: {', '.join(highlights.get('metric_cols', [])) or '-'}\n"
            f"- Name багана: {', '.join(highlights.get('name_cols', [])) or '-'}\n"
        )
    except Exception:
        return None


def hard_rule_yoy_growth_sql(query: str) -> Optional[str]:
    if not Intent.wants_yoy_growth(query):
        return None

    years = extract_years(query)
    if len(years) < 2:
        return None

    y1, y2 = years[0], years[1]

    return f"""
SELECT
  sumIf(f.NetSale, toYear(f.SalesDate) = {y2}) AS net_{y2},
  sumIf(f.NetSale, toYear(f.SalesDate) = {y1}) AS net_{y1},
  if(net_{y1} = 0, NULL,
     round((net_{y2} - net_{y1}) / net_{y1} * 100, 2)
  ) AS growth_pct
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) IN ({y1}, {y2})
""".strip()


def hard_rule_top_sold_product_name_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    if not year:
        return None

    if not (Intent.wants_name(query) and Intent.is_most_sold(query)):
        return None

    return f"""
SELECT
  d1.GDS_NM AS product_name,
  sum(f.SoldQty) AS total_qty
FROM {sales_fact()} f
LEFT JOIN {CLICKHOUSE_DATABASE}.Dimension_IM d1
  ON f.GDS_CD = d1.GDS_CD
WHERE toYear(f.SalesDate) = {year}
GROUP BY d1.GDS_NM
ORDER BY total_qty DESC
LIMIT 1
""".strip()


def hard_rule_total_qty_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    if not year:
        return None

    if not (Intent.wants_total(query) and Intent.wants_qty(query)):
        return None

    return f"""
SELECT
  sum(f.SoldQty) AS total_qty
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
""".strip()


def hard_rule_monthly_sales_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    if not year:
        return None

    if not (Intent.is_sales(query) and Intent.is_monthly(query)):
        return None

    return f"""
SELECT
  toYYYYMM(f.SalesDate) AS ym,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
GROUP BY ym
ORDER BY ym
""".strip()


def hard_rule_quarter_sales_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    quarter = extract_quarter(query)

    if not year or not quarter:
        return None

    if not (Intent.is_sales(query) and Intent.is_quarter(query)):
        return None

    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2

    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
  AND toMonth(f.SalesDate) BETWEEN {start_month} AND {end_month}
""".strip()


def hard_rule_top_n_sales_store_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    q = (query or "").lower()

    if not year:
        return None

    is_top10 = ("топ" in q or "top" in q) and ("10" in q)
    is_store = any(k in q for k in ["салбар", "дэлгүүр", "store"])
    wants_sales = Intent.is_sales(query)

    if not (is_top10 and is_store and wants_sales):
        return None

    return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales DESC
LIMIT 10
""".strip()


def hard_rule_total_sales_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    if not year:
        return None

    if not (Intent.wants_total(query) and Intent.is_sales(query)):
        return None

    if Intent.wants_group_store(query):
        return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales DESC
LIMIT 50
""".strip()

    if Intent.is_top_store(query):
        return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales DESC
LIMIT 1
""".strip()

    if Intent.is_bottom_store(query):
        return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales ASC
LIMIT 1
""".strip()

    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
""".strip()


HARD_SQL_RULES: List[Tuple[str, Callable[[str], Optional[str]]]] = [
    ("yoy_sales_growth_pct", hard_rule_yoy_growth_sql),
    ("top_sold_product_name", hard_rule_top_sold_product_name_sql),
    ("total_qty", hard_rule_total_qty_sql),
    ("monthly_sales_trend", hard_rule_monthly_sales_sql),
    ("quarter_sales_total", hard_rule_quarter_sales_sql),
    ("top10_store_sales", hard_rule_top_n_sales_store_sql),
    ("total_sales", hard_rule_total_sales_sql),
]