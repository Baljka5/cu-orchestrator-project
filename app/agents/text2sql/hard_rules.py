import re
from typing import Any, Optional

from app.config import CLICKHOUSE_DATABASE
from app.agents.text2sql.intents import (
    Intent,
    extract_year,
    extract_years,
    extract_quarter,
)

# =========================================================
# Canonical tables
# =========================================================

SALES_FACT = f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales"
PRODUCT_DIM = f"{CLICKHOUSE_DATABASE}.Dimension_IM"
STORE_DIM = f"{CLICKHOUSE_DATABASE}.Dimension_SM"
EVENT_DIM = f"{CLICKHOUSE_DATABASE}.Dimension_LEM"
EVENT_GOODS_DIM = f"{CLICKHOUSE_DATABASE}.Dimension_LEG"
INVENTORY_FACT = f"{CLICKHOUSE_DATABASE}.war_stock_2024_MV"


def sales_fact() -> str:
    return SALES_FACT


def product_dim() -> str:
    return PRODUCT_DIM


def store_dim() -> str:
    return STORE_DIM


def inventory_fact() -> str:
    return INVENTORY_FACT


# =========================================================
# Small helpers
# =========================================================

def _ql(query: str) -> str:
    return (query or "").strip().lower()


def _has_any(text: str, words: list[str]) -> bool:
    return any(w in text for w in words)


def _extract_top_n(query: str, default: int = 10) -> int:
    nums = re.findall(r"\b(\d{1,3})\b", query or "")
    if not nums:
        return default
    try:
        n = int(nums[0])
        return max(1, min(n, 100))
    except Exception:
        return default


def _looks_unrelated(query: str) -> bool:
    q = _ql(query)
    unrelated = [
        "hello", "hi", "hey", "сайн уу", "юу байна", "чи хэн бэ",
        "how are you", "who are you", "weather", "цаг агаар",
        "кино", "дуу", "music", "game", "тоглоом"
    ]
    return any(x in q for x in unrelated)


# =========================================================
# Help / schema text rules
# =========================================================

def hard_rule_dataset_help_text(query: str) -> Optional[str]:
    q = _ql(query)

    asks_where = any(k in q for k in [
        "хаана", "ямар table", "аль table", "ямар хүснэгт", "аль хүснэгт",
        "where", "which table", "table name"
    ])
    asks_sales = any(k in q for k in [
        "sales", "борлуул", "орлого", "netsale", "grosssale", "soldqty"
    ])

    if not (asks_where and asks_sales):
        return None

    return (
        "Sales-ийн үндсэн detail дата **"
        f"{SALES_FACT}"
        "** хүснэгт дээр байна.\n"
        "Түгээмэл хэмжигдэхүүнүүд: NetSale, GrossSale, SoldQty, Discount, Tax_VAT.\n"
        "Огноо: SalesDate.\n"
        "Дэлгүүрийн код: StoreID.\n"
        "Барааны код: GDS_CD.\n"
        f"Барааны нэр авах бол **{PRODUCT_DIM}**-тэй GDS_CD дээр join хийж GDS_NM авна.\n"
        f"Дэлгүүрийн нэр авах бол **{STORE_DIM}**-тэй StoreID = BIZLOC_CD дээр join хийж BIZLOC_NM авна."
    )


def hard_rule_inventory_dataset_help_text(query: str) -> Optional[str]:
    q = _ql(query)

    asks_where = any(k in q for k in [
        "хаана", "ямар table", "аль table", "ямар хүснэгт", "which table"
    ])
    asks_inventory = any(k in q for k in [
        "stock", "inventory", "үлдэгдэл", "агуулах", "on hand"
    ])

    if not (asks_where and asks_inventory):
        return None

    return (
        "Inventory / stock-ийн дата голчлон **"
        f"{INVENTORY_FACT}"
        "** хүснэгт дээр байна.\n"
        f"Барааны master/name авах бол **{PRODUCT_DIM}**-тэй join хий.\n"
        f"Store info хэрэгтэй бол **{STORE_DIM}** ашиглаж болно."
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


def hard_rule_sales_related_tables_text(query: str, registry: Any) -> Optional[str]:
    ql = _ql(query)

    if not any(k in ql for k in ["ямар table", "аль table", "хүснэгтүүд", "tables", "table list"]):
        return None

    if not any(k in ql for k in ["sales", "борлуул", "product", "бараа", "store", "салбар"]):
        return None

    lines = [
        f"- {SALES_FACT}: Sales transactions fact",
        f"- {PRODUCT_DIM}: Item master / product dimension",
        f"- {STORE_DIM}: Store master",
        f"- {EVENT_DIM}: Event master",
        f"- {EVENT_GOODS_DIM}: Event goods master",
    ]
    return "Холбоотой хүснэгтүүд:\n" + "\n".join(lines)


def hard_rule_out_of_domain_text(query: str) -> Optional[str]:
    if _looks_unrelated(query):
        return (
            "Энэ асуулт нь text2sql domain-д хамаарахгүй байна. "
            "Борлуулалт, дэлгүүр, бүтээгдэхүүн, үлдэгдэлтэй холбоотой асуулт асууна уу."
        )
    return None


# =========================================================
# Sales SQL rules
# =========================================================

def hard_rule_today_sales_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not _has_any(q, ["өнөөдөр", "today"]):
        return None
    if not _has_any(q, ["борлуул", "sales", "орлого", "netsale", "grosssale"]):
        return None

    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toDate(f.SalesDate) = today()
""".strip()


def hard_rule_yesterday_sales_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not _has_any(q, ["өчигдөр", "yesterday"]):
        return None
    if not _has_any(q, ["борлуул", "sales", "орлого", "netsale", "grosssale"]):
        return None

    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toDate(f.SalesDate) = today() - 1
""".strip()


def hard_rule_last_7_days_sales_trend_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not _has_any(q, ["7 хоног", "7 day", "last 7", "сүүлийн 7"]):
        return None
    if not _has_any(q, ["борлуул", "sales", "орлого", "trend", "тренд"]):
        return None

    return f"""
SELECT
  toDate(f.SalesDate) AS dt,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toDate(f.SalesDate) >= today() - 6
GROUP BY dt
ORDER BY dt ASC
LIMIT 50
""".strip()


def hard_rule_daily_average_sales_sql(query: str) -> Optional[str]:
    q = _ql(query)
    year = extract_year(query)

    if not _has_any(q, ["дундаж", "average", "avg"]):
        return None
    if not _has_any(q, ["өдөр бүр", "өдрийн", "daily", "per day"]):
        return None
    if not _has_any(q, ["борлуул", "sales", "орлого"]):
        return None

    if year:
        return f"""
SELECT
  round(avg(daily_sales), 2) AS avg_daily_net_sales
FROM
(
  SELECT
    toDate(f.SalesDate) AS dt,
    sum(f.NetSale) AS daily_sales
  FROM {sales_fact()} f
  WHERE toYear(f.SalesDate) = {year}
  GROUP BY dt
)
""".strip()

    return f"""
SELECT
  round(avg(daily_sales), 2) AS avg_daily_net_sales
FROM
(
  SELECT
    toDate(f.SalesDate) AS dt,
    sum(f.NetSale) AS daily_sales
  FROM {sales_fact()} f
  GROUP BY dt
)
""".strip()


def hard_rule_total_sales_year_only_sql(query: str) -> Optional[str]:
    q = _ql(query)
    year = extract_year(query)
    if not year:
        return None

    sales_words = ["борлуул", "sales", "netsale", "grosssale", "orlogo", "орлого"]
    total_words = ["нийт", "total", "sum", "niit"]

    if not any(k in q for k in sales_words):
        return None
    if not any(k in q for k in total_words):
        return None

    return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
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


def hard_rule_top_store_sales_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    if not year:
        return None

    if not (Intent.is_sales(query) and Intent.is_top_store(query)):
        return None

    q = _ql(query)
    if _has_any(q, ["10", "топ 10", "top 10"]):
        return None

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


def hard_rule_bottom_store_sales_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    if not year:
        return None

    q = _ql(query)
    if not _has_any(q, ["хамгийн бага", "bottom", "lowest", "worst", "бага"]):
        return None
    if not _has_any(q, ["дэлгүүр", "салбар", "store", "branch"]):
        return None
    if not Intent.is_sales(query):
        return None

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


def hard_rule_top_n_sales_store_sql(query: str) -> Optional[str]:
    q = _ql(query)
    year = extract_year(query)

    if not year:
        return None

    is_store = any(k in q for k in ["салбар", "дэлгүүр", "store", "branch"])
    wants_sales = Intent.is_sales(query)
    wants_top = any(k in q for k in ["хамгийн их", "top", "топ", "highest", "most"])
    n = _extract_top_n(query, default=10)

    if not (is_store and wants_sales and wants_top and n >= 2):
        return None

    return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales DESC
LIMIT {n}
""".strip()


def hard_rule_top_n_sales_store_with_name_sql(query: str) -> Optional[str]:
    q = _ql(query)
    year = extract_year(query)

    if not year:
        return None

    is_store = any(k in q for k in ["салбар", "дэлгүүр", "store", "branch"])
    wants_sales = Intent.is_sales(query)
    wants_top = any(k in q for k in ["хамгийн их", "top", "топ", "highest", "most"])
    wants_name = any(k in q for k in ["нэр", "name"])
    n = _extract_top_n(query, default=10)

    if not (is_store and wants_sales and wants_top and wants_name):
        return None

    return f"""
SELECT
  s.BIZLOC_CD AS store_id,
  s.BIZLOC_NM AS store_name,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
LEFT JOIN {store_dim()} s
  ON f.StoreID = s.BIZLOC_CD
WHERE toYear(f.SalesDate) = {year}
GROUP BY s.BIZLOC_CD, s.BIZLOC_NM
ORDER BY total_net_sales DESC
LIMIT {n}
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
ORDER BY ym ASC
LIMIT 50
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


def hard_rule_same_year_quarter_compare_sql(query: str) -> Optional[str]:
    q = _ql(query)
    year = extract_year(query)
    if not year:
        return None

    if not any(k in q for k in ["харьцуул", "compare", "vs", "ялгаа"]):
        return None

    quarters = re.findall(r"\bq([1-4])\b", q)
    if len(quarters) < 2:
        quarters = re.findall(r"([1-4])\s*[-]?\s*р\s*улирал", q)

    if len(quarters) < 2:
        return None

    q1 = int(quarters[0])
    q2 = int(quarters[1])

    if q1 == q2:
        return None

    q1_start = (q1 - 1) * 3 + 1
    q1_end = q1_start + 2
    q2_start = (q2 - 1) * 3 + 1
    q2_end = q2_start + 2

    return f"""
SELECT
  sumIf(f.NetSale, toYear(f.SalesDate) = {year} AND toMonth(f.SalesDate) BETWEEN {q1_start} AND {q1_end}) AS q{q1}_sales,
  sumIf(f.NetSale, toYear(f.SalesDate) = {year} AND toMonth(f.SalesDate) BETWEEN {q2_start} AND {q2_end}) AS q{q2}_sales,
  (q{q2}_sales - q{q1}_sales) AS diff_amount,
  if(q{q1}_sales = 0, NULL, round((q{q2}_sales - q{q1}_sales) / q{q1}_sales * 100, 2)) AS diff_pct
FROM {sales_fact()} f
""".strip()


def hard_rule_cross_year_quarter_compare_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["харьцуул", "compare", "vs", "ялгаа"]):
        return None

    pairs = re.findall(r"(20\d{2}).*?q([1-4])", q)
    if len(pairs) < 2:
        return None

    (y1, q1), (y2, q2) = pairs[0], pairs[1]
    y1, q1, y2, q2 = int(y1), int(q1), int(y2), int(q2)

    q1_start = (q1 - 1) * 3 + 1
    q1_end = q1_start + 2
    q2_start = (q2 - 1) * 3 + 1
    q2_end = q2_start + 2

    return f"""
SELECT
  sumIf(f.NetSale, toYear(f.SalesDate) = {y1} AND toMonth(f.SalesDate) BETWEEN {q1_start} AND {q1_end}) AS y{y1}_q{q1}_sales,
  sumIf(f.NetSale, toYear(f.SalesDate) = {y2} AND toMonth(f.SalesDate) BETWEEN {q2_start} AND {q2_end}) AS y{y2}_q{q2}_sales,
  (y{y2}_q{q2}_sales - y{y1}_q{q1}_sales) AS diff_amount,
  if(y{y1}_q{q1}_sales = 0, NULL, round((y{y2}_q{q2}_sales - y{y1}_q{q1}_sales) / y{y1}_q{q1}_sales * 100, 2)) AS diff_pct
FROM {sales_fact()} f
""".strip()


def hard_rule_same_quarter_two_years_sql(query: str) -> Optional[str]:
    q = _ql(query)
    years = extract_years(query)
    quarter = extract_quarter(query)

    if len(years) < 2 or not quarter:
        return None

    if not any(k in q for k in ["харьцуул", "compare", "vs", "ялгаа"]):
        return None

    y1, y2 = years[0], years[1]
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2

    return f"""
SELECT
  sumIf(f.NetSale, toYear(f.SalesDate) = {y1} AND toMonth(f.SalesDate) BETWEEN {start_month} AND {end_month}) AS y{y1}_q{quarter}_sales,
  sumIf(f.NetSale, toYear(f.SalesDate) = {y2} AND toMonth(f.SalesDate) BETWEEN {start_month} AND {end_month}) AS y{y2}_q{quarter}_sales,
  (y{y2}_q{quarter}_sales - y{y1}_q{quarter}_sales) AS diff_amount,
  if(y{y1}_q{quarter}_sales = 0, NULL, round((y{y2}_q{quarter}_sales - y{y1}_q{quarter}_sales) / y{y1}_q{quarter}_sales * 100, 2)) AS diff_pct
FROM {sales_fact()} f
""".strip()


def hard_rule_monthly_compare_two_years_sql(query: str) -> Optional[str]:
    q = _ql(query)
    years = extract_years(query)
    if len(years) < 2:
        return None

    if not any(k in q for k in ["сар", "monthly", "month"]):
        return None
    if not any(k in q for k in ["харьцуул", "compare", "vs"]):
        return None

    y1, y2 = years[0], years[1]

    return f"""
SELECT
  toMonth(f.SalesDate) AS month_no,
  sumIf(f.NetSale, toYear(f.SalesDate) = {y1}) AS sales_{y1},
  sumIf(f.NetSale, toYear(f.SalesDate) = {y2}) AS sales_{y2},
  (sales_{y2} - sales_{y1}) AS diff_amount,
  if(sales_{y1} = 0, NULL, round((sales_{y2} - sales_{y1}) / sales_{y1} * 100, 2)) AS diff_pct
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) IN ({y1}, {y2})
GROUP BY month_no
ORDER BY month_no ASC
LIMIT 50
""".strip()


def hard_rule_yoy_growth_sql(query: str) -> Optional[str]:
    if not Intent.wants_yoy_growth(query):
        return None

    years = extract_years(query)
    if len(years) >= 2:
        y1, y2 = years[0], years[1]
    else:
        year = extract_year(query)
        if not year:
            return None
        y2 = year
        y1 = year - 1

    return f"""
SELECT
  sumIf(f.NetSale, toYear(f.SalesDate) = {y2}) AS net_{y2},
  sumIf(f.NetSale, toYear(f.SalesDate) = {y1}) AS net_{y1},
  if(net_{y1} = 0, NULL, round((net_{y2} - net_{y1}) / net_{y1} * 100, 2)) AS growth_pct
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) IN ({y1}, {y2})
""".strip()


def hard_rule_top_growth_store_yoy_sql(query: str) -> Optional[str]:
    q = _ql(query)
    years = extract_years(query)

    if len(years) >= 2:
        y1, y2 = years[0], years[1]
    else:
        y2 = extract_year(query)
        if not y2:
            return None
        y1 = y2 - 1

    if not any(k in q for k in ["салбар", "дэлгүүр", "store", "branch"]):
        return None
    if not any(k in q for k in ["хамгийн их өссөн", "most increased", "most growth", "их өссөн"]):
        return None
    if not Intent.is_sales(query):
        return None

    return f"""
SELECT
  f.StoreID AS store_id,
  sumIf(f.NetSale, toYear(f.SalesDate) = {y1}) AS sales_{y1},
  sumIf(f.NetSale, toYear(f.SalesDate) = {y2}) AS sales_{y2},
  (sales_{y2} - sales_{y1}) AS growth_amount,
  if(sales_{y1} = 0, NULL, round((sales_{y2} - sales_{y1}) / sales_{y1} * 100, 2)) AS growth_pct
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) IN ({y1}, {y2})
GROUP BY f.StoreID
ORDER BY growth_amount DESC
LIMIT 1
""".strip()


# =========================================================
# Product SQL rules
# =========================================================

def hard_rule_top_product_sales_sql(query: str) -> Optional[str]:
    year = extract_year(query)
    if not year:
        return None

    q = _ql(query)
    wants_top_product = Intent.is_product_query(query) and any(k in q for k in ["хамгийн их", "top", "их"])
    wants_sales_or_qty = Intent.is_sales(query) or Intent.wants_qty(query) or "зарагдсан" in q

    if not (wants_top_product and wants_sales_or_qty):
        return None

    if Intent.wants_name(query) or "юу" in q or "аль" in q:
        return f"""
SELECT
  d1.GDS_NM AS product_name,
  sum(f.SoldQty) AS total_qty,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
LEFT JOIN {product_dim()} d1
  ON f.GDS_CD = d1.GDS_CD
WHERE toYear(f.SalesDate) = {year}
GROUP BY d1.GDS_NM
ORDER BY total_qty DESC, total_net_sales DESC
LIMIT 1
""".strip()

    return f"""
SELECT
  f.GDS_CD AS product_code,
  sum(f.SoldQty) AS total_qty,
  sum(f.NetSale) AS total_net_sales
FROM {sales_fact()} f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.GDS_CD
ORDER BY total_qty DESC, total_net_sales DESC
LIMIT 1
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
LEFT JOIN {product_dim()} d1
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


# =========================================================
# Inventory / master data SQL rules
# =========================================================

def hard_rule_inventory_total_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["stock", "inventory", "үлдэгдэл", "агуулах", "on hand"]):
        return None

    if any(k in q for k in ["нэр", "name", "product name", "барааны нэр"]):
        return None

    return f"""
SELECT
  sum(f.StockQty) AS total_stock_qty
FROM {inventory_fact()} f
LIMIT 50
""".strip()


def hard_rule_inventory_with_product_name_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["stock", "inventory", "үлдэгдэл", "агуулах", "on hand"]):
        return None

    if not any(k in q for k in ["нэр", "name", "product name", "барааны нэр"]):
        return None

    return f"""
SELECT
  d1.GDS_NM AS product_name,
  sum(f.StockQty) AS total_stock_qty
FROM {inventory_fact()} f
LEFT JOIN {product_dim()} d1
  ON f.GDS_CD = d1.GDS_CD
GROUP BY d1.GDS_NM
ORDER BY total_stock_qty DESC
LIMIT 50
""".strip()


def hard_rule_product_list_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["product list", "item master", "барааны жагсаалт", "product master"]):
        return None

    return f"""
SELECT
  f.GDS_CD AS product_code,
  f.GDS_NM AS product_name
FROM {product_dim()} f
LIMIT 50
""".strip()


def hard_rule_store_list_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["store list", "branch master", "салбарын мэдээлэл", "дэлгүүрийн жагсаалт"]):
        return None

    return f"""
SELECT
  f.BIZLOC_CD AS store_id,
  f.BIZLOC_NM AS store_name
FROM {store_dim()} f
LIMIT 50
""".strip()


def hard_rule_category_list_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["product category list", "category list", "ангиллын жагсаалт"]):
        return None

    return f"""
SELECT DISTINCT
  f.CategoryName AS category_name
FROM {product_dim()} f
WHERE f.CategoryName IS NOT NULL
ORDER BY category_name
LIMIT 100
""".strip()


def hard_rule_brand_list_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["brand list", "брэндийн жагсаалт"]):
        return None

    return f"""
SELECT DISTINCT
  f.BrandName AS brand_name
FROM {product_dim()} f
WHERE f.BrandName IS NOT NULL
ORDER BY brand_name
LIMIT 100
""".strip()


def hard_rule_supplier_list_sql(query: str) -> Optional[str]:
    q = _ql(query)

    if not any(k in q for k in ["supplier list", "vendor list", "нийлүүлэгчдийн жагсаалт"]):
        return None

    return f"""
SELECT DISTINCT
  f.SupplierName AS supplier_name
FROM {product_dim()} f
WHERE f.SupplierName IS NOT NULL
ORDER BY supplier_name
LIMIT 100
""".strip()


# =========================================================
# Ordered rule registry
# =========================================================

HARD_SQL_RULES = [
    # recent/date-based sales first
    ("today_sales", hard_rule_today_sales_sql),
    ("yesterday_sales", hard_rule_yesterday_sales_sql),
    ("last_7_days_sales_trend", hard_rule_last_7_days_sales_trend_sql),
    ("daily_average_sales", hard_rule_daily_average_sales_sql),

    # inventory / master data
    ("inventory_total", hard_rule_inventory_total_sql),
    ("inventory_with_product_name", hard_rule_inventory_with_product_name_sql),
    ("product_list", hard_rule_product_list_sql),
    ("store_list", hard_rule_store_list_sql),
    ("category_list", hard_rule_category_list_sql),
    ("brand_list", hard_rule_brand_list_sql),
    ("supplier_list", hard_rule_supplier_list_sql),

    # sales
    ("total_sales_year_only", hard_rule_total_sales_year_only_sql),
    ("yoy_sales_growth_pct", hard_rule_yoy_growth_sql),
    ("top_store_sales", hard_rule_top_store_sales_sql),
    ("bottom_store_sales", hard_rule_bottom_store_sales_sql),
    ("top_store_sales_with_name", hard_rule_top_n_sales_store_with_name_sql),
    ("top_product_sales", hard_rule_top_product_sales_sql),
    ("same_year_quarter_compare", hard_rule_same_year_quarter_compare_sql),
    ("cross_year_quarter_compare", hard_rule_cross_year_quarter_compare_sql),
    ("same_quarter_two_years_compare", hard_rule_same_quarter_two_years_sql),
    ("monthly_compare_two_years", hard_rule_monthly_compare_two_years_sql),
    ("top_growth_store_yoy", hard_rule_top_growth_store_yoy_sql),
    ("top_sold_product_name", hard_rule_top_sold_product_name_sql),
    ("total_qty", hard_rule_total_qty_sql),
    ("monthly_sales_trend", hard_rule_monthly_sales_sql),
    ("quarter_sales_total", hard_rule_quarter_sales_sql),
    ("top_n_store_sales", hard_rule_top_n_sales_store_sql),
    ("total_sales", hard_rule_total_sales_sql),
]
