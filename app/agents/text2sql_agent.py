import json
from typing import Any, Dict, List, Optional, Set

from app.agents.text2sql.intents import Intent, normalize_query
from app.agents.text2sql.plan_utils import normalize_plan, safe_json_loads
from app.config import CLICKHOUSE_DATABASE
from app.core.llm import LLMClient
from app.core.schema_catalog import format_schema_for_prompt

llm = LLMClient()

CANONICAL_FACTS = {
    "sales": f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales",
    "inventory": f"{CLICKHOUSE_DATABASE}.war_stock_2024_MV",
    "product_master": f"{CLICKHOUSE_DATABASE}.Dimension_IM",
    "store_master": f"{CLICKHOUSE_DATABASE}.Dimension_SM",
    "promotion": f"{CLICKHOUSE_DATABASE}.Dimension_LEM",
}


def infer_business_domain(query: str) -> str:
    if Intent.is_sales(query):
        return "sales"
    if Intent.is_inventory_query(query):
        return "inventory"
    if Intent.is_promotion_query(query):
        return "promotion"
    if Intent.is_product_query(query) and not Intent.is_sales(query):
        return "product_master"
    if Intent.is_store_query(query) and not Intent.is_sales(query):
        return "store_master"
    return "unknown"


def planner_system_prompt() -> str:
    return f"""
You are a ClickHouse Text-to-SQL planner.

Return ONLY valid JSON.
No markdown.
No explanation.
No prose.

Your task is NOT to write final SQL.
Your task is to produce a SAFE SQL PLAN in JSON.

STRICT SAFETY RULES:
- Use ONLY tables from allowed_tables
- fact_table must be one of allowed_tables
- Use ONLY columns that appear in schema_text or candidate_summary
- NEVER invent tables
- NEVER invent columns
- NEVER invent aliases beyond: f, d1, d2, d3
- Fact alias must always be: f
- Joined aliases must be: d1, d2, d3 ...
- If the request is unrelated to database analytics, output:
  {{
    "fact_table": "",
    "select": [],
    "joins": [],
    "where": [],
    "group_by": [],
    "order_by": [],
    "limit": 0
  }}

CLICKHOUSE RULES:
- Use ClickHouse-compatible expressions only
- For current date use today()
- For current timestamp use now()
- For daily grouping prefer toDate(f.SalesDate)
- For monthly grouping prefer toYYYYMM(f.SalesDate)
- For year filter prefer toYear(f.SalesDate)

BUSINESS CANONICAL RULES:
- Main sales fact table: {CLICKHOUSE_DATABASE}.Cluster_Main_Sales
- Main inventory fact table: {CLICKHOUSE_DATABASE}.war_stock_2024_MV
- Product master table: {CLICKHOUSE_DATABASE}.Dimension_IM
- Store master table: {CLICKHOUSE_DATABASE}.Dimension_SM
- Event master table: {CLICKHOUSE_DATABASE}.Dimension_LEM
- Event goods table: {CLICKHOUSE_DATABASE}.Dimension_LEG

VERY IMPORTANT SEMANTIC RULES:
- Dimension_SM is store master
- Dimension_LEM is event/promotion master
- Dimension_LEG is event goods master
- Do NOT use Dimension_LEM as store dimension
- Do NOT use Dimension_LEG as store dimension

SALES CANONICAL COLUMNS:
- Sales amount column: NetSale
- Gross sales column: GrossSale
- Quantity column: SoldQty
- VAT column: Tax_VAT
- Discount column: Discount
- Sales date column: SalesDate
- Store id column: StoreID
- Product code column: GDS_CD
- Promotion/event id column: PromotionID

JOIN RULES:
- For product name requests, prefer:
  f.GDS_CD = d1.GDS_CD
- For store name requests from sales, prefer:
  f.StoreID = d1.BIZLOC_CD
- For event/promotion name requests, prefer:
  f.PromotionID = d1.EVT_CD
- Do not create joins unless needed

AGGREGATION RULES:
- If the question asks total sales, prefer sum(f.NetSale)
- If the question asks total quantity, prefer sum(f.SoldQty)
- If the question asks store-level aggregation, group by f.StoreID
- If the question asks product-level aggregation, group by f.GDS_CD unless product name is explicitly required
- If the question asks monthly trend, group by toYYYYMM(f.SalesDate)
- If the question asks daily trend, group by toDate(f.SalesDate)
- Avoid selecting non-aggregated columns unless they are in GROUP BY
- Prefer concise plans
- Prefer the canonical fact table for the detected business domain

OUTPUT FORMAT:
{{
  "fact_table": "DB.TABLE",
  "select": [{{"expr":"...", "as":"..."}}],
  "joins": [{{"type":"LEFT","table":"DB.TABLE","alias":"d1","on":"f.col = d1.col"}}],
  "where": ["..."],
  "group_by": ["..."],
  "order_by": ["..."],
  "limit": 50
}}
""".strip()


def planning_examples() -> List[Dict[str, Any]]:
    return [
        {
            "question": "2024 оны нийт борлуулалт",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [{"expr": "sum(f.NetSale)", "as": "total_net_sales"}],
                "joins": [],
                "where": ["toYear(f.SalesDate) = 2024"],
                "group_by": [],
                "order_by": [],
                "limit": 50,
            },
        },
        {
            "question": "2025 онд хамгийн их борлуулалттай 10 дэлгүүр",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [
                    {"expr": "f.StoreID", "as": "store_id"},
                    {"expr": "sum(f.NetSale)", "as": "total_net_sales"},
                ],
                "joins": [],
                "where": ["toYear(f.SalesDate) = 2025"],
                "group_by": ["f.StoreID"],
                "order_by": ["total_net_sales DESC"],
                "limit": 10,
            },
        },
        {
            "question": "2025 онд хамгийн бага борлуулалттай дэлгүүр",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [
                    {"expr": "f.StoreID", "as": "store_id"},
                    {"expr": "sum(f.NetSale)", "as": "total_net_sales"},
                ],
                "joins": [],
                "where": ["toYear(f.SalesDate) = 2025"],
                "group_by": ["f.StoreID"],
                "order_by": ["total_net_sales ASC"],
                "limit": 1,
            },
        },
        {
            "question": "2025 онд хамгийн их зарагдсан барааны нэр",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [
                    {"expr": "d1.GDS_NM", "as": "product_name"},
                    {"expr": "sum(f.SoldQty)", "as": "total_qty"},
                    {"expr": "sum(f.NetSale)", "as": "total_net_sales"},
                ],
                "joins": [
                    {
                        "type": "LEFT",
                        "table": "BI_DB.Dimension_IM",
                        "alias": "d1",
                        "on": "f.GDS_CD = d1.GDS_CD",
                    }
                ],
                "where": ["toYear(f.SalesDate) = 2025"],
                "group_by": ["d1.GDS_NM"],
                "order_by": ["total_qty DESC", "total_net_sales DESC"],
                "limit": 1,
            },
        },
        {
            "question": "2025 онд салбарын нэрээр борлуулалт",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [
                    {"expr": "d1.BIZLOC_NM", "as": "store_name"},
                    {"expr": "sum(f.NetSale)", "as": "total_net_sales"},
                ],
                "joins": [
                    {
                        "type": "LEFT",
                        "table": "BI_DB.Dimension_SM",
                        "alias": "d1",
                        "on": "f.StoreID = d1.BIZLOC_CD",
                    }
                ],
                "where": ["toYear(f.SalesDate) = 2025"],
                "group_by": ["d1.BIZLOC_NM"],
                "order_by": ["total_net_sales DESC"],
                "limit": 50,
            },
        },
        {
            "question": "2024 оны сарын борлуулалтын тренд",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [
                    {"expr": "toYYYYMM(f.SalesDate)", "as": "ym"},
                    {"expr": "sum(f.NetSale)", "as": "total_net_sales"},
                ],
                "joins": [],
                "where": ["toYear(f.SalesDate) = 2024"],
                "group_by": ["toYYYYMM(f.SalesDate)"],
                "order_by": ["ym ASC"],
                "limit": 50,
            },
        },
        {
            "question": "сүүлийн 7 хоногийн борлуулалтын тренд",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [
                    {"expr": "toDate(f.SalesDate)", "as": "dt"},
                    {"expr": "sum(f.NetSale)", "as": "total_net_sales"},
                ],
                "joins": [],
                "where": ["toDate(f.SalesDate) >= today() - 7"],
                "group_by": ["toDate(f.SalesDate)"],
                "order_by": ["dt ASC"],
                "limit": 50,
            },
        },
        {
            "question": "өнөөдрийн борлуулалтын дүн",
            "plan": {
                "fact_table": "BI_DB.Cluster_Main_Sales",
                "select": [{"expr": "sum(f.NetSale)", "as": "total_net_sales"}],
                "joins": [],
                "where": ["toDate(f.SalesDate) = today()"],
                "group_by": [],
                "order_by": [],
                "limit": 50,
            },
        },
        {
            "question": "store master table",
            "plan": {
                "fact_table": "BI_DB.Dimension_SM",
                "select": [{"expr": "f.BIZLOC_CD", "as": "store_id"}],
                "joins": [],
                "where": [],
                "group_by": [],
                "order_by": [],
                "limit": 50,
            },
        },
        {
            "question": "event master table",
            "plan": {
                "fact_table": "BI_DB.Dimension_LEM",
                "select": [{"expr": "f.EVT_CD", "as": "event_code"}],
                "joins": [],
                "where": [],
                "group_by": [],
                "order_by": [],
                "limit": 50,
            },
        },
        {
            "question": "hello",
            "plan": {
                "fact_table": "",
                "select": [],
                "joins": [],
                "where": [],
                "group_by": [],
                "order_by": [],
                "limit": 0,
            },
        },
    ]


def compact_table_summary(t: Any, registry: Any) -> str:
    highlights = registry.highlights(t)
    role = registry.infer_table_role(t)

    cols = getattr(t, "columns", [])[:25]
    col_names = ", ".join([c.name for c in cols]) if cols else "-"
    date_cols = ", ".join(highlights.get("date_cols", [])) or "-"
    key_cols = ", ".join(highlights.get("key_cols", [])) or "-"
    metric_cols = ", ".join(highlights.get("metric_cols", [])) or "-"
    name_cols = ", ".join(highlights.get("name_cols", [])) or "-"

    return (
        f"TABLE: {t.db}.{t.table}\n"
        f"ROLE: {role}\n"
        f"ENTITY: {getattr(t, 'entity', '') or '-'}\n"
        f"DESCRIPTION: {getattr(t, 'description', '') or '-'}\n"
        f"COLUMNS: {col_names}\n"
        f"DATE_COLUMNS: {date_cols}\n"
        f"KEY_COLUMNS: {key_cols}\n"
        f"METRIC_COLUMNS: {metric_cols}\n"
        f"NAME_COLUMNS: {name_cols}"
    )


def summarize_candidates(
        candidates: List[Any],
        rel_filtered: List[Dict[str, Any]],
        registry: Any,
        query: str,
) -> str:
    domain = infer_business_domain(query)
    lines: List[str] = [f"DETECTED_DOMAIN: {domain}", "CANDIDATE_TABLES:"]

    for t in candidates[:8]:
        lines.append(compact_table_summary(t, registry))
        lines.append("")

    if rel_filtered:
        lines.append("RELATIONSHIPS:")
        for rel in rel_filtered[:25]:
            if rel.get("type") == "join_key":
                lines.append(f"- JOIN_KEY: {rel['left']} = {rel['right']}")
            elif rel.get("type") == "name_column":
                lines.append(f"- NAME_COLUMN: {rel['table']}.{rel['name_column']}")

    lines.append("")
    lines.append("PREFERRED_CANONICAL_TABLES:")
    for k, v in CANONICAL_FACTS.items():
        lines.append(f"- {k}: {v}")

    return "\n".join(lines)


def select_candidate_names(query: str, candidates: List[Any]) -> List[str]:
    domain = infer_business_domain(query)

    prioritized: List[str] = []

    if domain == "sales":
        prioritized.extend(
            [
                f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales",
                f"{CLICKHOUSE_DATABASE}.Dimension_IM",
                f"{CLICKHOUSE_DATABASE}.Dimension_SM",
                f"{CLICKHOUSE_DATABASE}.Dimension_LEM",
            ]
        )
    elif domain == "inventory":
        prioritized.extend(
            [
                f"{CLICKHOUSE_DATABASE}.war_stock_2024_MV",
                f"{CLICKHOUSE_DATABASE}.war_stock_2025_MV",
                f"{CLICKHOUSE_DATABASE}.store_stock",
                f"{CLICKHOUSE_DATABASE}.store_stock_ttl",
                f"{CLICKHOUSE_DATABASE}.Dimension_IM",
                f"{CLICKHOUSE_DATABASE}.Dimension_SM",
            ]
        )
    elif domain == "product_master":
        prioritized.extend([f"{CLICKHOUSE_DATABASE}.Dimension_IM"])
    elif domain == "store_master":
        prioritized.extend([f"{CLICKHOUSE_DATABASE}.Dimension_SM"])
    elif domain == "promotion":
        prioritized.extend(
            [
                f"{CLICKHOUSE_DATABASE}.Dimension_LEM",
                f"{CLICKHOUSE_DATABASE}.Dimension_LEG",
            ]
        )

    for t in candidates[:8]:
        full_name = f"{t.db}.{t.table}"
        if full_name not in prioritized:
            prioritized.append(full_name)

    return prioritized[:10]


def build_user_payload(
        query: str,
        candidates: List[Any],
        rel_filtered: List[Dict[str, Any]],
        allowed_tables: Set[str],
        registry: Any,
) -> Dict[str, Any]:
    normalized = normalize_query(query)
    domain = infer_business_domain(query)

    candidate_names = select_candidate_names(query, candidates)
    schema_text = format_schema_for_prompt(candidate_names)
    candidate_summary = summarize_candidates(candidates, rel_filtered, registry, query)

    return {
        "question": query,
        "normalized_question": normalized,
        "detected_domain": domain,
        "candidate_summary": candidate_summary,
        "schema_text": schema_text,
        "allowed_tables": sorted(list(allowed_tables))[:100],
        "relationships": rel_filtered[:25],
        "examples": planning_examples(),
        "instructions": {
            "return_empty_plan_if_unrelated": True,
            "prefer_canonical_fact_by_domain": True,
            "never_invent_columns": True,
            "clickhouse_only": True,
        },
    }


def is_empty_plan(plan: Dict[str, Any]) -> bool:
    return (
            not plan.get("fact_table")
            and not plan.get("select")
            and not plan.get("joins")
            and not plan.get("where")
            and not plan.get("group_by")
            and not plan.get("order_by")
            and int(plan.get("limit") or 0) == 0
    )


async def plan_with_llm(
        query: str,
        candidates: List[Any],
        rel_filtered: List[Dict[str, Any]],
        allowed_tables: Set[str],
        registry: Any,
) -> Optional[Dict[str, Any]]:
    user_payload = build_user_payload(
        query=query,
        candidates=candidates,
        rel_filtered=rel_filtered,
        allowed_tables=allowed_tables,
        registry=registry,
    )

    out = await llm.chat(
        [
            {"role": "system", "content": planner_system_prompt()},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        temperature=0.0,
        max_tokens=1400,
    )

    plan = safe_json_loads(out)
    if not isinstance(plan, dict):
        return None

    plan = normalize_plan(plan)

    if is_empty_plan(plan):
        return plan

    return plan
