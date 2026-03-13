# app/agents/text2sql/planner.py
import json
from typing import Any, Dict, List, Optional, Set

from app.core.llm import LLMClient
from app.agents.text2sql.plan_utils import safe_json_loads, normalize_plan
from app.agents.text2sql.intents import normalize_query
from app.core.schema_catalog import format_schema_for_prompt

llm = LLMClient()


def planner_system_prompt() -> str:
    return """
You are a ClickHouse Text-to-SQL planner.
Return ONLY valid JSON. No markdown. No explanation.

Your task is NOT to write final SQL.
Your task is to produce a SAFE SQL PLAN in JSON.

Strict rules:
- Use only tables from allowed_tables
- fact_table must be one of allowed_tables
- Fact alias must always be: f
- Joined aliases must be: d1, d2, d3 ...
- For sales-related questions, prefer BI_DB.Cluster_Main_Sales as fact_table
- Do not invent tables
- Do not invent columns
- Use aggregate functions correctly with GROUP BY
- Avoid selecting non-aggregated columns unless grouped
- Prefer concise plans
- Prefer business-canonical columns when available

Canonical business rules:
- Main sales fact table: BI_DB.Cluster_Main_Sales
- Sales amount column: NetSale
- Gross sales column: GrossSale
- Quantity column: SoldQty
- VAT column: Tax_VAT
- Discount column: Discount
- Sales date column: SalesDate
- Store id column: StoreID
- Product code column: GDS_CD
- Product name dimension: BI_DB.Dimension_IM
- Product name column: GDS_NM
- For product name requests, join BI_DB.Dimension_IM on f.GDS_CD = d1.GDS_CD
- Do not use f.Store if StoreID exists
- Do not use f.Item or f.Product if GDS_CD exists

Planning policy:
- If the question asks total sales, prefer sum(f.NetSale)
- If the question asks total quantity, prefer sum(f.SoldQty)
- If the question asks product name, include a join to Dimension_IM
- If the question asks store-level aggregation, group by f.StoreID
- If the question asks monthly trend, group by toYYYYMM(f.SalesDate)
- If the question asks quarter totals, filter by toMonth(f.SalesDate)

Return JSON with this exact shape:
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
            "question": "2025 онд хамгийн их борлуулалттай салбар",
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
                "order_by": ["ym"],
                "limit": 50,
            },
        },
    ]


def summarize_candidates(candidates: List[Any], rel_filtered: List[Dict[str, Any]], registry: Any) -> str:
    lines: List[str] = []
    lines.append("Candidate tables:")

    for i, t in enumerate(candidates[:8], start=1):
        try:
            highlights = registry.highlights(t)
        except Exception:
            highlights = {}

        cols = getattr(t, "columns", [])[:20]
        col_names = ", ".join([c.name for c in cols]) if cols else "-"
        date_cols = ", ".join(highlights.get("date_cols", [])) or "-"
        key_cols = ", ".join(highlights.get("key_cols", [])) or "-"
        metric_cols = ", ".join(highlights.get("metric_cols", [])) or "-"
        name_cols = ", ".join(highlights.get("name_cols", [])) or "-"

        lines.append(
            f"{i}. {t.db}.{t.table}\n"
            f"   entity: {getattr(t, 'entity', '') or '-'}\n"
            f"   description: {getattr(t, 'description', '') or '-'}\n"
            f"   sample_columns: {col_names}\n"
            f"   date_columns: {date_cols}\n"
            f"   key_columns: {key_cols}\n"
            f"   metric_columns: {metric_cols}\n"
            f"   name_columns: {name_cols}"
        )

    if rel_filtered:
        lines.append("\nRelationships:")
        for rel in rel_filtered[:20]:
            if rel.get("type") == "join_key":
                lines.append(f"- JOIN: {rel['left']} = {rel['right']}")
            elif rel.get("type") == "name_column":
                lines.append(f"- NAME COLUMN: {rel['table']}.{rel['name_column']}")

    return "\n".join(lines)


def build_user_payload(
        query: str,
        candidates: List[Any],
        rel_filtered: List[Dict[str, Any]],
        allowed_tables: Set[str],
        registry: Any,
) -> Dict[str, Any]:
    normalized = normalize_query(query)

    candidate_names = []
    for t in candidates[:8]:
        candidate_names.append(t.table)
        candidate_names.append(f"{t.db}.{t.table}")

    schema_text = format_schema_for_prompt(candidate_names)
    candidate_summary = summarize_candidates(candidates, rel_filtered, registry)

    payload = {
        "question": query,
        "normalized_question": normalized,
        "candidate_summary": candidate_summary,
        "schema_text": schema_text,
        "allowed_tables": sorted(list(allowed_tables))[:120],
        "relationships": rel_filtered[:25],
        "examples": planning_examples(),
    }
    return payload


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
        max_tokens=1200,
    )

    plan = safe_json_loads(out)
    if not isinstance(plan, dict):
        return None

    return normalize_plan(plan)
