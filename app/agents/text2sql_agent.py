from typing import Any, Dict, Optional

from app.agents.text2sql.executor import run_sql_preview
from app.agents.text2sql.hard_rules import (
    hard_rule_dataset_help_text,
    hard_rule_table_about_text,
    hard_rule_sales_related_tables_text,
    HARD_SQL_RULES,
)
from app.agents.planner import plan_with_llm
from app.agents.text2sql.postprocess import (
    force_fact_table_by_domain,
    inject_name_join_from_registry,
    ensure_product_name_join,
    repair_canonical_columns,
    drop_suspicious_joins,
)
from app.agents.text2sql.registry_utils import (
    registry,
    filter_relationships,
    build_allowed_tables,
)
from app.agents.text2sql.sql_builder import build_sql_from_plan
from app.agents.text2sql.response import text_response, sql_response, error_response
from app.agents.text2sql.history import persist_result
from app.agents.text2sql.intents import normalize_query
from app.config import CLICKHOUSE_DATABASE
from app.agents.text2sql.query_router import classify_query_domain
from app.agents.text2sql.registry_utils import rerank_candidates
from app.agents.text2sql.validator import validate_and_repair_plan
from app.agents.text2sql.intents import Intent, extract_year, extract_quarter


def fallback_sql_by_domain(query: str) -> Optional[str]:
    year = extract_year(query)

    if Intent.is_inventory_query(query):
        if Intent.wants_name(query):
            return f"""
SELECT
  d1.GDS_NM AS product_name,
  sum(f.StockQty) AS total_stock_qty
FROM {CLICKHOUSE_DATABASE}.war_stock_2024_MV f
LEFT JOIN {CLICKHOUSE_DATABASE}.Dimension_IM d1
  ON f.GDS_CD = d1.GDS_CD
GROUP BY d1.GDS_NM
ORDER BY total_stock_qty DESC
LIMIT 50
""".strip()
        return f"""
SELECT
  sum(f.StockQty) AS total_stock_qty
FROM {CLICKHOUSE_DATABASE}.war_stock_2024_MV f
LIMIT 50
""".strip()

    if Intent.is_product_query(query) and not Intent.is_sales(query):
        return f"""
SELECT
  f.GDS_CD AS product_code,
  f.GDS_NM AS product_name
FROM {CLICKHOUSE_DATABASE}.Dimension_IM f
LIMIT 50
""".strip()

    if Intent.is_store_query(query) and not Intent.is_sales(query):
        return f"""
SELECT
  f.StoreID AS store_id,
  f.StoreName AS store_name
FROM {CLICKHOUSE_DATABASE}.Dimension_LEM f
LIMIT 50
""".strip()

    if Intent.is_sales(query) and year:
        quarter = extract_quarter(query)
        if quarter:
            start_month = (quarter - 1) * 3 + 1
            end_month = start_month + 2
            return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
  AND toMonth(f.SalesDate) BETWEEN {start_month} AND {end_month}
LIMIT 50
""".strip()

        return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
LIMIT 50
""".strip()

    return None


async def text2sql_answer(query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
    result: Dict[str, Any]

    about_txt = hard_rule_table_about_text(query, registry)
    if about_txt:
        result = text_response(about_txt, "table_about")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    sales_tables_txt = hard_rule_sales_related_tables_text(query, registry)
    if sales_tables_txt:
        result = text_response(sales_tables_txt, "sales_related_tables")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    dataset_help = hard_rule_dataset_help_text(query)
    if dataset_help:
        result = text_response(dataset_help, "sales_dataset_help")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    for rule_name, rule_fn in HARD_SQL_RULES:
        sql = rule_fn(query)
        if sql:
            result = sql_response(sql, rule_name, run_sql_preview)
            persist_result(query=query, result=result, session_id=session_id)
            return result

    normalized_query = normalize_query(query)
    domain_info = classify_query_domain(query)
    domain = domain_info["domain"]

    candidates = registry.search(normalized_query, top_k=20) or registry.search(query, top_k=20)

    if not candidates:
        result = error_response("Schema олдсонгүй.", "schema_not_found")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    candidates = rerank_candidates(candidates, domain)
    rel_filtered = filter_relationships(candidates, registry.build_relationships())
    allowed_tables = build_allowed_tables(candidates)

    try:
        plan = await plan_with_llm(
            query=query,
            candidates=candidates,
            rel_filtered=rel_filtered,
            allowed_tables=allowed_tables,
            registry=registry,
        )
    except Exception as e:
        plan = None
        llm_error = str(e)

    if not plan:
        fallback_sql = fallback_sql_by_domain(query)
        if fallback_sql:
            result = sql_response(fallback_sql, "domain_fallback", run_sql_preview)
            persist_result(query=query, result=result, session_id=session_id)
            return result

        result = error_response(
            f"LLM planner алдаа: {llm_error if 'llm_error' in locals() else 'unknown'}",
            "planner_failed",
        )
        persist_result(query=query, result=result, session_id=session_id)
        return result

    plan = force_fact_table_by_domain(plan, query, domain, candidates)
    plan = repair_canonical_columns(plan)
    plan = drop_suspicious_joins(plan, query)
    plan = inject_name_join_from_registry(plan, candidates, rel_filtered, query)
    plan = ensure_product_name_join(plan, query)
    plan = repair_canonical_columns(plan)
    plan = validate_and_repair_plan(plan, candidates, allowed_tables, query)

    fallback_fact = f"{candidates[0].db}.{candidates[0].table}"
    built = build_sql_from_plan(plan, allowed_tables, fallback_fact, CLICKHOUSE_DATABASE)

    if built.get("error"):
        result = error_response(built["error"], built["error"])
        persist_result(query=query, result=result, session_id=session_id)
        return result

    sql = built["sql"]
    result = sql_response(sql, "llm_plan", run_sql_preview)
    persist_result(query=query, result=result, session_id=session_id)
    return result
