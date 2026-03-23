from typing import Any, Dict, Optional

from app.agents.text2sql.executor import run_sql_preview
from app.agents.text2sql.hard_rules import (
    hard_rule_dataset_help_text,
    hard_rule_inventory_dataset_help_text,
    hard_rule_table_about_text,
    hard_rule_sales_related_tables_text,
    hard_rule_out_of_domain_text,
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
    rerank_candidates,
)
from app.agents.text2sql.sql_builder import build_sql_from_plan
from app.agents.text2sql.response import text_response, sql_response, error_response
from app.agents.text2sql.history import persist_result
from app.agents.text2sql.intents import (
    normalize_query,
    Intent,
    extract_year,
    extract_quarter,
)
from app.agents.text2sql.validator import validate_and_repair_plan
from app.config import CLICKHOUSE_DATABASE
from app.agents.text2sql.query_router import classify_query_domain


# =========================================================
# Helpers
# =========================================================

def is_empty_plan(plan: Dict[str, Any]) -> bool:
    if not isinstance(plan, dict):
        return True

    return (
            not (plan.get("fact_table") or "").strip()
            and not (plan.get("select") or [])
            and not (plan.get("joins") or [])
            and not (plan.get("where") or [])
            and not (plan.get("group_by") or [])
            and not (plan.get("order_by") or [])
            and int(plan.get("limit") or 0) == 0
    )


def fallback_sql_by_domain(query: str) -> Optional[str]:
    year = extract_year(query)

    # ---------------- inventory ----------------
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

    # ---------------- product master ----------------
    if Intent.is_product_query(query) and not Intent.is_sales(query):
        if Intent.wants_name(query):
            return f"""
SELECT
  f.GDS_CD AS product_code,
  f.GDS_NM AS product_name
FROM {CLICKHOUSE_DATABASE}.Dimension_IM f
LIMIT 50
""".strip()

        return f"""
SELECT
  f.GDS_CD AS product_code,
  f.GDS_NM AS product_name
FROM {CLICKHOUSE_DATABASE}.Dimension_IM f
LIMIT 50
""".strip()

    # ---------------- store master ----------------
    if Intent.is_store_query(query) and not Intent.is_sales(query):
        return f"""
SELECT
  f.BIZLOC_CD AS store_id,
  f.BIZLOC_NM AS store_name
FROM {CLICKHOUSE_DATABASE}.Dimension_SM f
LIMIT 50
""".strip()

    # ---------------- sales ----------------
    if Intent.is_sales(query):
        # yearly / quarterly
        if year:
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

            # top/bottom store
            ql = normalize_query(query)
            if Intent.is_top_store(query):
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

            if Intent.is_bottom_store(query):
                return f"""
SELECT
  f.StoreID AS store_id,
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
GROUP BY f.StoreID
ORDER BY total_net_sales ASC
LIMIT 10
""".strip()

            # monthly trend
            if Intent.is_monthly(query):
                return f"""
SELECT
  toYYYYMM(f.SalesDate) AS ym,
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
GROUP BY ym
ORDER BY ym ASC
LIMIT 50
""".strip()

            # total sales
            return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
WHERE toYear(f.SalesDate) = {year}
LIMIT 50
""".strip()

        # no year but sales question
        return f"""
SELECT
  sum(f.NetSale) AS total_net_sales
FROM {CLICKHOUSE_DATABASE}.Cluster_Main_Sales f
LIMIT 50
""".strip()

    return None


# =========================================================
# Main entry
# =========================================================

async def text2sql_answer(query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
    result: Dict[str, Any]
    normalized_query = normalize_query(query)

    # -----------------------------------------------------
    # 0) Out-of-domain text
    # -----------------------------------------------------
    out_of_domain_txt = hard_rule_out_of_domain_text(query)
    if out_of_domain_txt:
        result = text_response(out_of_domain_txt, "out_of_domain")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    # -----------------------------------------------------
    # 1) Help / schema text rules
    # -----------------------------------------------------
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

    inventory_help = hard_rule_inventory_dataset_help_text(query)
    if inventory_help:
        result = text_response(inventory_help, "inventory_dataset_help")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    # -----------------------------------------------------
    # 2) Hard SQL rules
    # -----------------------------------------------------
    for rule_name, rule_fn in HARD_SQL_RULES:
        try:
            sql = rule_fn(query)
        except Exception:
            sql = None

        if sql:
            result = sql_response(sql, rule_name, run_sql_preview)
            persist_result(query=query, result=result, session_id=session_id)
            return result

    # -----------------------------------------------------
    # 3) Domain & candidate discovery
    # -----------------------------------------------------
    domain_info = classify_query_domain(query)
    domain = domain_info.get("domain", "unknown")

    candidates = registry.search(normalized_query, top_k=20) or registry.search(query, top_k=20)

    if not candidates:
        fallback_sql = fallback_sql_by_domain(query)
        if fallback_sql:
            result = sql_response(fallback_sql, "fallback_no_candidates", run_sql_preview)
            persist_result(query=query, result=result, session_id=session_id)
            return result

        result = text_response(
            "Таны асуултад тохирох table эсвэл schema олдсонгүй. "
            "Борлуулалт, салбар, бараа, үлдэгдэлтэй холбоотой асуугаарай.",
            "schema_not_found_text",
        )
        persist_result(query=query, result=result, session_id=session_id)
        return result

    candidates = rerank_candidates(candidates, domain)
    rel_filtered = filter_relationships(candidates, registry.build_relationships())
    allowed_tables = build_allowed_tables(candidates)

    # -----------------------------------------------------
    # 4) Planner
    # -----------------------------------------------------
    llm_error: Optional[str] = None
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

    # Planner decided unrelated / empty
    if plan and is_empty_plan(plan):
        result = text_response(
            "Энэ асуулт нь sales / store / product / inventory analytics төрлийн асуулт биш байна. "
            "Борлуулалт, дэлгүүр, бүтээгдэхүүн, үлдэгдэлтэй холбоотой асуулт асууна уу.",
            "planner_out_of_domain",
        )
        persist_result(query=query, result=result, session_id=session_id)
        return result

    # Planner failed -> fallback
    if not plan:
        fallback_sql = fallback_sql_by_domain(query)
        if fallback_sql:
            result = sql_response(fallback_sql, "domain_fallback", run_sql_preview)
            persist_result(query=query, result=result, session_id=session_id)
            return result

        result = text_response(
            "Таны асуултыг SQL болгон найдвартай хөрвүүлж чадсангүй. "
            "Жишээ нь: '2024 оны нийт борлуулалт', "
            "'2025 онд хамгийн их борлуулалттай 10 дэлгүүр' гэж асуугаарай.",
            "planner_failed_text",
        )
        if llm_error:
            result["meta"]["planner_error"] = llm_error
        persist_result(query=query, result=result, session_id=session_id)
        return result

    # -----------------------------------------------------
    # 5) Post-process plan
    # -----------------------------------------------------
    plan = force_fact_table_by_domain(plan, query, domain, candidates)
    plan = repair_canonical_columns(plan)
    plan = drop_suspicious_joins(plan, query)
    plan = inject_name_join_from_registry(plan, candidates, rel_filtered, query)
    plan = ensure_product_name_join(plan, query)
    plan = repair_canonical_columns(plan)
    plan = validate_and_repair_plan(plan, candidates, allowed_tables, query)

    # -----------------------------------------------------
    # 6) Build SQL
    # -----------------------------------------------------
    fallback_fact = f"{candidates[0].db}.{candidates[0].table}"
    built = build_sql_from_plan(
        plan=plan,
        allowed_tables=allowed_tables,
        fallback_fact=fallback_fact,
        default_db=CLICKHOUSE_DATABASE,
    )

    if built.get("error"):
        fallback_sql = fallback_sql_by_domain(query)
        if fallback_sql:
            result = sql_response(fallback_sql, "build_sql_fallback", run_sql_preview)
            persist_result(query=query, result=result, session_id=session_id)
            return result

        result = error_response(built["error"], built["error"])
        persist_result(query=query, result=result, session_id=session_id)
        return result

    sql = built["sql"]

    # -----------------------------------------------------
    # 7) Execute preview
    # -----------------------------------------------------
    result = sql_response(sql, "llm_plan", run_sql_preview)
    persist_result(query=query, result=result, session_id=session_id)
    return result
