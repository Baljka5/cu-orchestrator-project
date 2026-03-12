# app/agents/text2sql.py
from typing import Any, Dict, Optional

from app.agents.text2sql.executor import run_sql_preview
from app.agents.text2sql.hard_rules import (
    hard_rule_dataset_help_text,
    hard_rule_table_about_text,
    HARD_SQL_RULES,
)
from app.agents.planner import plan_with_llm
from app.agents.text2sql.postprocess import (
    force_fact_sales_table,
    inject_name_join_from_registry,
    ensure_product_name_join,
)
from app.agents.text2sql.registry_utils import (
    registry,
    filter_relationships,
    build_allowed_tables,
)
from app.agents.text2sql.sql_builder import build_sql_from_plan
from app.agents.text2sql.response import text_response, sql_response, error_response
from app.agents.text2sql.history import persist_result
from app.config import CLICKHOUSE_DATABASE


async def text2sql_answer(query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
    result: Dict[str, Any]

    about_txt = hard_rule_table_about_text(query, registry)
    if about_txt:
        result = text_response(about_txt, "table_about")
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

    candidates = registry.search(query, top_k=8)
    if not candidates:
        result = error_response("Schema олдсонгүй.", "schema_not_found")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    rel_filtered = filter_relationships(candidates, registry.build_relationships())
    allowed_tables = build_allowed_tables(candidates)

    plan = await plan_with_llm(
        query=query,
        candidates=candidates,
        rel_filtered=rel_filtered,
        allowed_tables=allowed_tables,
        registry=registry,
    )
    if not plan:
        result = error_response("SQL plan parse хийж чадсангүй.", "json_parse")
        persist_result(query=query, result=result, session_id=session_id)
        return result

    plan = force_fact_sales_table(plan, query)
    plan = inject_name_join_from_registry(plan, candidates, rel_filtered, query)
    plan = ensure_product_name_join(plan, query)

    fallback_fact = f"{candidates[0].db}.{candidates[0].table}"
    built = build_sql_from_plan(plan, allowed_tables, fallback_fact, CLICKHOUSE_DATABASE)
    if built.get("error"):
        result = error_response(built["error"], built["error"])
        persist_result(query=query, result=result, session_id=session_id)
        return result

    sql = built["sql"]
    data = run_sql_preview(sql, max_rows=50)

    meta = {
        "agent": "text2sql",
        "mode": "sql",
        "rule": "llm_plan",
        "data": data,
        "plan": plan,
    }
    if data.get("error"):
        meta["error"] = data["error"]

    result = {
        "answer": sql,
        "meta": meta,
    }
    persist_result(query=query, result=result, session_id=session_id)
    return result