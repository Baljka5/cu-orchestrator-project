# app/agents/text2sql/sql_builder.py
from typing import Any, Dict, List, Set

from app.agents.text2sql.registry_utils import normalize_table_ref, safe_table

ALLOWED_JOIN_TYPES = {"INNER", "LEFT", "RIGHT", "FULL", "CROSS"}
DEFAULT_LIMIT = 50
MAX_LIMIT = 500


def _ensure_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _clean_str_items(value: Any) -> List[str]:
    items = _ensure_list(value)
    result: List[str] = []
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def _normalize_limit(value: Any) -> int:
    try:
        limit = int(value or DEFAULT_LIMIT)
    except Exception:
        limit = DEFAULT_LIMIT
    return max(1, min(limit, MAX_LIMIT))


def _normalize_join_type(value: Any) -> str:
    join_type = str(value or "LEFT").strip().upper()
    return join_type if join_type in ALLOWED_JOIN_TYPES else "LEFT"


def build_select_clause(
        select_items: List[str],
        metrics: List[str],
        distinct: bool = False,
) -> str:
    """
    New planner structure:
    - select: non-aggregate dimensions / plain columns
    - metrics: aggregate expressions
    """
    parts: List[str] = []

    for item in select_items:
        text = str(item).strip()
        if text:
            parts.append(text)

    for metric in metrics:
        text = str(metric).strip()
        if text:
            parts.append(text)

    if not parts:
        base = "count() AS cnt"
    else:
        base = ", ".join(parts)

    if distinct:
        return f"DISTINCT {base}"
    return base


def build_sql_from_plan(
        plan: Dict[str, Any],
        allowed_tables: Set[str],
        fallback_fact: str,
        default_db: str,
) -> Dict[str, Any]:
    plan = plan or {}

    fact = normalize_table_ref(plan.get("fact_table") or fallback_fact, default_db)
    if not safe_table(fact, allowed_tables, default_db):
        return {"error": "Unsafe fact_table"}

    select_items = _clean_str_items(plan.get("select"))
    metrics = _clean_str_items(plan.get("metrics"))
    where_parts = _clean_str_items(plan.get("where"))
    group_parts = _clean_str_items(plan.get("group_by"))
    having_parts = _clean_str_items(plan.get("having"))
    order_parts = _clean_str_items(plan.get("order_by"))
    joins = _ensure_list(plan.get("joins"))
    distinct = bool(plan.get("distinct", False))
    limit = _normalize_limit(plan.get("limit"))

    sql_parts: List[str] = []
    sql_parts.append(
        f"SELECT {build_select_clause(select_items=select_items, metrics=metrics, distinct=distinct)}"
    )
    sql_parts.append(f"FROM {fact} f")

    used_aliases = {"f"}

    for idx, j in enumerate(joins, start=1):
        if not isinstance(j, dict):
            continue

        join_type = _normalize_join_type(j.get("type"))
        table_name = normalize_table_ref(j.get("table") or "", default_db)
        on = str(j.get("on") or "").strip()

        if not table_name or not on:
            continue
        if not safe_table(table_name, allowed_tables, default_db):
            continue

        alias = str(j.get("alias") or "").strip()
        if not alias:
            alias = f"d{idx}"

        if alias in used_aliases:
            alias = f"d{idx}"

        used_aliases.add(alias)

        sql_parts.append(f"{join_type} JOIN {table_name} {alias} ON {on}")

    if where_parts:
        sql_parts.append("WHERE " + " AND ".join(where_parts))

    if group_parts:
        sql_parts.append("GROUP BY " + ", ".join(group_parts))

    if having_parts:
        sql_parts.append("HAVING " + " AND ".join(having_parts))

    if order_parts:
        sql_parts.append("ORDER BY " + ", ".join(order_parts))

    sql_parts.append(f"LIMIT {limit}")

    sql = "\n".join(sql_parts)
    return {"sql": sql}
