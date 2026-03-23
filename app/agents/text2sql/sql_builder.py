# app/agents/text2sql/sql_builder.py
from typing import Any, Dict, List, Set

from app.agents.text2sql.registry_utils import normalize_table_ref, safe_table


def build_select_clause(select_items: List[Dict[str, Any]]) -> str:
    parts = []
    for item in select_items:
        if not isinstance(item, dict):
            continue

        expr = (item.get("expr") or "").strip()
        alias = (item.get("as") or "").strip()

        if not expr:
            continue

        if alias:
            parts.append(f"{expr} AS {alias}")
        else:
            parts.append(expr)

    return ", ".join(parts) if parts else "count() AS cnt"


def build_sql_from_plan(
    plan: Dict[str, Any],
    allowed_tables: Set[str],
    fallback_fact: str,
    default_db: str,
) -> Dict[str, Any]:
    fact = normalize_table_ref(plan.get("fact_table") or fallback_fact, default_db)
    if not safe_table(fact, allowed_tables, default_db):
        return {"error": "Unsafe fact_table"}

    sql = f"SELECT {build_select_clause(plan.get('select', []))}\nFROM {fact} f"

    for j in plan.get("joins", []):
        if not isinstance(j, dict):
            continue

        join_type = (j.get("type") or "LEFT").upper()
        table_name = normalize_table_ref(j.get("table") or "", default_db)
        alias = (j.get("alias") or "").strip() or "d1"
        on = (j.get("on") or "").strip()

        if not table_name or not on:
            continue
        if not safe_table(table_name, allowed_tables, default_db):
            continue

        sql += f"\n{join_type} JOIN {table_name} {alias} ON {on}"

    where_parts = [x.strip() for x in plan.get("where", []) if isinstance(x, str) and x.strip()]
    if where_parts:
        sql += "\nWHERE " + " AND ".join(where_parts)

    group_parts = [x.strip() for x in plan.get("group_by", []) if isinstance(x, str) and x.strip()]
    if group_parts:
        sql += "\nGROUP BY " + ", ".join(group_parts)

    order_parts = [x.strip() for x in plan.get("order_by", []) if isinstance(x, str) and x.strip()]
    if order_parts:
        sql += "\nORDER BY " + ", ".join(order_parts)

    try:
        limit = int(plan.get("limit") or 50)
    except Exception:
        limit = 50

    limit = max(1, min(limit, 500))
    sql += f"\nLIMIT {limit}"

    return {"sql": sql}