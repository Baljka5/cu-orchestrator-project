from typing import Any, Dict, List, Set

from app.config import SCHEMA_DICT_PATH, CLICKHOUSE_DATABASE
from app.core.schema_registry import SchemaRegistry, TableInfo

registry = SchemaRegistry(SCHEMA_DICT_PATH)
registry.load()

CORE_TABLES = {
    "Cluster_Main_Sales",
    "Dimension_IM",
    "Dimension_LEM",
    "Dimension_LEG",
    "agg_sales_2024",
    "war_stock_2024_MV",
}


def normalize_table_ref(table_name: str, default_db: str = CLICKHOUSE_DATABASE) -> str:
    t = (table_name or "").strip()
    if not t:
        return t
    if "." not in t:
        return f"{default_db}.{t}"
    return t


def safe_table(table_name: str, allowed: Set[str], default_db: str = CLICKHOUSE_DATABASE) -> bool:
    t = normalize_table_ref(table_name, default_db)
    base = t.split(".", 1)[-1]
    return t in allowed or base in allowed


def build_allowed_tables(candidates: List[TableInfo]) -> Set[str]:
    allowed: Set[str] = set()

    for t in candidates:
        allowed.add(t.table)
        allowed.add(f"{t.db}.{t.table}")

    for tbl in CORE_TABLES:
        allowed.add(tbl)
        allowed.add(f"{CLICKHOUSE_DATABASE}.{tbl}")

    tables_obj = getattr(registry, "tables", {})
    values = tables_obj.values() if isinstance(tables_obj, dict) else []

    for t in values:
        table_name = getattr(t, "table", "")
        db_name = getattr(t, "db", CLICKHOUSE_DATABASE)
        desc = (getattr(t, "description", "") or "").lower()
        entity = (getattr(t, "entity", "") or "").lower()

        if any(k in desc for k in ["sales", "store", "product", "item", "branch", "inventory"]):
            allowed.add(table_name)
            allowed.add(f"{db_name}.{table_name}")
        elif any(k in entity for k in ["sales", "store", "product", "inventory"]):
            allowed.add(table_name)
            allowed.add(f"{db_name}.{table_name}")

    return allowed


def filter_relationships(
    candidates: List[TableInfo],
    all_relationships: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cand_tables = {t.table for t in candidates[:12]}
    cand_tables.add("Dimension_IM")
    cand_tables.add("Cluster_Main_Sales")

    rel_filtered: List[Dict[str, Any]] = []

    for rel in all_relationships:
        rel_type = rel.get("type")

        if rel_type == "join_key":
            lt = rel["left"].split(".", 1)[0]
            rt = rel["right"].split(".", 1)[0]
            if lt in cand_tables and rt in cand_tables:
                rel_filtered.append(rel)

        elif rel_type == "name_column":
            if rel.get("table") in cand_tables:
                rel_filtered.append(rel)

    rel_filtered.sort(key=lambda x: x.get("score", 0), reverse=True)
    return rel_filtered[:100]