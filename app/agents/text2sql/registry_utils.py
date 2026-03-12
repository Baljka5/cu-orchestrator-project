# app/agents/text2sql/registry_utils.py
from typing import Any, Dict, List, Set

from app.config import SCHEMA_DICT_PATH, CLICKHOUSE_DATABASE
from app.core.schema_registry import SchemaRegistry, TableInfo

registry = SchemaRegistry(SCHEMA_DICT_PATH)
registry.load()


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

    allowed.update(
        {
            "Cluster_Main_Sales",
            f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales",
            "Dimension_IM",
            f"{CLICKHOUSE_DATABASE}.Dimension_IM",
        }
    )
    return allowed


def filter_relationships(
        candidates: List[TableInfo],
        all_relationships: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cand_tables = {t.table for t in candidates[:8]}
    cand_tables.add("Dimension_IM")

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
    return rel_filtered[:80]
