from typing import Any, Dict, List
import clickhouse_connect

from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
)


SYSTEM_DATABASES = {"system", "information_schema", "INFORMATION_SCHEMA"}


def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def list_tables(databases: List[str] | None = None) -> List[Dict[str, Any]]:
    client = ch_client()

    if databases:
        db_list = ", ".join([f"'{db}'" for db in databases])
        sql = f"""
        SELECT
            database,
            name AS table_name,
            engine,
            comment
        FROM system.tables
        WHERE database IN ({db_list})
        ORDER BY database, table_name
        """
    else:
        sql = """
        SELECT
            database,
            name AS table_name,
            engine,
            comment
        FROM system.tables
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY database, table_name
        """

    result = client.query(sql)
    rows = []
    for row in result.result_rows:
        rows.append(
            {
                "database": row[0],
                "table": row[1],
                "engine": row[2],
                "comment": row[3],
            }
        )
    return rows


def list_columns(databases: List[str] | None = None) -> List[Dict[str, Any]]:
    client = ch_client()

    if databases:
        db_list = ", ".join([f"'{db}'" for db in databases])
        sql = f"""
        SELECT
            database,
            table,
            name AS column_name,
            type,
            default_kind,
            default_expression,
            comment
        FROM system.columns
        WHERE database IN ({db_list})
        ORDER BY database, table, position
        """
    else:
        sql = """
        SELECT
            database,
            table,
            name AS column_name,
            type,
            default_kind,
            default_expression,
            comment
        FROM system.columns
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY database, table, position
        """

    result = client.query(sql)
    rows = []
    for row in result.result_rows:
        rows.append(
            {
                "database": row[0],
                "table": row[1],
                "name": row[2],
                "type": row[3],
                "default_kind": row[4],
                "default_expression": row[5],
                "comment": row[6],
            }
        )
    return rows


def infer_column_semantics(column_name: str, column_type: str) -> List[str]:
    n = (column_name or "").lower()
    t = (column_type or "").lower()

    tags: List[str] = []

    if any(x in n for x in ["date", "dt", "time", "created_at", "updated_at", "salesdate"]):
        tags.append("date")

    if any(x in n for x in ["name", "nm", "desc"]):
        tags.append("name")

    if any(x in n for x in ["id", "code", "cd", "no"]):
        tags.append("key")

    if any(x in n for x in ["sale", "amount", "amt", "price", "cost", "tax", "discount"]):
        tags.append("metric")

    if any(x in n for x in ["qty", "cnt", "count", "quantity"]):
        tags.append("quantity")

    if "date" in t or "datetime" in t:
        if "date" not in tags:
            tags.append("date")

    return tags


def infer_table_entity(table_name: str) -> str:
    t = (table_name or "").lower()

    if "sales" in t:
        return "sales_fact"
    if "dimension" in t and ("im" in t or "product" in t or "item" in t):
        return "product_dimension"
    if "dimension" in t and ("store" in t or "lem" in t or "leg" in t):
        return "store_dimension"
    if "stock" in t or "inventory" in t:
        return "inventory_fact"
    if "promo" in t or "campaign" in t:
        return "promotion_fact"
    return "table"


def build_schema_catalog(databases: List[str] | None = None) -> Dict[str, Any]:
    tables = list_tables(databases=databases)
    columns = list_columns(databases=databases)

    grouped_cols: Dict[str, List[Dict[str, Any]]] = {}
    for col in columns:
        key = f"{col['database']}.{col['table']}"
        grouped_cols.setdefault(key, []).append(col)

    catalog_tables = []
    for tbl in tables:
        key = f"{tbl['database']}.{tbl['table']}"
        cols = grouped_cols.get(key, [])

        catalog_tables.append(
            {
                "db": tbl["database"],
                "table": tbl["table"],
                "entity": infer_table_entity(tbl["table"]),
                "description": tbl.get("comment") or "",
                "engine": tbl.get("engine") or "",
                "columns": [
                    {
                        "name": c["name"],
                        "type": c["type"],
                        "description": c.get("comment") or "",
                        "semantic": infer_column_semantics(c["name"], c["type"]),
                    }
                    for c in cols
                ],
            }
        )

    return {
        "tables": catalog_tables,
        "relationships": [],
    }