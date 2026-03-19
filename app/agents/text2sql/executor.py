from typing import Any, Dict
import clickhouse_connect

from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)

def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

def ensure_preview_sql(sql: str, max_rows: int = 50) -> str:
    sql_clean = sql.strip().rstrip(";")
    if "limit" not in sql_clean.lower():
        sql_clean += f"\nLIMIT {max_rows}"
    return sql_clean

def run_sql_preview(sql: str, max_rows: int = 50) -> Dict[str, Any]:
    try:
        client = ch_client()
        safe_sql = ensure_preview_sql(sql, max_rows=max_rows)
        result = client.query(safe_sql)
        return {
            "columns": result.column_names or [],
            "rows": (result.result_rows or [])[:max_rows],
            "executed_sql": safe_sql,
        }
    except Exception as e:
        return {
            "columns": [],
            "rows": [],
            "error": str(e),
            "executed_sql": sql,
        }