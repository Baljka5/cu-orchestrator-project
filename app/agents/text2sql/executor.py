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


def run_sql_preview(sql: str, max_rows: int = 50) -> Dict[str, Any]:
    try:
        client = ch_client()
        result = client.query(sql)
        return {
            "columns": result.column_names or [],
            "rows": (result.result_rows or [])[:max_rows],
        }
    except Exception as e:
        return {
            "columns": [],
            "rows": [],
            "error": str(e),
        }