from typing import Any, Dict, Optional
import re
import time

import clickhouse_connect

from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)


# ======================================================
# ClickHouse client
# ======================================================

def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


# ======================================================
# SQL safety / normalization
# ======================================================

def normalize_sql(sql: str) -> str:
    if not sql:
        return sql

    s = sql.strip().rstrip(";")

    # CURRENT_DATE -> today()
    s = re.sub(r"\bCURRENT_DATE\b", "today()", s, flags=re.IGNORECASE)

    # NOW() normalization (ClickHouse ok, but ensure format)
    s = re.sub(r"\bNOW\(\)\b", "now()", s, flags=re.IGNORECASE)

    # Remove dangerous statements
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
    if any(f in s.upper() for f in forbidden):
        raise ValueError("Only SELECT queries are allowed.")

    return s


def ensure_limit(sql: str, max_rows: int = 50) -> str:
    s = sql.strip()

    if re.search(r"\blimit\b", s, re.IGNORECASE):
        return s

    return f"{s}\nLIMIT {max_rows}"


def fix_common_errors(sql: str, error: str) -> Optional[str]:
    """
    Attempt simple auto-fixes based on ClickHouse errors
    """

    if not sql or not error:
        return None

    # UNKNOWN_IDENTIFIER → remove bad column
    if "UNKNOWN_IDENTIFIER" in error:
        # remove suspicious tokens like f.StoreIDIDID
        sql = re.sub(r"f\.[A-Za-z0-9_]{20,}", "", sql)
        return sql

    # CURRENT_DATE error
    if "CURRENT_DATE" in error:
        return sql.replace("CURRENT_DATE", "today()")

    # ym not defined → remove ORDER BY ym
    if "ym" in error and "ORDER BY ym" in sql:
        sql = re.sub(r"ORDER BY\s+ym\s*(ASC|DESC)?", "", sql, flags=re.IGNORECASE)
        return sql

    return None


# ======================================================
# Query execution
# ======================================================

def run_query(sql: str) -> Dict[str, Any]:
    client = ch_client()
    result = client.query(sql)

    return {
        "columns": result.column_names or [],
        "rows": result.result_rows or [],
    }


# ======================================================
# Main preview executor
# ======================================================

def run_sql_preview(sql: str, max_rows: int = 50) -> Dict[str, Any]:
    """
    Execute SQL safely with:
    - normalization
    - limit enforcement
    - auto-fix retry
    """

    if not sql:
        return {
            "columns": [],
            "rows": [],
            "error": "empty_sql",
            "executed_sql": "",
        }

    try:
        safe_sql = normalize_sql(sql)
        safe_sql = ensure_limit(safe_sql, max_rows)

    except Exception as e:
        return {
            "columns": [],
            "rows": [],
            "error": str(e),
            "executed_sql": sql,
        }

    # First attempt
    try:
        data = run_query(safe_sql)

        return {
            "columns": data["columns"],
            "rows": data["rows"][:max_rows],
            "executed_sql": safe_sql,
        }

    except Exception as e:
        error_msg = str(e)

        # Try auto-fix
        fixed_sql = fix_common_errors(safe_sql, error_msg)

        if fixed_sql and fixed_sql != safe_sql:
            try:
                fixed_sql = ensure_limit(fixed_sql, max_rows)
                data = run_query(fixed_sql)

                return {
                    "columns": data["columns"],
                    "rows": data["rows"][:max_rows],
                    "executed_sql": fixed_sql,
                    "auto_fixed": True,
                    "original_error": error_msg,
                }

            except Exception as e2:
                return {
                    "columns": [],
                    "rows": [],
                    "error": str(e2),
                    "executed_sql": fixed_sql,
                    "original_error": error_msg,
                }

        return {
            "columns": [],
            "rows": [],
            "error": error_msg,
            "executed_sql": safe_sql,
        }
