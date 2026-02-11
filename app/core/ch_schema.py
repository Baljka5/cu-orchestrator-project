import os
import httpx

CH_HOST = os.getenv("CH_HOST", "")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "")

SCHEMA_MAX_TABLES = int(os.getenv("SCHEMA_MAX_TABLES", "40"))
SCHEMA_MAX_COLS_PER_TABLE = int(os.getenv("SCHEMA_MAX_COLS_PER_TABLE", "40"))

def _ch_url() -> str:
    return f"http://{CH_HOST}:{CH_PORT}"

def _auth():
    if CH_USER:
        return (CH_USER, CH_PASSWORD)
    return None

async def fetch_schema_markdown() -> str:
    if not CH_HOST or not CH_DATABASE:
        return "ClickHouse schema not configured."

    sql_tables = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{CH_DATABASE}'
      AND engine NOT IN ('View','MaterializedView')
    ORDER BY name
    LIMIT {SCHEMA_MAX_TABLES}
    FORMAT TabSeparated
    """

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(_ch_url(), content=sql_tables, auth=_auth())
        r.raise_for_status()
        tables = [t.strip() for t in r.text.splitlines() if t.strip()]

        parts = [f"# ClickHouse schema ({CH_DATABASE})", ""]
        for t in tables:
            sql_cols = f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{CH_DATABASE}' AND table = '{t}'
            ORDER BY position
            LIMIT {SCHEMA_MAX_COLS_PER_TABLE}
            FORMAT TabSeparated
            """
            rc = await client.post(_ch_url(), content=sql_cols, auth=_auth())
            rc.raise_for_status()
            rows = [x.split("\t") for x in rc.text.splitlines() if x.strip()]
            parts.append(f"## {t}")
            for name, typ in rows:
                parts.append(f"- {name}: {typ}")
            parts.append("")

        return "\n".join(parts)
