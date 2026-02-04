import re
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.config import SQLITE_PATH

_engine = create_async_engine(f"sqlite+aiosqlite:///{SQLITE_PATH}", echo=False)

async def _init_db():
    async with _engine.begin() as conn:
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sales(
            store_code TEXT,
            day TEXT,
            net_sales REAL
        )
        """))
        await conn.execute(text("""
        INSERT INTO sales(store_code, day, net_sales)
        SELECT 'CU520','2026-02-01', 123456.0
        WHERE NOT EXISTS(SELECT 1 FROM sales WHERE store_code='CU520' AND day='2026-02-01')
        """))

def _extract_store(query: str) -> str | None:
    m = re.search(r"(CU\d{3,4})", query.upper())
    return m.group(1) if m else None

async def text2sql_answer(query: str) -> str:
    await _init_db()
    store = _extract_store(query)
    if not store:
        return "Text2SQL: CU код (ж: CU520) олдсонгүй. Асуултад салбарын код оруулаарай."

    sql = "SELECT store_code, day, net_sales FROM sales WHERE store_code = :store ORDER BY day DESC LIMIT 10"
    async with _engine.connect() as conn:
        rows = (await conn.execute(text(sql), {"store": store})).fetchall()

    if not rows:
        return f"Text2SQL: {store} дээр өгөгдөл олдсонгүй (demo sqlite)."

    lines = ["store_code | day | net_sales"]
    for r in rows:
        lines.append(f"{r[0]} | {r[1]} | {r[2]}")
    return "Text2SQL (demo sqlite) үр дүн:\n" + "\n".join(lines)
