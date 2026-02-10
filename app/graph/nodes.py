# app/graph/nodes.py
from app.core.schema_registry import SchemaRegistry
from app.config import SCHEMA_DICT_PATH
from app.core.schema_catalog import format_schema_for_prompt  # (Хэрвээ ашиглахгүй бол хасаж болно)
from app.core.llm_client import chat_completion

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()


def _build_schema_text_from_candidates(candidates, max_tables=5, max_cols=60) -> str:
    parts = []
    for t in candidates[:max_tables]:
        cols = "\n".join([f"- {c.name} ({c.dtype}): {c.attr}" for c in t.columns[:max_cols]])
        parts.append(
            f"DB: {t.db}\n"
            f"TABLE: {t.table}\n"
            f"ENTITY: {t.entity}\n"
            f"DESC: {t.description}\n"
            f"COLUMNS:\n{cols}\n"
        )
    return "\n".join(parts).strip()


async def node_run_text2sql(state):
    q = (state.normalized_message or state.raw_message or "").strip()

    candidates = _registry.search(q, top_k=8)
    if not candidates:
        state.final_answer = "SELECT 'UNKNOWN_SCHEMA' AS error;"
        state.meta["mode"] = "sql"
        return state

    schema_txt = _build_schema_text_from_candidates(candidates, max_tables=5, max_cols=60)

    system = f"""
Та ClickHouse SQL бичдэг туслах.
Зөвхөн ClickHouse SELECT query буцаа (тайлбаргүй, markdown биш).
Хэрэв шаардлагатай column/table schema-д байхгүй бол:
SELECT 'UNKNOWN_SCHEMA' AS error;

Доорх schema-г л ашигла:

{schema_txt}

Дүрэм:
- Огноо фильтерт SalesDate эсвэл schema дээр байгаа date баганыг ашигла.
- Жилийн дүн гэвэл toYear(date_col)=YYYY эсвэл date range ашигла.
- Борлуулалтын дүн гэвэл NetSale/NetSales аль тохирохыг schema-аас сонго.
- LIMIT шаардлагагүй бол бүү нэм.
""".strip()

    sql = await chat_completion(q, system=system)
    state.final_answer = sql
    state.meta["mode"] = "sql"
    return state
