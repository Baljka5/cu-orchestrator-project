# app/graph/nodes.py
import re

from app.core.schemas import OrchestratorState, ClassificationResult
from app.core.schema_registry import SchemaRegistry
from app.config import SCHEMA_DICT_PATH
from app.core.llm_client import chat_completion

# Dictionary schema registry (xlsx)
_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()


def _build_schema_text_from_candidates(candidates, max_tables: int = 5, max_cols: int = 60) -> str:
    parts = []
    for t in candidates[:max_tables]:
        cols = "\n".join(
            [f"- {c.name} ({c.dtype}): {c.attr}" for c in t.columns[:max_cols]]
        )
        parts.append(
            f"DB: {t.db}\n"
            f"TABLE: {t.table}\n"
            f"ENTITY: {t.entity}\n"
            f"DESC: {t.description}\n"
            f"COLUMNS:\n{cols}\n"
        )
    return "\n".join(parts).strip()


async def node_classify(state: OrchestratorState) -> OrchestratorState:
    """
    Simple rule-based classifier:
    - forced_agent байвал шууд тэрийг ашиглана
    - sales / store / date keywords байвал text2sql
    - бусад үед general
    """
    if state.forced_agent:
        state.classification = ClassificationResult(
            agent=state.forced_agent, confidence=1.0, rationale="forced_from_ui"
        )
        state.meta["agent"] = state.forced_agent
        return state

    q = (state.normalized_message or state.raw_message or "").strip()
    q_low = q.lower()
    q_up = q.upper()

    data_keywords = [
        "борлуул", "sales", "netsale", "gross", "татвар", "discount",
        "тоо", "хэд", "тайлан", "дэлгүүр", "store", "2025", "2024", "2023",
        "item", "product", "sku", "promo", "promotion"
    ]

    if any(k in q_low for k in data_keywords) or re.search(r"\bCU\d{3,4}\b", q_up):
        state.classification = ClassificationResult(
            agent="text2sql", confidence=0.9, rationale="rule_data_query"
        )
        state.meta["agent"] = "text2sql"
        return state

    state.classification = ClassificationResult(
        agent="general", confidence=0.3, rationale="fallback_general"
    )
    state.meta["agent"] = "general"
    return state


async def node_run_llm_general(state: OrchestratorState) -> OrchestratorState:
    system = "Та бол CU Orchestrator assistant. Хэрэглэгчийн асуултад товч, тодорхой хариул."
    answer = await chat_completion(state.raw_message, system=system)
    state.final_answer = answer
    state.meta["mode"] = "text"
    return state


async def node_run_text2sql(state: OrchestratorState) -> OrchestratorState:
    """
    LLM generates SQL only, with dynamic schema from dictionary.
    (Хэрвээ чи SQL+DATA буцаадаг болгохыг хүсвэл дараа нь энэ node дээр
     text2sql_answer() дууддаг болгож болно.)
    """
    q = (state.normalized_message or state.raw_message or "").strip()

    candidates = _registry.search(q, top_k=8)
    if not candidates:
        state.final_answer = "SELECT 'UNKNOWN_SCHEMA' AS error;"
        state.meta["agent"] = "text2sql"
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
- Огноо фильтерт schema дээрх date баганыг ашигла (ихэвчлэн SalesDate).
- Жилийн дүн гэвэл toYear(date_col)=YYYY эсвэл date range ашигла.
- Борлуулалтын дүн гэвэл NetSale/NetSales аль тохирохыг schema-аас сонго.
- Хэрвээ асуулт тодорхойгүй бол хамгийн боломжит table дээр:
  SELECT * FROM <table> LIMIT 20;
""".strip()

    sql = await chat_completion(q, system=system)

    state.final_answer = sql
    state.meta["agent"] = "text2sql"
    state.meta["mode"] = "sql"
    return state
