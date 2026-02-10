import re
from app.core.schemas import OrchestratorState, ClassificationResult
from app.core.llm_client import chat_completion
from app.core.schema_catalog import format_schema_for_prompt


async def node_classify(state: OrchestratorState) -> OrchestratorState:
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
        "тоо", "хэд", "тайлан", "дэлгүүр", "2025", "2024", "2023"
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
    return state


async def node_run_text2sql(state: OrchestratorState) -> OrchestratorState:
    schema_txt = format_schema_for_prompt(["Cluster_Main_Sales"])

    system = f"""
Та ClickHouse SQL бичдэг туслах.
Зөвхөн ClickHouse SQL буцаа (markdown биш, тайлбаргүй).
Хэрэв асуултад шаардлагатай багана/table schema-д байхгүй бол:
SELECT 'UNKNOWN_SCHEMA' AS error;

Доорх schema-г ашигла:

{schema_txt}

Дүрэм:
- Огноо фильтерт SalesDate-г ашигла.
- Жилийн дүн гэвэл toYear(SalesDate)=YYYY.
- Борлуулалтын дүн гэвэл sum(NetSale) гэж ойлго.
- Дэлгүүрээр гэвэл StoreID group by.
- limit-ийг шаардлагагүй бол бүү нэм.
""".strip()

    sql = await chat_completion(state.raw_message, system=system)
    state.final_answer = sql
    state.meta["mode"] = "sql"
    return state
