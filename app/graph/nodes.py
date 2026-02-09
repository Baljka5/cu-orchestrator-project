import re
from app.core.schemas import OrchestratorState, ClassificationResult
from app.core.llm_client import chat_completion
from app.core.schema_catalog import format_schema_for_prompt
from app.agents.text2sql_agent import text2sql_execute


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
    result = await text2sql_execute(state.raw_message)

    sql = (result.get("sql") or "").strip()
    notes = (result.get("notes") or "").strip()
    row_count = int(result.get("row_count") or 0)

    if sql and row_count > 0:
        state.final_answer = f"Амжилттай. {row_count} мөрийн үр дүн олдлоо."
        if notes:
            state.final_answer += f" ({notes})"
    elif sql and row_count == 0:
        state.final_answer = "Query ажилласан ч үр дүн хоосон байна."
        if notes:
            state.final_answer += f" ({notes})"
    else:
        state.final_answer = "SQL үүссэнгүй."
        if notes:
            state.final_answer += f" ({notes})"

    # 2) Meta дээр query + data
    state.meta["mode"] = result.get("mode", "sql_result")
    state.meta["sql"] = sql
    state.meta["notes"] = notes
    state.meta["columns"] = result.get("columns", [])
    state.meta["rows"] = result.get("rows", [])
    state.meta["row_count"] = row_count

    return state
