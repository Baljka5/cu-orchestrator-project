import re
from app.core.schemas import OrchestratorState, ClassificationResult

async def node_classify(state: OrchestratorState) -> OrchestratorState:

    # -------------------------------------------------

    # -------------------------------------------------
    if state.forced_agent:
        state.classification = ClassificationResult(
            agent=state.forced_agent,
            confidence=1.0,
            rationale="forced_from_ui"
        )
        state.meta["agent"] = state.forced_agent
        return state

    q = (state.normalized_message or state.raw_message or "").strip()
    q_low = q.lower()
    q_up = q.upper()

    # -------------------------------------------------

    # -------------------------------------------------
    data_keywords = [
        "борлуул", "sales", "netsale", "gross",
        "татвар", "discount", "өртөг",
        "тоо", "хэд", "тайлан", "дэлгүүр"
    ]

    if (
        any(k in q_low for k in data_keywords)
        or re.search(r"\bCU\d{3,4}\b", q_up)
    ):
        state.classification = ClassificationResult(
            agent="text2sql",
            confidence=0.9,
            rationale="rule_data_query"
        )
        state.meta["agent"] = "text2sql"
        return state

    # -------------------------------------------------

    # -------------------------------------------------
    if any(k in q_low for k in ["журам", "policy", "дотоод журам"]):
        state.classification = ClassificationResult(
            agent="policy",
            confidence=0.7,
            rationale="rule_policy"
        )
        state.meta["agent"] = "policy"
        return state

    # -------------------------------------------------

    # -------------------------------------------------
    if any(k in q_low for k in ["судалгаа", "research", "баримт"]):
        state.classification = ClassificationResult(
            agent="research",
            confidence=0.6,
            rationale="rule_research"
        )
        state.meta["agent"] = "research"
        return state

    # -------------------------------------------------
    # -------------------------------------------------
    state.classification = ClassificationResult(
        agent="general",
        confidence=0.3,
        rationale="fallback_general"
    )
    state.meta["agent"] = "general"
    return state
