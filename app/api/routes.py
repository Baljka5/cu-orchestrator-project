import logging
import re

from fastapi import APIRouter
from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph

# direct text2sql import
from app.agents.text2sql_agent import text2sql_answer

router = APIRouter()
log = logging.getLogger("cu-orchestrator")


def _norm_agent(a: str | None) -> str:
    """
    Normalize agent string to stable token:
    - "text2sql (sql)" -> "text2sql"
    - "Text2SQL" -> "text2sql"
    - "text2sql-agent" -> "text2sql"
    - "sql" -> "sql"
    - None -> ""
    """
    a = (a or "").strip().lower()
    if not a:
        return ""
    # take first token split by space/(),[],-,_,/
    return re.split(r"[\s\(\)\[\]\-_/]+", a)[0]


@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    # OPTIONAL DEBUG: see what UI actually sends
    log.info("REQ force_agent=%r normalized=%r message=%r",
             req.force_agent, _norm_agent(req.force_agent), (req.message or "")[:120])

    forced = _norm_agent(req.force_agent)

    # ✅ FAST PATH: if user forces text2sql, bypass graph and call real agent
    if forced in ("text2sql", "sql"):
        try:
            result = await text2sql_answer(req.message)

            meta = (result.get("meta") or {}) if isinstance(result, dict) else {}
            answer = (
                result.get("final_answer")
                or result.get("answer")
                or result.get("output")
                or result.get("response")
            ) if isinstance(result, dict) else str(result)

            if not answer:
                answer = f"Хариу үүсээгүй байна. meta={meta}"

            return ChatResponse(answer=answer, meta=meta)

        except Exception as e:
            log.exception("Direct text2sql failed")
            return ChatResponse(
                answer=f"Text2SQL алдаа: {e}",
                meta={"agent": "text2sql", "mode": "sql", "error": str(e)}
            )

    # -----------------------------
    # Original orchestrator flow
    # -----------------------------
    state = OrchestratorState(
        raw_message=req.message,
        forced_agent=req.force_agent
    )

    graph = build_graph()
    result = await graph.ainvoke(state)

    # DEBUG logs
    try:
        if isinstance(result, dict):
            log.info("GRAPH_RESULT_KEYS=%s", list(result.keys()))
            log.info("GRAPH_RESULT_META=%s", result.get("meta"))
        else:
            log.info("GRAPH_RESULT_TYPE=%s", type(result))
            log.info("GRAPH_RESULT_STR=%s", str(result)[:800])
    except Exception:
        log.exception("Failed to log graph result")

    answer = None
    meta = {}

    if isinstance(result, dict):
        meta = result.get("meta") or {}
        answer = (
            result.get("final_answer")
            or result.get("answer")
            or result.get("output")
            or result.get("response")
        )

    if not answer:
        answer = f"Хариу үүсээгүй байна. meta={meta}"

    return ChatResponse(answer=answer, meta=meta)