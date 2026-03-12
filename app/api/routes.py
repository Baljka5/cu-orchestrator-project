import logging
import re

from fastapi import APIRouter
from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph

from app.agents.text2sql_agent import text2sql_answer

router = APIRouter()
log = logging.getLogger("cu-orchestrator")


def _norm_agent(a: str | None) -> str:
    a = (a or "").strip().lower()
    if not a:
        return ""
    return re.split(r"[\s\(\)\[\]\-_/]+", a)[0]


@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    session_id = getattr(req, "session_id", None)

    log.info(
        "REQ force_agent=%r normalized=%r session_id=%r message=%r",
        req.force_agent,
        _norm_agent(req.force_agent),
        session_id,
        (req.message or "")[:120],
    )

    forced = _norm_agent(req.force_agent)

    if forced in ("text2sql", "sql"):
        try:
            result = await text2sql_answer(
                query=req.message,
                session_id=session_id,
            )

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
                meta={
                    "agent": "text2sql",
                    "mode": "sql",
                    "error": str(e),
                    "session_id": session_id,
                },
            )

    state = OrchestratorState(
        raw_message=req.message,
        forced_agent=req.force_agent,
        session_id=session_id,
    )

    graph = build_graph()
    result = await graph.ainvoke(state)

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
