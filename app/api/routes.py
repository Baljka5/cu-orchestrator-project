import logging

from fastapi import APIRouter
from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph

router = APIRouter()
log = logging.getLogger("cu-orchestrator")


@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
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
