from fastapi import APIRouter
from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph

router = APIRouter()
graph = build_graph()

@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    state = OrchestratorState(raw_message=req.message)

    result = await graph.ainvoke(state)

    final_answer = result.get("final_answer") or result.get("agent_result") or "Алдаа гарлаа."
    meta = result.get("meta") or {}

    return ChatResponse(answer=final_answer, meta=meta)
