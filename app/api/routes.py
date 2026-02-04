from fastapi import APIRouter
from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph

router = APIRouter()
graph = build_graph()

@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    state = OrchestratorState(raw_message=req.message)
    result = await graph.ainvoke(state)
    return ChatResponse(answer=result.final_answer, meta=result.meta)
