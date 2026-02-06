from fastapi import APIRouter
from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph

router = APIRouter()
_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    state = OrchestratorState(
        raw_message=req.message,
        forced_agent=req.force_agent
    )

    graph = build_graph()

    if hasattr(graph, "ainvoke"):
        result = await graph.ainvoke(state)

    elif callable(graph):
        maybe = graph(state)
        result = await maybe if hasattr(maybe, "__await__") else maybe

    else:
        raise RuntimeError(f"Invalid graph returned by build_graph(): {type(graph)}")

    return ChatResponse(
        answer=(result.get("final_answer") if isinstance(result, dict) else None) or "Хариу үүссэнгүй.",
        meta=(result.get("meta") if isinstance(result, dict) else {}) or {}
    )
