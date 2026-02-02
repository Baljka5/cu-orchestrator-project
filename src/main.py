from fastapi import FastAPI
from src.schemas import AskRequest, AskResponse
from src.config import settings
from src.logging_setup import setup_logging
from src.orchestration.graph import build_graph

setup_logging()

app = FastAPI(title=settings.app_name)
graph = build_graph()

@app.get("/health")
def health():
    return {"ok": True, "app": settings.app_name, "env": settings.env}

@app.post("/ask", response_model=AskResponse)
async def ask(req: AskRequest):
    state = {"user_query": req.query}
    out = await graph.ainvoke(state)
    c = out.get("classification", {}) or {}
    return AskResponse(
        answer=out.get("final_answer", ""),
        route=out.get("route"),
        label=c.get("label"),
        confidence=float(c.get("confidence", 0.0) or 0.0),
    )
