# CU Orchestrator (FastAPI + LangGraph)

## Run (Docker)
1) Copy `.env.example` -> `.env` and edit if needed
2) `docker compose up -d --build`
3) Open:
   - API docs: http://localhost:8000/docs
   - UI:      http://localhost:8000/

## LLM
Uses OpenAI-compatible endpoint:
- LLM_BASE_URL=http://llm:8001/v1
- Chat completions path: /chat/completions
