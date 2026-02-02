# CU Orchestrator (Multi-Agent Orchestration)

FastAPI + LangGraph + local LLM (vLLM OpenAI-compatible) starter project for:
- Prompt Guarding
- Query Reformulation (galig->kiril / standardization)
- Prompt Classification (route to agent)
- Multi-agent routing and response synthesis
- Evaluation scaffold (DeepEval)

## Quickstart (Docker + vLLM)
1) Copy env
```bash
cp .env.example .env
```
2) Put model files under `./models/Llama-3.1-8B-Instruct` (or change compose path)
3) Start
```bash
docker compose up --build
```
4) Test
```bash
curl -X POST http://localhost:8000/ask -H "Content-Type: application/json" -d '{"query":"CU-ийн дотоод журамд чөлөө авах процесс?"}'
```

## Local run (no docker)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn src.main:app --reload --port 8000
```

## Eval
```bash
python -m src.eval.deepeval_runner
```

> Note: Agents are stubs by default. Plug your RAG/DB integrations inside `src/agents/*`.
