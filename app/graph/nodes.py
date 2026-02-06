import re
import os
import json
import httpx
from typing import Any, Dict

from app.core.schemas import OrchestratorState


def _normalize(text: str) -> str:
    return " ".join((text or "").strip().split())


async def node_reformulate(state: OrchestratorState) -> OrchestratorState:
    state.normalized_message = _normalize(state.raw_message)
    return state


async def node_classify(state: OrchestratorState) -> OrchestratorState:
    # forced
    if state.forced_agent:
        state.classification = {"agent": state.forced_agent, "confidence": 1.0, "rationale": "forced_from_ui"}
        state.meta["agent"] = state.forced_agent
        return state

    q = (state.normalized_message or state.raw_message or "").strip()
    q_low = q.lower()
    q_up = q.upper()

    data_keywords = [
        "борлуул", "sales", "netsale", "gross",
        "татвар", "discount", "өртөг",
        "тоо", "хэд", "тайлан", "дэлгүүр",
        "clickhouse", "sql", "query", "select"
    ]

    if any(k in q_low for k in data_keywords) or re.search(r"\bCU\d{3,4}\b", q_up):
        state.classification = {"agent": "text2sql", "confidence": 0.9, "rationale": "rule_data_query"}
        state.meta["agent"] = "text2sql"
        return state

    state.classification = {"agent": "general", "confidence": 0.4, "rationale": "fallback_general"}
    state.meta["agent"] = "general"
    return state


async def _call_vllm_chat(prompt: str) -> str:
    """
    vLLM OpenAI-compatible endpoint руу асууна.
    ENV: LLM_BASE_URL=http://host.docker.internal:8001 эсвэл http://llm:8001
    """
    base_url = os.getenv("LLM_BASE_URL", "http://llm:8001").rstrip("/")
    model = os.getenv("LLM_MODEL", "/models/Llama-3.1-8B-Instruct-AWQ")

    url = f"{base_url}/v1/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Та CU Orchestrator туслах. Богино, тодорхой хариул."},
            {"role": "user", "content": prompt},
        ],
        "temperature": float(os.getenv("LLM_TEMPERATURE", "0.2")),
        "max_tokens": int(os.getenv("LLM_MAX_TOKENS", "512")),
    }

    timeout = float(os.getenv("LLM_TIMEOUT", "60"))

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()

    # OpenAI style
    try:
        return data["choices"][0]["message"]["content"].strip()
    except Exception:
        return json.dumps(data)[:1200]


async def node_run_agent(state: OrchestratorState) -> OrchestratorState:
    agent = (state.meta.get("agent") or (state.classification or {}).get("agent") or "general").strip()

    if agent == "text2sql":
        # ---- STUB: энд schema + SQL generation + ClickHouse run нэмнэ ----
        q = state.normalized_message or state.raw_message
        state.meta["sql"] = "-- TODO: generate SQL for ClickHouse\n-- question: " + q
        state.final_answer = (
            "Text2SQL agent сонгогдлоо.\n"
            "Одоогоор SQL үүсгэх/ажиллуулах хэсгийг stub хийсэн байна.\n"
            "Дараагийн алхам: ClickHouse schema (table/column тайлбар)-аа холбоод SQL гаргадаг болгоё."
        )
        return state

    # general agent: LLM call
    prompt = state.normalized_message or state.raw_message
    try:
        answer = await _call_vllm_chat(prompt)
        state.final_answer = answer or "Хариу хоосон ирлээ."
    except Exception as e:
        state.final_answer = f"LLM call алдаа: {type(e).__name__}: {e}"
    return state


async def node_finalize(state: OrchestratorState) -> Dict[str, Any]:
    # Graph-ийн эцсийн result dict
    return {
        "final_answer": state.final_answer or "Хариу үүсээгүй байна.",
        "meta": state.meta,
        "classification": state.classification,
    }
