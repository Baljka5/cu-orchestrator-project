import os
import httpx
from typing import Any, Dict, Optional

def _join(base: str, path: str) -> str:
    base = (base or "").strip().rstrip("/")
    path = "/" + path.lstrip("/")
    return base + path

def _env_float(key: str, default: str) -> float:
    try:
        return float(os.getenv(key, default))
    except Exception:
        return float(default)

def _env_int(key: str, default: str) -> int:
    try:
        return int(os.getenv(key, default))
    except Exception:
        return int(default)

def _extract_chat_text(data: Dict[str, Any]) -> str:
    # OpenAI ChatCompletions compatible
    try:
        return (data.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
    except Exception:
        return ""

def _extract_responses_text(data: Dict[str, Any]) -> str:
    # OpenAI Responses API compatible (vLLM returns output_text in several shapes)
    # Common shapes:
    # 1) {"output":[{"content":[{"type":"output_text","text":"..."}]}]}
    # 2) {"output_text":"..."}  (some servers)
    if isinstance(data.get("output_text"), str) and data["output_text"]:
        return data["output_text"]

    out = data.get("output")
    if isinstance(out, list) and out:
        content = out[0].get("content")
        if isinstance(content, list):
            texts = []
            for c in content:
                if c.get("type") in ("output_text", "text") and isinstance(c.get("text"), str):
                    texts.append(c["text"])
            return "".join(texts).strip()
    return ""

async def chat_completion(prompt: str, system: str = "You are a helpful assistant.") -> str:
    base_url = os.getenv("LLM_BASE_URL", "http://localhost:8001")
    model = os.getenv("LLM_MODEL", "local")
    timeout = _env_float("LLM_TIMEOUT", "60")
    temperature = _env_float("LLM_TEMPERATURE", "0.2")
    max_tokens = _env_int("LLM_MAX_TOKENS", "512")

    chat_url = _join(base_url, "/v1/chat/completions")
    resp_url = _join(base_url, "/v1/responses")

    chat_payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    # Responses API payload
    resp_payload = {
        "model": model,
        "input": prompt,
        "temperature": temperature,
        "max_output_tokens": max_tokens,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        # 1) Try ChatCompletions
        r = await client.post(chat_url, json=chat_payload)
        if r.status_code == 404:
            # 2) Fallback to Responses
            r2 = await client.post(resp_url, json=resp_payload)
            r2.raise_for_status()
            data2 = r2.json()
            text2 = _extract_responses_text(data2)
            return text2 or "Хариу үүссэнгүй."

        # Any other error -> raise
        r.raise_for_status()
        data = r.json()
        text = _extract_chat_text(data)
        return text or "Хариу үүссэнгүй."
