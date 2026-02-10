# app/app/core/llm_client.py
import os
from typing import Optional, Dict, Any, List

import httpx

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://host.docker.internal:8001").rstrip("/")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3-awq")
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "60"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "512"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.2"))


def _headers() -> Dict[str, str]:
    return {"Content-Type": "application/json"}


async def _pick_model(client: httpx.AsyncClient) -> str:
    if LLM_MODEL:
        return LLM_MODEL

    try:
        r = await client.get(f"{LLM_BASE_URL}/v1/models", headers=_headers())
        r.raise_for_status()
        data = r.json()
        models = data.get("data") or []
        if models and isinstance(models, list) and "id" in models[0]:
            return models[0]["id"]
    except Exception:
        pass

    return "llama3-awq"


async def chat_completion(
    user_message: str,
    system: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> str:
    temp = LLM_TEMPERATURE if temperature is None else temperature
    mtok = LLM_MAX_TOKENS if max_tokens is None else max_tokens

    messages: List[Dict[str, Any]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user_message})

    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        model = await _pick_model(client)

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temp,
            "max_tokens": mtok,
        }

        r = await client.post(
            f"{LLM_BASE_URL}/v1/chat/completions",
            headers=_headers(),
            json=payload,
        )
        r.raise_for_status()
        data = r.json()

        choices = data.get("choices") or []
        if choices and "message" in choices[0]:
            msg = choices[0]["message"]
            if isinstance(msg, dict):
                content = msg.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()

        if choices and "text" in choices[0] and isinstance(choices[0]["text"], str):
            return choices[0]["text"].strip()

        return "Хариу үүссэнгүй."
