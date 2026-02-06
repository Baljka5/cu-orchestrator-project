import os
import httpx

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://host.docker.internal:8001/v1").rstrip("/")
LLM_MODEL = os.getenv("LLM_MODEL", "cu-llama31-awq")
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "60"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.2"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "512"))


async def chat_completion(user_message: str, system: str = "") -> str:
    url = f"{LLM_BASE_URL}/chat/completions"

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system or "You are a helpful assistant."},
            {"role": "user", "content": user_message},
        ],
        "temperature": LLM_TEMPERATURE,
        "max_tokens": LLM_MAX_TOKENS,
    }

    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()

    # OpenAI-compatible response
    return (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
