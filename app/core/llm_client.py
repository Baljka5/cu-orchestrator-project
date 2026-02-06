import os
import httpx

def _join(base: str, path: str) -> str:
    base = (base or "").strip().rstrip("/")
    path = "/" + path.lstrip("/")
    return base + path

async def chat_completion(prompt: str, system: str = "You are a helpful assistant.") -> str:
    base_url = os.getenv("LLM_BASE_URL", "http://localhost:8001")
    model = os.getenv("LLM_MODEL", "local")
    timeout = float(os.getenv("LLM_TIMEOUT", "60"))
    temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    max_tokens = int(os.getenv("LLM_MAX_TOKENS", "512"))

    url = _join(base_url, "/v1/chat/completions")

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()

    # OpenAI compatible response
    return (data.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
