import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from src.config import settings

class LLMClient:
    def __init__(self):
        self.base_url = settings.llm_base_url.rstrip("/")
        self.model = settings.llm_model

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6))
    async def chat(self, messages, temperature=None, max_tokens=None):
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": settings.llm_temperature if temperature is None else temperature,
            "max_tokens": settings.llm_max_tokens if max_tokens is None else max_tokens,
        }
        async with httpx.AsyncClient(timeout=settings.llm_timeout) as client:
            r = await client.post(f"{self.base_url}/v1/chat/completions", json=payload)
            r.raise_for_status()
            data = r.json()
            return data["choices"][0]["message"]["content"]

llm_client = LLMClient()
