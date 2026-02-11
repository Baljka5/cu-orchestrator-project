# app/core/llm.py
from typing import List, Dict, Any, Optional

from app.core.llm_client import chat_completion


class LLMClient:

    async def chat(
            self,
            messages: List[Dict[str, Any]],
            temperature: Optional[float] = None,
            max_tokens: Optional[int] = None
    ) -> str:
        system = None
        user = None

        for m in messages:
            if m.get("role") == "system" and system is None:
                system = m.get("content")
            if m.get("role") == "user":
                user = m.get("content")

        user = user or ""
        return await chat_completion(
            user_message=user,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
        )
