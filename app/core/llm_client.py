# app/app/core/llm_client.py
import os
import json
import logging
from typing import Optional, Dict, Any, List

import httpx

logger = logging.getLogger(__name__)

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://host.docker.internal:8001").rstrip("/")
LLM_MODEL = os.getenv("LLM_MODEL", "").strip()
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "60"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "512"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.2"))


def _headers() -> Dict[str, str]:
    return {"Content-Type": "application/json"}


async def _list_models(client: httpx.AsyncClient) -> List[str]:
    try:
        r = await client.get(f"{LLM_BASE_URL}/v1/models", headers=_headers())
        r.raise_for_status()
        data = r.json()
        models = data.get("data") or []
        ids: List[str] = []
        for m in models:
            if isinstance(m, dict) and m.get("id"):
                ids.append(str(m["id"]))
        return ids
    except Exception as e:
        logger.warning("Failed to fetch model list from %s: %s", LLM_BASE_URL, e)
        return []


async def _pick_model(client: httpx.AsyncClient) -> str:
    available_models = await _list_models(client)

    # .env дээр model өгсөн бол эхлээд түүнийг ашиглана
    if LLM_MODEL:
        if available_models and LLM_MODEL not in available_models:
            logger.warning(
                "Configured LLM_MODEL '%s' not found in server model list. Available: %s",
                LLM_MODEL,
                available_models,
            )
        return LLM_MODEL

    # .env дээр өгөөгүй бол server-ийн эхний model-ийг авна
    if available_models:
        logger.info("Auto-selected LLM model: %s", available_models[0])
        return available_models[0]

    # fallback
    logger.warning("No model discovered from server; using fallback model name 'llama3-awq'")
    return "llama3-awq"


def _truncate_text(value: str, max_len: int = 4000) -> str:
    if not isinstance(value, str):
        return str(value)
    if len(value) <= max_len:
        return value
    return value[:max_len] + "...[truncated]"


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

        url = f"{LLM_BASE_URL}/v1/chat/completions"

        try:
            r = await client.post(
                url,
                headers=_headers(),
                json=payload,
            )

            if r.status_code >= 400:
                logger.error("LLM request failed")
                logger.error("LLM URL: %s", url)
                logger.error("LLM status: %s", r.status_code)
                logger.error("LLM model: %s", model)
                logger.error("LLM response: %s", _truncate_text(r.text, 8000))

                try:
                    logger.error(
                        "LLM payload info: %s",
                        json.dumps(
                            {
                                "model": model,
                                "temperature": temp,
                                "max_tokens": mtok,
                                "messages_count": len(messages),
                                "system_len": len(messages[0]["content"]) if system else 0,
                                "user_len": len(user_message or ""),
                            },
                            ensure_ascii=False,
                        ),
                    )
                except Exception:
                    logger.exception("Failed to log LLM payload info")

            r.raise_for_status()
            data = r.json()

        except httpx.HTTPStatusError:
            raise
        except Exception:
            logger.exception("Unexpected error during LLM request")
            raise

        choices = data.get("choices") or []
        if choices and "message" in choices[0]:
            msg = choices[0]["message"]
            if isinstance(msg, dict):
                content = msg.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()

        if choices and "text" in choices[0] and isinstance(choices[0]["text"], str):
            return choices[0]["text"].strip()

        logger.warning("LLM returned no usable content. Raw response: %s", _truncate_text(json.dumps(data, ensure_ascii=False), 4000))
        return "Хариу үүссэнгүй."