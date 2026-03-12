# app/agents/text2sql/history.py
from typing import Any, Dict, Optional

from app.db.chat_history import save_chat_history


def persist_result(
    *,
    query: str,
    result: Dict[str, Any],
    session_id: Optional[str] = None,
) -> None:
    if not isinstance(result, dict):
        return

    answer = result.get("answer")
    meta = result.get("meta") or {}

    mode = meta.get("mode")
    rule_name = meta.get("rule")
    error_code = meta.get("error")
    agent_name = meta.get("agent", "text2sql")

    generated_sql = answer if mode == "sql" else None
    answer_text = answer

    save_chat_history(
        session_id=session_id,
        user_query=query,
        answer_text=answer_text,
        generated_sql=generated_sql,
        agent_name=agent_name,
        mode=mode,
        rule_name=rule_name,
        error_code=error_code,
        meta=meta,
    )