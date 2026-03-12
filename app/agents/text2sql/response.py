# app/agents/text2sql/response.py
from typing import Any, Callable, Dict


def text_response(text: str, rule: str) -> Dict[str, Any]:
    return {
        "answer": text,
        "meta": {
            "agent": "text2sql",
            "mode": "text",
            "rule": rule,
        },
    }


def sql_response(sql: str, rule: str, runner: Callable[..., Dict[str, Any]]) -> Dict[str, Any]:
    data = runner(sql, max_rows=50)
    meta = {
        "agent": "text2sql",
        "mode": "sql",
        "rule": rule,
        "data": data,
    }
    if data.get("error"):
        meta["error"] = data["error"]

    return {
        "answer": sql,
        "meta": meta,
    }


def error_response(message: str, code: str) -> Dict[str, Any]:
    return {
        "answer": message,
        "meta": {
            "agent": "text2sql",
            "mode": "sql",
            "error": code,
        },
    }