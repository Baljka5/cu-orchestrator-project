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
    data = runner(sql, max_rows=20)

    if data.get("error"):
        answer = f"Query ажиллуулахад алдаа гарлаа: {data['error']}"
    elif data.get("rows"):
        answer = f"'{rule}' асуултад зориулсан SQL үүслээ."
    else:
        answer = "SQL үүслээ, гэхдээ preview result хоосон байна."

    return {
        "answer": answer,
        "meta": {
            "agent": "text2sql",
            "mode": "sql",
            "rule": rule,
            "sql": sql,
            "data": data,
            **({"error": data["error"]} if data.get("error") else {}),
        },
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