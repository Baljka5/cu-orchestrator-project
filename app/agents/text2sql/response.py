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


def sql_response(sql: str, rule: str, runner):
    data = runner(sql, max_rows=50)

    human_answer = "SQL query амжилттай үүслээ."
    if data.get("error"):
        human_answer = f"Query ажиллуулахад алдаа гарлаа: {data['error']}"
    elif data.get("rows"):
        human_answer = f"{rule} асуултад зориулсан query үүслээ. {len(data['rows'])} мөр preview байна."
    else:
        human_answer = "SQL query үүслээ, гэхдээ preview result хоосон байна."

    meta = {
        "agent": "text2sql",
        "mode": "sql",
        "rule": rule,
        "sql": sql,
        "data": data,
    }
    if data.get("error"):
        meta["error"] = data["error"]

    return {
        "answer": human_answer,
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