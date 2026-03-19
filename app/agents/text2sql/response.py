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

    preview_rows = len(data.get("rows") or [])
    preview_cols = data.get("columns") or []
    first_row = data.get("rows", [None])[0] if data.get("rows") else None

    answer_text = f"Query үүслээ. {preview_rows} мөр preview байна."
    if data.get("error"):
        answer_text = f"Query үүслээ, гэхдээ preview execute дээр алдаа гарлаа: {data['error']}"

    meta = {
        "agent": "text2sql",
        "mode": "sql",
        "rule": rule,
        "sql": sql,
        "data": data,
        "preview": {
            "row_count": preview_rows,
            "columns": preview_cols,
            "first_row": first_row,
        },
    }

    if data.get("error"):
        meta["error"] = data["error"]

    return {
        "answer": answer_text,
        "meta": meta,
    }

def error_response(message: str, code: str) -> Dict[str, Any]:
    return {
        "answer": message,
        "meta": {
            "agent": "text2sql",
            "mode": "error",
            "error": code,
        },
    }