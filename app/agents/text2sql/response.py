from typing import Any, Callable, Dict, List, Optional


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def text_response(
        text: str,
        rule: str,
        *,
        domain: str = "",
        intent: str = "",
        extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = {
        "agent": "text2sql",
        "mode": "text",
        "rule": rule,
        "domain": domain,
        "intent": intent,
    }

    if extra_meta:
        meta.update(extra_meta)

    return {
        "answer": text,
        "meta": meta,
    }


def sql_response(
        sql: str,
        rule: str,
        runner: Callable[..., Dict[str, Any]],
        *,
        domain: str = "",
        intent: str = "",
        plan: Optional[Dict[str, Any]] = None,
        warnings: Optional[List[str]] = None,
        max_rows: int = 20,
) -> Dict[str, Any]:
    """
    Build a standard SQL response with preview execution.

    runner(sql, max_rows=N) is expected to return a dict like:
    {
        "columns": [...],
        "rows": [...],
        "error": "...",   # optional
        ...
    }
    """

    raw_data = runner(sql, max_rows=max_rows)
    data = _safe_dict(raw_data)

    rows = _safe_list(data.get("rows"))
    columns = _safe_list(data.get("columns"))
    error = data.get("error")

    preview_rows = len(rows)
    preview_cols = columns
    first_row = rows[0] if rows else None

    if error:
        answer_text = f"Query үүслээ, гэхдээ preview execute дээр алдаа гарлаа: {error}"
    else:
        if preview_rows == 0:
            answer_text = "Query үүслээ. Preview дээр 0 мөр байна."
        else:
            answer_text = f"Query үүслээ. {preview_rows} мөр preview байна."

    meta = {
        "agent": "text2sql",
        "mode": "sql",
        "rule": rule,
        "domain": domain,
        "intent": intent,
        "sql": sql,
        "plan": plan or {},
        "warnings": warnings or [],
        "data": data,
        "preview": {
            "row_count": preview_rows,
            "column_count": len(preview_cols),
            "columns": preview_cols,
            "first_row": first_row,
        },
    }

    if error:
        meta["error"] = error

    return {
        "answer": answer_text,
        "meta": meta,
    }


def plan_response(
        plan: Dict[str, Any],
        rule: str,
        *,
        domain: str = "",
        intent: str = "",
        message: str = "Plan үүслээ.",
        extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Use this when planner output should be returned without executing SQL yet.
    """
    meta = {
        "agent": "text2sql",
        "mode": "plan",
        "rule": rule,
        "domain": domain,
        "intent": intent,
        "plan": plan or {},
    }

    if extra_meta:
        meta.update(extra_meta)

    return {
        "answer": message,
        "meta": meta,
    }


def error_response(
        message: str,
        code: str,
        *,
        domain: str = "",
        intent: str = "",
        extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = {
        "agent": "text2sql",
        "mode": "error",
        "error": code,
        "domain": domain,
        "intent": intent,
    }

    if extra_meta:
        meta.update(extra_meta)

    return {
        "answer": message,
        "meta": meta,
    }
