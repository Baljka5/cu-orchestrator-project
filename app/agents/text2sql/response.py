from typing import Any, Callable, Dict, List, Optional


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_str(value: Any, default: str = "") -> str:
    return value if isinstance(value, str) else default


def _preview_from_data(data: Dict[str, Any]) -> Dict[str, Any]:
    data = _safe_dict(data)

    rows = _safe_list(data.get("rows"))
    cols = _safe_list(data.get("columns"))
    first_row = rows[0] if rows else None

    return {
        "row_count": len(rows),
        "columns": cols,
        "first_row": first_row,
    }


def _humanize_sql_error(error_text: str) -> str:
    err = _safe_str(error_text).strip()
    if not err:
        return "SQL execute алдаа гарлаа."

    upper_err = err.upper()

    if "UNKNOWN_IDENTIFIER" in upper_err:
        return f"Query үүслээ, гэхдээ preview execute дээр баганын нэрийн алдаа гарлаа: {err}"

    if "SYNTAX_ERROR" in upper_err:
        return f"Query үүслээ, гэхдээ preview execute дээр SQL syntax алдаа гарлаа: {err}"

    if "ONLY SELECT QUERIES ARE ALLOWED" in upper_err:
        return "Зөвхөн SELECT query preview execute хийхийг зөвшөөрнө."

    if "EMPTY_SQL" in upper_err:
        return "SQL query хоосон байна."

    return f"Query үүслээ, гэхдээ preview execute дээр алдаа гарлаа: {err}"


def text_response(text: str, rule: str, extra_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    meta = {
        "agent": "text2sql",
        "mode": "text",
        "rule": rule,
    }

    if isinstance(extra_meta, dict) and extra_meta:
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
        max_rows: int = 20,
) -> Dict[str, Any]:
    data = runner(sql, max_rows=max_rows)
    data = _safe_dict(data)

    preview = _preview_from_data(data)
    preview_rows = preview["row_count"]

    executed_sql = _safe_str(data.get("executed_sql"), sql)
    error_text = _safe_str(data.get("error"))
    auto_fixed = bool(data.get("auto_fixed"))
    original_error = _safe_str(data.get("original_error"))

    if error_text:
        answer_text = _humanize_sql_error(error_text)
    else:
        answer_text = f"Query үүслээ. {preview_rows} мөр preview байна."
        if auto_fixed:
            answer_text = f"Query үүслээ. Auto-fix хийгдэж {preview_rows} мөр preview байна."

    meta: Dict[str, Any] = {
        "agent": "text2sql",
        "mode": "sql",
        "rule": rule,
        "sql": sql,
        "executed_sql": executed_sql,
        "data": data,
        "preview": preview,
    }

    if auto_fixed:
        meta["auto_fixed"] = True

    if original_error:
        meta["original_error"] = original_error

    if error_text:
        meta["error"] = error_text

    return {
        "answer": answer_text,
        "meta": meta,
    }


def error_response(
        message: str,
        code: str,
        *,
        extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = {
        "agent": "text2sql",
        "mode": "error",
        "error": code,
    }

    if isinstance(extra_meta, dict) and extra_meta:
        meta.update(extra_meta)

    return {
        "answer": message,
        "meta": meta,
    }
