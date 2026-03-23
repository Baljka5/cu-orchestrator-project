import json
import re
from typing import Any, Dict, List, Optional

DEFAULT_LIMIT = 50
MAX_LIMIT = 500


def safe_json_loads(raw: str) -> Optional[Dict[str, Any]]:
    """
    Try to parse a JSON object from raw LLM output.

    Supports:
    - pure JSON
    - markdown fenced ```json ... ```
    - extra explanation text before/after the JSON object
    """
    if not raw or not isinstance(raw, str):
        return None

    raw = raw.strip()
    if not raw:
        return None

    # 1) direct parse
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass

    # 2) fenced code block parse
    fenced_patterns = [
        r"```json\s*(\{.*?\})\s*```",
        r"```\s*(\{.*?\})\s*```",
    ]
    for pattern in fenced_patterns:
        try:
            m = re.search(pattern, raw, re.DOTALL | re.IGNORECASE)
            if m:
                obj = json.loads(m.group(1))
                return obj if isinstance(obj, dict) else None
        except Exception:
            pass

    # 3) first top-level json object-like block
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else None
    except Exception:
        pass

    return None


def ensure_list(value: Any) -> List[Any]:
    """
    Normalize a value into list form.

    Examples:
    - None -> []
    - "a" -> ["a"]
    - {"x": 1} -> [{"x": 1}]
    - [1,2] -> [1,2]
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def ensure_str_list(value: Any) -> List[str]:
    """
    Normalize to a list[str], dropping empty values.
    """
    items = ensure_list(value)
    result: List[str] = []
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def ensure_dict_list(value: Any) -> List[Dict[str, Any]]:
    """
    Normalize to a list[dict], dropping non-dict items.
    """
    items = ensure_list(value)
    result: List[Dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            result.append(item)
    return result


def normalize_limit(value: Any, default: int = DEFAULT_LIMIT) -> int:
    """
    Normalize limit to a safe bounded integer.
    """
    if value is None or value == "":
        return default

    try:
        limit = int(value)
    except Exception:
        return default

    if limit <= 0:
        return default

    return min(limit, MAX_LIMIT)


def normalize_bool(value: Any, default: bool = False) -> bool:
    """
    Normalize common truthy / falsy values.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False

    if isinstance(value, (int, float)):
        return bool(value)

    return default


def normalize_text(value: Any, default: str = "") -> str:
    """
    Normalize scalar text field.
    """
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def normalize_plan(plan: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Normalize planner output into a stable structure for executor usage.

    Supported keys:
    - fact_table: str
    - select: list[str]
    - metrics: list[str]
    - joins: list[dict]
    - where: list[str]
    - group_by: list[str]
    - having: list[str]
    - order_by: list[str]
    - limit: int
    - distinct: bool
    - intent: str
    - domain: str
    - time_grain: str
    - notes: list[str]
    """
    plan = plan or {}

    normalized = {
        "fact_table": normalize_text(plan.get("fact_table")),
        "select": ensure_str_list(plan.get("select")),
        "metrics": ensure_str_list(plan.get("metrics")),
        "joins": ensure_dict_list(plan.get("joins")),
        "where": ensure_str_list(plan.get("where")),
        "group_by": ensure_str_list(plan.get("group_by")),
        "having": ensure_str_list(plan.get("having")),
        "order_by": ensure_str_list(plan.get("order_by")),
        "limit": normalize_limit(plan.get("limit")),
        "distinct": normalize_bool(plan.get("distinct"), default=False),
        "intent": normalize_text(plan.get("intent")),
        "domain": normalize_text(plan.get("domain")),
        "time_grain": normalize_text(plan.get("time_grain")),
        "notes": ensure_str_list(plan.get("notes")),
    }

    # backward compatibility:
    # if metrics is empty but select contains aggregates, executor can still work.
    # if select is empty but metrics exists, keep as-is and let executor compose final SELECT.
    return normalized


def parse_and_normalize_plan(raw: str) -> Dict[str, Any]:
    """
    Convenience helper:
    raw LLM output -> parsed dict -> normalized plan
    """
    parsed = safe_json_loads(raw) or {}
    return normalize_plan(parsed)
