import json
import re
from typing import Any, Dict, List, Optional


EMPTY_PLAN: Dict[str, Any] = {
    "fact_table": "",
    "select": [],
    "joins": [],
    "where": [],
    "group_by": [],
    "order_by": [],
    "limit": 0,
}


def ensure_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def ensure_str(value: Any, default: str = "") -> str:
    return value.strip() if isinstance(value, str) else default


def ensure_int(value: Any, default: int = 50) -> int:
    try:
        return int(value)
    except Exception:
        return default


def strip_code_fences(raw: str) -> str:
    if not isinstance(raw, str):
        return ""

    text = raw.strip()

    # ```json ... ```
    text = re.sub(r"^\s*```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```\s*$", "", text)

    return text.strip()


def extract_json_object(raw: str) -> Optional[str]:
    if not isinstance(raw, str):
        return None

    text = strip_code_fences(raw)

    # direct object
    if text.startswith("{") and text.endswith("}"):
        return text

    # first JSON object block
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        return m.group(0).strip()

    return None


def remove_trailing_commas(text: str) -> str:
    if not isinstance(text, str):
        return ""
    # { "a": 1, } or [1,2,]
    text = re.sub(r",\s*([}\]])", r"\1", text)
    return text


def safe_json_loads(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None

    # 1) direct parse
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # 2) strip fences + extract object
    extracted = extract_json_object(raw)
    if not extracted:
        return None

    # 3) parse extracted
    try:
        obj = json.loads(extracted)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # 4) trailing comma fix
    cleaned = remove_trailing_commas(extracted)
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None

    return None


def normalize_select_items(select_items: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for item in ensure_list(select_items):
        if isinstance(item, dict):
            expr = ensure_str(item.get("expr"))
            alias = ensure_str(item.get("as"))
            if expr:
                out.append({"expr": expr, "as": alias})
            continue

        if isinstance(item, str) and item.strip():
            out.append({"expr": item.strip(), "as": ""})

    return out


def normalize_join_items(joins: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for j in ensure_list(joins):
        if not isinstance(j, dict):
            continue

        join_type = ensure_str(j.get("type"), "LEFT").upper()
        table_name = ensure_str(j.get("table"))
        alias = ensure_str(j.get("alias"))
        on = ensure_str(j.get("on"))

        if not table_name:
            continue

        if join_type not in {"LEFT", "INNER", "RIGHT", "FULL", "CROSS"}:
            join_type = "LEFT"

        out.append(
            {
                "type": join_type,
                "table": table_name,
                "alias": alias,
                "on": on,
            }
        )

    return out


def normalize_str_list(values: Any) -> List[str]:
    out: List[str] = []
    for v in ensure_list(values):
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
    return out


def normalize_limit(value: Any, default: int = 50) -> int:
    limit = ensure_int(value, default=default)

    # out-of-domain / empty plan special case
    if limit <= 0:
        return 0

    # safe bounds
    return max(1, min(limit, 500))


def normalize_plan(plan: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(plan, dict):
        return dict(EMPTY_PLAN)

    fact_table = ensure_str(plan.get("fact_table"))

    normalized = {
        "fact_table": fact_table,
        "select": normalize_select_items(plan.get("select")),
        "joins": normalize_join_items(plan.get("joins")),
        "where": normalize_str_list(plan.get("where")),
        "group_by": normalize_str_list(plan.get("group_by")),
        "order_by": normalize_str_list(plan.get("order_by")),
        "limit": normalize_limit(plan.get("limit"), default=50),
    }

    # If everything is empty, treat as empty plan
    if (
        not normalized["fact_table"]
        and not normalized["select"]
        and not normalized["joins"]
        and not normalized["where"]
        and not normalized["group_by"]
        and not normalized["order_by"]
        and normalized["limit"] == 0
    ):
        return dict(EMPTY_PLAN)

    # If no fact_table and no useful content -> empty plan
    if not normalized["fact_table"] and not normalized["select"] and not normalized["joins"]:
        return dict(EMPTY_PLAN)

    return normalized


def is_empty_plan(plan: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(plan, dict):
        return True

    return (
        not ensure_str(plan.get("fact_table"))
        and not ensure_list(plan.get("select"))
        and not ensure_list(plan.get("joins"))
        and not ensure_list(plan.get("where"))
        and not ensure_list(plan.get("group_by"))
        and not ensure_list(plan.get("order_by"))
        and ensure_int(plan.get("limit"), default=0) == 0
    )


def make_empty_plan() -> Dict[str, Any]:
    return dict(EMPTY_PLAN)