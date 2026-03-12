# app/agents/text2sql/plan_utils.py
import json
import re
from typing import Any, Dict, List, Optional


def safe_json_loads(raw: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0))
    except Exception:
        return None

    return None


def ensure_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def normalize_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    plan = plan or {}
    return {
        "fact_table": plan.get("fact_table") or "",
        "select": ensure_list(plan.get("select")),
        "joins": ensure_list(plan.get("joins")),
        "where": ensure_list(plan.get("where")),
        "group_by": ensure_list(plan.get("group_by")),
        "order_by": ensure_list(plan.get("order_by")),
        "limit": plan.get("limit", 50),
    }
