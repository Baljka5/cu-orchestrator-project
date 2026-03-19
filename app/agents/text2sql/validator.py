from typing import Any, Dict, List, Set

def get_candidate_columns(candidates: List[Any]) -> Set[str]:
    cols = set()
    for t in candidates:
        for c in getattr(t, "columns", []):
            cols.add(c.name)
    return cols

def validate_and_repair_plan(
    plan: Dict[str, Any],
    candidates: List[Any],
    allowed_tables: Set[str],
    query: str,
) -> Dict[str, Any]:
    valid_columns = get_candidate_columns(candidates)

    cleaned_select = []
    for item in plan.get("select", []):
        if not isinstance(item, dict):
            continue
        expr = item.get("expr", "")
        if any(token in expr for token in ["sum(", "count(", "avg(", "min(", "max(", "toYear(", "toMonth("]):
            cleaned_select.append(item)
            continue
        if "." in expr:
            col = expr.split(".")[-1]
            if col in valid_columns:
                cleaned_select.append(item)
        else:
            if expr in valid_columns:
                cleaned_select.append(item)

    if cleaned_select:
        plan["select"] = cleaned_select

    cleaned_group_by = []
    for g in plan.get("group_by", []):
        if "." in g:
            col = g.split(".")[-1]
            if col in valid_columns:
                cleaned_group_by.append(g)
        else:
            if g in valid_columns:
                cleaned_group_by.append(g)
    plan["group_by"] = cleaned_group_by

    return plan