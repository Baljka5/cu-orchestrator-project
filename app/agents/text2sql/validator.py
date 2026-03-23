import re
from typing import Any, Dict, List, Set

ALLOWED_FUNCTION_PREFIXES = (
    "sum(",
    "count(",
    "avg(",
    "min(",
    "max(",
    "round(",
    "if(",
    "sumif(",
    "countif(",
    "avgif(",
    "toyear(",
    "tomonth(",
    "toyyyymm(",
    "todate(",
    "today(",
    "now(",
    "coalesce(",
    "nullif(",
    "upper(",
    "lower(",
    "substring(",
    "concat(",
    "multiif(",
)

SQL_KEYWORDS = {
    "and", "or", "not", "in", "is", "null", "like", "between",
    "asc", "desc", "as", "on", "if", "then", "else", "end",
    "case", "when", "distinct",
}


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_str(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def get_candidate_columns(candidates: List[Any]) -> Set[str]:
    cols: Set[str] = set()

    for t in candidates:
        for c in getattr(t, "columns", []):
            name = getattr(c, "name", "")
            if name:
                cols.add(name)
                cols.add(name.lower())

    return cols


def get_candidate_table_columns(candidates: List[Any]) -> Dict[str, Set[str]]:
    table_cols: Dict[str, Set[str]] = {}

    for t in candidates:
        table_name = getattr(t, "table", "")
        if not table_name:
            continue

        cols = table_cols.setdefault(table_name, set())
        cols_lower = table_cols.setdefault(table_name.lower(), set())

        for c in getattr(t, "columns", []):
            name = getattr(c, "name", "")
            if name:
                cols.add(name)
                cols.add(name.lower())
                cols_lower.add(name)
                cols_lower.add(name.lower())

    return table_cols


def get_known_alias_map(plan: Dict[str, Any]) -> Dict[str, str]:
    alias_map: Dict[str, str] = {"f": ""}

    fact_table = _safe_str(plan.get("fact_table"))
    if fact_table:
        fact_base = fact_table.split(".")[-1]
        alias_map["f"] = fact_base

    for j in _safe_list(plan.get("joins")):
        if not isinstance(j, dict):
            continue
        alias = _safe_str(j.get("alias"))
        table_name = _safe_str(j.get("table"))
        if alias and table_name:
            alias_map[alias] = table_name.split(".")[-1]

    return alias_map


def is_function_expr(expr: str) -> bool:
    e = _safe_str(expr).lower()
    return e.startswith(ALLOWED_FUNCTION_PREFIXES)


def extract_alias_column_refs(expr: str) -> List[tuple[str, str]]:
    """
    Extract alias.column style refs like:
    f.StoreID, d1.GDS_NM
    """
    if not isinstance(expr, str):
        return []
    return re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b", expr)


def extract_bare_identifiers(expr: str) -> List[str]:
    """
    Extract possible bare column identifiers.
    Excludes numeric literals and common SQL keywords.
    """
    if not isinstance(expr, str):
        return []

    tokens = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expr)
    out: List[str] = []

    for t in tokens:
        tl = t.lower()
        if tl in SQL_KEYWORDS:
            continue
        if tl in {"today", "now", "sum", "count", "avg", "min", "max", "round",
                  "sumif", "countif", "avgif", "toyear", "tomonth", "toyyyymm",
                  "todate", "coalesce", "nullif", "upper", "lower", "substring",
                  "concat", "multiif"}:
            continue
        out.append(t)

    return out


def is_valid_column_ref(
        alias: str,
        col: str,
        alias_map: Dict[str, str],
        valid_columns: Set[str],
        table_columns: Dict[str, Set[str]],
) -> bool:
    if col in valid_columns or col.lower() in valid_columns:
        return True

    table_name = alias_map.get(alias, "")
    if table_name:
        tcols = table_columns.get(table_name, set()) | table_columns.get(table_name.lower(), set())
        if col in tcols or col.lower() in tcols:
            return True

    return False


def is_valid_expr(
        expr: str,
        alias_map: Dict[str, str],
        valid_columns: Set[str],
        table_columns: Dict[str, Set[str]],
) -> bool:
    expr = _safe_str(expr)
    if not expr:
        return False

    # direct function expressions are generally allowed, but referenced cols must still be sane
    refs = extract_alias_column_refs(expr)
    if refs:
        for alias, col in refs:
            if not is_valid_column_ref(alias, col, alias_map, valid_columns, table_columns):
                return False

    # if it's a simple function and all alias refs are fine -> allow
    if is_function_expr(expr):
        return True

    # simple alias.col
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", expr):
        alias, col = expr.split(".", 1)
        return is_valid_column_ref(alias, col, alias_map, valid_columns, table_columns)

    # simple bare column / alias
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", expr):
        return expr in valid_columns or expr.lower() in valid_columns

    # Expressions like:
    # total_net_sales DESC
    # toYYYYMM(f.SalesDate) ASC
    ordering_expr = re.sub(r"\s+(ASC|DESC)\s*$", "", expr, flags=re.IGNORECASE).strip()
    if ordering_expr != expr:
        return is_valid_expr(ordering_expr, alias_map, valid_columns, table_columns)

    # If expression has quoted strings / operators, validate alias refs at least
    if refs:
        return True

    # bare identifiers fallback
    bare_tokens = extract_bare_identifiers(expr)
    if not bare_tokens:
        return True

    # if all bare identifiers are valid columns, allow
    for tok in bare_tokens:
        if tok in valid_columns or tok.lower() in valid_columns:
            continue
        return False

    return True


def clean_select_items(
        select_items: List[Any],
        alias_map: Dict[str, str],
        valid_columns: Set[str],
        table_columns: Dict[str, Set[str]],
) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []

    for item in _safe_list(select_items):
        if not isinstance(item, dict):
            continue

        expr = _safe_str(item.get("expr"))
        alias = _safe_str(item.get("as"))

        if not expr:
            continue

        if is_valid_expr(expr, alias_map, valid_columns, table_columns):
            cleaned.append({"expr": expr, "as": alias})

    return cleaned


def clean_str_expr_list(
        values: List[Any],
        alias_map: Dict[str, str],
        valid_columns: Set[str],
        table_columns: Dict[str, Set[str]],
) -> List[str]:
    cleaned: List[str] = []

    for v in _safe_list(values):
        expr = _safe_str(v)
        if not expr:
            continue

        if is_valid_expr(expr, alias_map, valid_columns, table_columns):
            cleaned.append(expr)

    return cleaned


def clean_join_items(
        joins: List[Any],
        allowed_tables: Set[str],
        valid_columns: Set[str],
        table_columns: Dict[str, Set[str]],
        fact_table: str,
) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []

    fact_base = fact_table.split(".")[-1] if fact_table else ""

    temp_alias_map: Dict[str, str] = {"f": fact_base}

    for j in _safe_list(joins):
        if not isinstance(j, dict):
            continue

        join_type = _safe_str(j.get("type")).upper() or "LEFT"
        table_name = _safe_str(j.get("table"))
        alias = _safe_str(j.get("alias"))
        on_expr = _safe_str(j.get("on"))

        if not table_name or not alias or not on_expr:
            continue

        table_base = table_name.split(".")[-1]
        if table_name not in allowed_tables and table_base not in allowed_tables:
            continue

        temp_alias_map[alias] = table_base

        if not is_valid_expr(on_expr, temp_alias_map, valid_columns, table_columns):
            temp_alias_map.pop(alias, None)
            continue

        cleaned.append(
            {
                "type": join_type if join_type in {"LEFT", "INNER", "RIGHT", "FULL", "CROSS"} else "LEFT",
                "table": table_name,
                "alias": alias,
                "on": on_expr,
            }
        )

    return cleaned


def deduplicate_select(select_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()

    for item in select_items:
        expr = _safe_str(item.get("expr"))
        alias = _safe_str(item.get("as"))
        key = (expr, alias)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def deduplicate_str_list(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()

    for item in items:
        s = _safe_str(item)
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)

    return out


def validate_and_repair_plan(
        plan: Dict[str, Any],
        candidates: List[Any],
        allowed_tables: Set[str],
        query: str,
) -> Dict[str, Any]:
    plan = _safe_dict(plan).copy()

    valid_columns = get_candidate_columns(candidates)
    table_columns = get_candidate_table_columns(candidates)

    fact_table = _safe_str(plan.get("fact_table"))
    if fact_table:
        fact_base = fact_table.split(".")[-1]
        if fact_table not in allowed_tables and fact_base not in allowed_tables:
            # fallback to first candidate
            if candidates:
                t = candidates[0]
                plan["fact_table"] = f"{getattr(t, 'db', '')}.{getattr(t, 'table', '')}".strip(".")
            else:
                plan["fact_table"] = ""

    fact_table = _safe_str(plan.get("fact_table"))

    # joins first, so alias map becomes reliable
    joins = clean_join_items(
        joins=_safe_list(plan.get("joins")),
        allowed_tables=allowed_tables,
        valid_columns=valid_columns,
        table_columns=table_columns,
        fact_table=fact_table,
    )
    plan["joins"] = joins

    alias_map = get_known_alias_map(plan)

    # clean select/group/order/where
    cleaned_select = clean_select_items(
        select_items=_safe_list(plan.get("select")),
        alias_map=alias_map,
        valid_columns=valid_columns,
        table_columns=table_columns,
    )
    cleaned_where = clean_str_expr_list(
        values=_safe_list(plan.get("where")),
        alias_map=alias_map,
        valid_columns=valid_columns,
        table_columns=table_columns,
    )
    cleaned_group_by = clean_str_expr_list(
        values=_safe_list(plan.get("group_by")),
        alias_map=alias_map,
        valid_columns=valid_columns,
        table_columns=table_columns,
    )
    cleaned_order_by = clean_str_expr_list(
        values=_safe_list(plan.get("order_by")),
        alias_map=alias_map,
        valid_columns=valid_columns,
        table_columns=table_columns,
    )

    plan["select"] = deduplicate_select(cleaned_select)
    plan["where"] = deduplicate_str_list(cleaned_where)
    plan["group_by"] = deduplicate_str_list(cleaned_group_by)
    plan["order_by"] = deduplicate_str_list(cleaned_order_by)

    # If group_by exists, keep only valid non-aggregate selects or aggregate selects
    if plan["group_by"]:
        repaired_select: List[Dict[str, Any]] = []
        group_set = {g.lower() for g in plan["group_by"]}

        for item in plan["select"]:
            expr = _safe_str(item.get("expr"))
            if not expr:
                continue

            if is_function_expr(expr):
                repaired_select.append(item)
                continue

            if expr.lower() in group_set:
                repaired_select.append(item)
                continue

            # allow alias.column if same expression exists in group_by
            if any(expr.lower() == g.lower() for g in plan["group_by"]):
                repaired_select.append(item)
                continue

        if repaired_select:
            plan["select"] = repaired_select

    # minimal safety: if select becomes empty but fact exists, add count()
    if plan.get("fact_table") and not plan.get("select"):
        plan["select"] = [{"expr": "count()", "as": "cnt"}]

    # limit normalize
    try:
        limit = int(plan.get("limit") or 50)
    except Exception:
        limit = 50

    if limit < 0:
        limit = 0
    elif limit == 0:
        limit = 50
    else:
        limit = min(limit, 500)

    plan["limit"] = limit

    return plan
