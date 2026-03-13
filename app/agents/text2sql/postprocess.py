from typing import Any, Dict, List

from app.agents.text2sql.intents import Intent
from app.agents.text2sql.registry_utils import normalize_table_ref
from app.config import CLICKHOUSE_DATABASE


CANONICAL_REPLACEMENTS = {
    "f.Store": "f.StoreID",
    "f.Item": "f.GDS_CD",
    "f.Product": "f.GDS_CD",
}


def _replace_expr(expr: str) -> str:
    out = expr or ""
    for old, new in CANONICAL_REPLACEMENTS.items():
        out = out.replace(old, new)
    return out


def repair_canonical_columns(plan: Dict[str, Any]) -> Dict[str, Any]:
    plan.setdefault("select", [])
    plan.setdefault("joins", [])
    plan.setdefault("where", [])
    plan.setdefault("group_by", [])
    plan.setdefault("order_by", [])

    for item in plan["select"]:
        if isinstance(item, dict) and item.get("expr"):
            item["expr"] = _replace_expr(item["expr"])

    for join in plan["joins"]:
        if isinstance(join, dict) and join.get("on"):
            join["on"] = _replace_expr(join["on"])

    plan["where"] = [_replace_expr(x) for x in plan["where"] if isinstance(x, str)]
    plan["group_by"] = [_replace_expr(x) for x in plan["group_by"] if isinstance(x, str)]
    plan["order_by"] = [_replace_expr(x) for x in plan["order_by"] if isinstance(x, str)]
    return plan


def force_fact_sales_table(plan: Dict[str, Any], query: str) -> Dict[str, Any]:
    if Intent.is_sales(query) or Intent.is_store_query(query) or Intent.is_product_query(query):
        plan["fact_table"] = f"{CLICKHOUSE_DATABASE}.Cluster_Main_Sales"
    return plan


def drop_suspicious_joins(plan: Dict[str, Any], query: str) -> Dict[str, Any]:
    safe_joins = []

    for j in plan.get("joins", []):
        if not isinstance(j, dict):
            continue

        table_name = (j.get("table") or "").split(".")[-1]

        if table_name == "Dimension_IM":
            safe_joins.append(j)
            continue

        if Intent.is_sales(query) and not Intent.wants_name(query):
            continue

        safe_joins.append(j)

    plan["joins"] = safe_joins
    return plan


def find_dim_im_join(plan: Dict[str, Any]) -> Dict[str, Any] | None:
    for j in plan.get("joins", []):
        table_name = (j.get("table") or "").split(".")[-1]
        if table_name == "Dimension_IM":
            return j
    return None


def ensure_product_name_join(plan: Dict[str, Any], query: str) -> Dict[str, Any]:
    if not Intent.wants_name(query):
        return plan

    plan.setdefault("select", [])
    plan.setdefault("joins", [])
    plan.setdefault("group_by", [])
    plan.setdefault("order_by", [])
    plan.setdefault("limit", 50)

    dim_join = find_dim_im_join(plan)
    if not dim_join:
        alias = f"d{len(plan['joins']) + 1}"
        dim_join = {
            "type": "LEFT",
            "table": f"{CLICKHOUSE_DATABASE}.Dimension_IM",
            "alias": alias,
            "on": f"f.GDS_CD = {alias}.GDS_CD",
        }
        plan["joins"].append(dim_join)

    alias = dim_join.get("alias") or "d1"

    has_name = any(
        isinstance(x, dict)
        and ("GDS_NM" in (x.get("expr") or "") or x.get("as") in ("product_name", "item_name"))
        for x in plan["select"]
    )
    if not has_name:
        plan["select"].insert(0, {"expr": f"{alias}.GDS_NM", "as": "product_name"})

    if Intent.is_most_sold(query) or Intent.is_top_product(query):
        has_qty = any(
            isinstance(x, dict) and "SoldQty" in (x.get("expr") or "")
            for x in plan["select"]
        )
        if not has_qty:
            plan["select"].append({"expr": "sum(f.SoldQty)", "as": "total_qty"})

        has_sales = any(
            isinstance(x, dict) and "NetSale" in (x.get("expr") or "")
            for x in plan["select"]
        )
        if not has_sales:
            plan["select"].append({"expr": "sum(f.NetSale)", "as": "total_net_sales"})

        if f"{alias}.GDS_NM" not in plan["group_by"]:
            plan["group_by"].insert(0, f"{alias}.GDS_NM")

        plan["order_by"] = ["total_qty DESC", "total_net_sales DESC"]
        plan["limit"] = 1

    return plan


def inject_name_join_from_registry(
    plan: Dict[str, Any],
    candidates: List[Any],
    rel_filtered: List[Dict[str, Any]],
    query: str,
) -> Dict[str, Any]:
    if not Intent.wants_name(query):
        return plan

    if find_dim_im_join(plan):
        return plan

    fact_full = (plan.get("fact_table") or "").strip()
    fact = fact_full.split()[0].split(".")[-1] if fact_full else candidates[0].table

    name_cols = [r for r in rel_filtered if r.get("type") == "name_column"]
    join_keys = [r for r in rel_filtered if r.get("type") == "join_key"]

    if not name_cols:
        return plan

    target = name_cols[0]
    dim_tbl = target["table"]
    name_col = target["name_column"]

    matched_join = None
    for rel in join_keys:
        lt, lc = rel["left"].split(".", 1)
        rt, rc = rel["right"].split(".", 1)
        if (lt == fact and rt == dim_tbl) or (rt == fact and lt == dim_tbl):
            matched_join = rel
            break

    if not matched_join:
        return plan

    plan.setdefault("joins", [])
    if not plan["joins"]:
        alias = "d1"
        lt, lc = matched_join["left"].split(".", 1)
        rt, rc = matched_join["right"].split(".", 1)

        if lt == fact:
            on = f"f.{lc} = {alias}.{rc}"
        else:
            on = f"f.{rc} = {alias}.{lc}"

        plan["joins"].append(
            {
                "type": "LEFT",
                "table": normalize_table_ref(dim_tbl),
                "alias": alias,
                "on": on,
            }
        )

    plan.setdefault("select", [])
    plan.setdefault("group_by", [])

    if not any(
        isinstance(x, dict) and x.get("as") in ("item_name", "product_name")
        for x in plan["select"]
    ):
        plan["select"].insert(0, {"expr": "d1." + name_col, "as": "item_name"})

    if f"d1.{name_col}" not in plan["group_by"]:
        plan["group_by"].insert(0, f"d1.{name_col}")

    return plan