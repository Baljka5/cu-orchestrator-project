from typing import Dict, List, Tuple, Any

from app.config import SCHEMA_DICT_PATH
from app.core.schema_registry import SchemaRegistry

registry = SchemaRegistry(SCHEMA_DICT_PATH)
registry.load()

CANONICAL_TERMS = {
    "sales_fact": "BI_DB.Cluster_Main_Sales",
    "product_dimension": "BI_DB.Dimension_IM",
    "store_dimension": "BI_DB.Dimension_SM",
    "event_dimension": "BI_DB.Dimension_LEM",
    "event_goods_dimension": "BI_DB.Dimension_LEG",
    "sales_amount": "NetSale",
    "gross_sales": "GrossSale",
    "qty": "SoldQty",
    "date": "SalesDate",
    "store_id": "StoreID",
    "product_code": "GDS_CD",
    "product_name": "Dimension_IM.GDS_NM",
    "store_name": "Dimension_SM.BIZLOC_NM",
    "event_name": "Dimension_LEM.EVT_NM",
}

ROLE_HINTS = {
    "sales_fact": {
        "grain": "1 row = sales transaction/detail grain",
        "recommended_joins": [
            "Cluster_Main_Sales.GDS_CD = Dimension_IM.GDS_CD",
            "Cluster_Main_Sales.StoreID = Dimension_SM.BIZLOC_CD",
            "Cluster_Main_Sales.PromotionID = Dimension_LEM.EVT_CD",
        ],
        "avoid": [
            "Do not invent columns like Store, Product, SalesAmount if canonical columns already exist.",
            "Prefer NetSale, GrossSale, SoldQty, Discount, Tax_VAT.",
        ],
    },
    "product_dimension": {
        "grain": "1 row = 1 product",
        "recommended_joins": [
            "Dimension_IM.GDS_CD = Cluster_Main_Sales.GDS_CD",
        ],
        "avoid": [
            "Use only product descriptive attributes here.",
        ],
    },
    "store_dimension": {
        "grain": "1 row = 1 store/location",
        "recommended_joins": [
            "Dimension_SM.BIZLOC_CD = Cluster_Main_Sales.StoreID",
        ],
        "avoid": [
            "Do not treat event tables as store dimension.",
        ],
    },
    "event_dimension": {
        "grain": "1 row = 1 event/promotion master",
        "recommended_joins": [
            "Dimension_LEM.EVT_CD = Cluster_Main_Sales.PromotionID",
            "Dimension_LEM.EVT_CD = Dimension_LEG.EVT_CD",
        ],
        "avoid": [
            "Do not treat this as store master.",
        ],
    },
    "event_goods_dimension": {
        "grain": "1 row = event-product mapping",
        "recommended_joins": [
            "Dimension_LEG.EVT_CD = Dimension_LEM.EVT_CD",
        ],
        "avoid": [
            "Do not treat this as store master.",
        ],
    },
    "inventory_fact": {
        "grain": "inventory snapshot / stock summary / warehouse stock movement",
        "recommended_joins": [
            "inventory table product key -> Dimension_IM.GDS_CD or ITEM_CD",
            "inventory table store key -> Dimension_SM.BIZLOC_CD where applicable",
        ],
        "avoid": [
            "Do not use inventory facts for sales totals unless explicitly requested.",
        ],
    },
}


def infer_semantic_tags(column_name: str, dtype: str) -> List[str]:
    c = (column_name or "").lower()
    d = (dtype or "").lower()
    tags: List[str] = []

    if any(x in c for x in ["date", "time", "week", "period", "hour", "_ymd", "_dtm", "_dt"]):
        tags.append("date")

    if any(x in c for x in ["id", "cd", "code", "no", "seq"]):
        tags.append("key")

    if any(x in c for x in ["name", "nm"]):
        tags.append("name")

    if any(x in c for x in ["sale", "tax", "discount", "cost", "amt", "amount", "stockqty", "stck_qty", "qty"]):
        tags.append("metric")

    if any(x in c for x in ["qty", "quantity", "cnt", "count"]):
        if "quantity" not in tags:
            tags.append("quantity")

    if "date" in d or "datetime" in d:
        if "date" not in tags:
            tags.append("date")

    return tags


def _find_table(base_name: str):
    base = (base_name or "").split(".")[-1]
    for t in registry.tables:
        if t.table == base:
            return t
    return None


def get_table_info(table_name: str) -> Dict[str, Any] | None:
    t = _find_table(table_name)
    if not t:
        return None

    role = registry.infer_table_role(t)
    highlights = registry.highlights(t)
    role_hint = ROLE_HINTS.get(role, {})

    metrics = {}
    for m in highlights.get("metric_cols", [])[:10]:
        metrics[f"sum_{m.lower()}"] = f"sum({m})"

    info = {
        "db": t.db,
        "table": t.table,
        "entity": role,
        "description": t.description or t.entity or "",
        "grain": role_hint.get("grain", ""),
        "date_column": highlights.get("date_cols", [""])[0] if highlights.get("date_cols") else "",
        "primary_keys": highlights.get("key_cols", [])[:10],
        "name_columns": highlights.get("name_cols", [])[:10],
        "common_metrics": metrics,
        "joins": role_hint.get("recommended_joins", []),
        "avoid": role_hint.get("avoid", []),
        "columns": [(c.name, c.attr or "", c.dtype or "") for c in t.columns[:120]],
    }

    if t.table == "Cluster_Main_Sales":
        info["common_metrics"].update(
            {
                "total_net_sales": "sum(NetSale)",
                "total_gross_sales": "sum(GrossSale)",
                "total_qty": "sum(SoldQty)",
                "vat_sum": "sum(Tax_VAT)",
                "discount_sum": "sum(Discount)",
                "cost_sum": "sum(ActualCost)",
            }
        )
        info["joins"] = [
            "Cluster_Main_Sales.GDS_CD = Dimension_IM.GDS_CD",
            "Cluster_Main_Sales.StoreID = Dimension_SM.BIZLOC_CD",
            "Cluster_Main_Sales.PromotionID = Dimension_LEM.EVT_CD",
        ]

    if t.table == "Dimension_IM":
        info["joins"] = ["Dimension_IM.GDS_CD = Cluster_Main_Sales.GDS_CD"]

    if t.table == "Dimension_SM":
        info["joins"] = ["Dimension_SM.BIZLOC_CD = Cluster_Main_Sales.StoreID"]

    if t.table == "Dimension_LEM":
        info["joins"] = [
            "Dimension_LEM.EVT_CD = Cluster_Main_Sales.PromotionID",
            "Dimension_LEM.EVT_CD = Dimension_LEG.EVT_CD",
        ]

    if t.table == "Dimension_LEG":
        info["joins"] = ["Dimension_LEG.EVT_CD = Dimension_LEM.EVT_CD"]

    return info


def to_prompt_block(table_name: str) -> str:
    info = get_table_info(table_name)
    if not info:
        return ""

    cols = []
    for col_name, desc, dtype in info.get("columns", []):
        tags = infer_semantic_tags(col_name, dtype)
        tag_txt = f" [tags: {', '.join(tags)}]" if tags else ""
        desc_txt = desc if desc else "-"
        dtype_txt = dtype if dtype else "-"
        cols.append(f"- {col_name} ({dtype_txt}): {desc_txt}{tag_txt}")

    joins = info.get("joins", [])
    join_txt = "\n".join([f"- {j}" for j in joins]) if joins else "-"

    metrics = info.get("common_metrics", {})
    metric_txt = "\n".join([f"- {k}: {v}" for k, v in metrics.items()]) if metrics else "-"

    avoids = info.get("avoid", [])
    avoid_txt = "\n".join([f"- {x}" for x in avoids]) if avoids else "-"

    pk_txt = ", ".join(info.get("primary_keys", [])) or "-"
    name_txt = ", ".join(info.get("name_columns", [])) or "-"
    date_txt = info.get("date_column") or "-"
    db_txt = info.get("db", "BI_DB")

    return (
            f"TABLE: {db_txt}.{table_name.split('.')[-1]}\n"
            f"ENTITY: {info.get('entity', '-')}\n"
            f"DESC: {info.get('description', '-')}\n"
            f"GRAIN: {info.get('grain', '-')}\n"
            f"DATE_COLUMN: {date_txt}\n"
            f"PRIMARY_KEYS: {pk_txt}\n"
            f"NAME_COLUMNS: {name_txt}\n"
            f"COMMON_METRICS:\n{metric_txt}\n"
            f"JOINS:\n{join_txt}\n"
            f"AVOID:\n{avoid_txt}\n"
            f"COLUMNS:\n" + "\n".join(cols)
    ).strip()


def format_schema_for_prompt(table_names: List[str]) -> str:
    seen = set()
    parts: List[str] = []

    for t in table_names:
        base = (t or "").split(".")[-1]
        if base in seen:
            continue
        seen.add(base)

        block = to_prompt_block(base)
        if block:
            parts.append(block)

    if not parts:
        return ""

    canonical_txt = "\n".join([f"- {k}: {v}" for k, v in CANONICAL_TERMS.items()])

    return (
            "CANONICAL BUSINESS TERMS:\n"
            f"{canonical_txt}\n\n"
            "STRICT SEMANTIC NOTES:\n"
            "- Dimension_SM is the store master dimension.\n"
            "- Dimension_LEM is the event/promotion master dimension.\n"
            "- Dimension_LEG is event goods master.\n"
            "- Do not use event tables as store master.\n"
            "- For sales totals prefer Cluster_Main_Sales.\n\n"
            "SCHEMA DETAILS:\n\n"
            + "\n\n".join(parts)
    ).strip()
