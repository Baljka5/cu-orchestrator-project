# app/core/schema_catalog.py
from typing import Dict, List, Tuple, Any


SCHEMA: Dict[str, Dict[str, Any]] = {
    "Cluster_Main_Sales": {
        "entity": "sales_fact",
        "description": "Борлуулалтын дэлгэрэнгүй бичлэг (дэлгүүр/бараа/өдөр/цаг/промо/татвар).",
        "grain": "1 мөр = дэлгүүр + бараа + борлуулалтын огноо/цаг орчим түвшин",
        "date_column": "SalesDate",
        "primary_keys": ["StoreID", "GDS_CD", "SalesDate", "ReceiptNO"],
        "name_columns": [],
        "common_metrics": {
            "total_net_sales": "sum(NetSale)",
            "total_gross_sales": "sum(GrossSale)",
            "total_qty": "sum(SoldQty)",
            "vat_sum": "sum(Tax_VAT)",
            "city_tax_sum": "sum(City_Tax)",
            "discount_sum": "sum(Discount)",
        },
        "joins": [
            "Cluster_Main_Sales.GDS_CD = Dimension_IM.GDS_CD",
            "Cluster_Main_Sales.StoreID = Dimension_LEM.StoreID",
        ],
        "columns": [
            ("SalesDate", "Historic date", "Date"),
            ("StoreID", "Store number", "String"),
            ("GDS_CD", "Product Code (Item code)", "String"),
            ("ReceiptNO", "Receipt No", "String"),
            ("GrossSale", "Gross Sale", "Decimal(9,2)"),
            ("NetSale", "Net sale", "Decimal(9,2)"),
            ("Tax_VAT", "VAT", "Decimal(9,2)"),
            ("City_Tax", "City tax", "Decimal(9,2)"),
            ("Discount", "Discount", "Decimal(9,2)"),
            ("SoldQty", "Sold quantity", "Int32/Decimal"),
            ("CATE_CD", "Category code", "String"),
            ("LCLSS_CD", "Major classification code", "String"),
            ("MCLSS_CD", "Middle classification code", "String"),
            ("SCLSS_CD", "Sub-category code", "String"),
            ("STR_FMAT_TP", "Classification of store format", "String"),
            ("BIZLOC_TP", "Location type", "String"),
            ("loc_area", "Location area", "String"),
            ("PRMT_KIND_TP", "Classification of promotion types", "String"),
            ("PromotionID", "Promotion ID", "String"),
            ("SalesHourS", "Sales Hour", "String"),
            ("toHours", "Hour", "Int32"),
            ("SalesWeek", "Sales Week", "String"),
            ("weeknum", "Week number", "String"),
            ("period", "Period", "String"),
            ("PROC_DATE", "Created date (txt file)", "String"),
            ("Order_Type", "Order type", "Nullable(FixedString(1))"),
            ("ITEM_ATTR_TP", "Item attribute classification", "String"),
            ("GDS_TP", "Product type", "String"),
            ("REPR_VEN_CD", "Representative vendor/customer code", "String"),
            ("ActualCost", "Actual cost", "Decimal(9,2)"),
            ("MUST_HAVE_PROD", "Must-have product", "String"),
        ],
    },
    "Dimension_IM": {
        "entity": "product_dimension",
        "description": "Барааны мастер dimension хүснэгт. Барааны нэр, ангилал, кодын мэдээлэл агуулна.",
        "grain": "1 мөр = 1 бараа",
        "date_column": "",
        "primary_keys": ["GDS_CD"],
        "name_columns": ["GDS_NM"],
        "common_metrics": {},
        "joins": [
            "Dimension_IM.GDS_CD = Cluster_Main_Sales.GDS_CD",
        ],
        "columns": [
            ("GDS_CD", "Product code", "String"),
            ("GDS_NM", "Product name", "String"),
            ("CATE_CD", "Category code", "String"),
            ("LCLSS_CD", "Major class code", "String"),
            ("MCLSS_CD", "Middle class code", "String"),
            ("SCLSS_CD", "Sub class code", "String"),
            ("GDS_TP", "Product type", "String"),
            ("ITEM_ATTR_TP", "Item attribute type", "String"),
            ("REPR_VEN_CD", "Representative vendor code", "String"),
        ],
    },
    "Dimension_LEM": {
        "entity": "store_dimension",
        "description": "Store master / location dimension. Салбарын нэр, формат, байршлын мэдээлэл.",
        "grain": "1 мөр = 1 салбар",
        "date_column": "",
        "primary_keys": ["StoreID"],
        "name_columns": ["StoreName"],
        "common_metrics": {},
        "joins": [
            "Dimension_LEM.StoreID = Cluster_Main_Sales.StoreID",
        ],
        "columns": [
            ("StoreID", "Store number", "String"),
            ("StoreName", "Store name", "String"),
            ("STR_FMAT_TP", "Store format type", "String"),
            ("BIZLOC_TP", "Business location type", "String"),
            ("loc_area", "Location area", "String"),
            ("AreaName", "Area name", "String"),
            ("RegionName", "Region name", "String"),
        ],
    },
    "Dimension_LEG": {
        "entity": "store_dimension",
        "description": "Store/location related dimension table. Салбарын нэмэлт ангилал, group мэдээлэл агуулж болно.",
        "grain": "1 мөр = 1 салбар эсвэл 1 location grouping",
        "date_column": "",
        "primary_keys": ["StoreID"],
        "name_columns": ["StoreName"],
        "common_metrics": {},
        "joins": [
            "Dimension_LEG.StoreID = Cluster_Main_Sales.StoreID",
        ],
        "columns": [
            ("StoreID", "Store number", "String"),
            ("StoreName", "Store name", "String"),
            ("Region", "Region", "String"),
            ("Area", "Area", "String"),
            ("StoreType", "Store type", "String"),
        ],
    },
    "agg_sales_2024": {
        "entity": "sales_aggregate",
        "description": "2024 оны агрегат борлуулалтын хүснэгт. Зарим тусгай aggregation эсвэл materialized source байж болно.",
        "grain": "aggregated sales rows",
        "date_column": "SalesDate",
        "primary_keys": ["Store", "Item", "SalesDate"],
        "name_columns": [],
        "common_metrics": {
            "total_net_sales": "sum(NetSale)",
            "total_discount": "sum(Discount)",
        },
        "joins": [],
        "columns": [
            ("SalesDate", "Sales date", "Date"),
            ("Store", "Store code", "String"),
            ("Item", "Item code", "String"),
            ("NetSale", "Net sale", "Decimal"),
            ("Discount", "Discount", "Decimal"),
            ("StoreDay", "Store-day key", "String"),
        ],
    },
    "war_stock_2024_MV": {
        "entity": "inventory_fact",
        "description": "2024 stock / warehouse related materialized view. Нөөц, агуулахын мэдээлэлтэй холбоотой.",
        "grain": "inventory snapshot or movement summary",
        "date_column": "",
        "primary_keys": ["TASK_CENT_CD", "GDS_CD"],
        "name_columns": [],
        "common_metrics": {},
        "joins": [
            "war_stock_2024_MV.TASK_CENT_CD = agg_sales_2024.Store",
            "war_stock_2024_MV.GDS_CD = agg_sales_2024.Item",
        ],
        "columns": [
            ("TASK_CENT_CD", "Store or task center code", "String"),
            ("GDS_CD", "Product code", "String"),
            ("StockQty", "Stock quantity", "Decimal"),
            ("StockAmt", "Stock amount", "Decimal"),
        ],
    },
}


CANONICAL_TERMS = {
    "sales_fact": "Cluster_Main_Sales",
    "product_dimension": "Dimension_IM",
    "store_dimension": "Dimension_LEM",
    "sales_amount": "NetSale",
    "gross_sales": "GrossSale",
    "qty": "SoldQty",
    "date": "SalesDate",
    "store_id": "StoreID",
    "product_code": "GDS_CD",
    "product_name": "Dimension_IM.GDS_NM",
}


def get_table_info(table_name: str) -> Dict[str, Any] | None:
    base = (table_name or "").split(".")[-1]
    return SCHEMA.get(base)


def get_known_tables() -> List[str]:
    return sorted(SCHEMA.keys())


def infer_semantic_tags(column_name: str, dtype: str) -> List[str]:
    c = (column_name or "").lower()
    d = (dtype or "").lower()
    tags: List[str] = []

    if any(x in c for x in ["date", "time", "week", "period", "hour"]):
        tags.append("date")

    if any(x in c for x in ["id", "cd", "code", "no"]):
        tags.append("key")

    if any(x in c for x in ["name", "nm"]):
        tags.append("name")

    if any(x in c for x in ["sale", "tax", "discount", "cost", "amt", "amount"]):
        tags.append("metric")

    if any(x in c for x in ["qty", "quantity", "cnt", "count"]):
        tags.append("quantity")

    if "date" in d or "datetime" in d:
        if "date" not in tags:
            tags.append("date")

    return tags


def to_prompt_block(table_name: str) -> str:
    info = get_table_info(table_name)
    if not info:
        return ""

    cols = []
    for col_name, desc, dtype in info.get("columns", []):
        tags = infer_semantic_tags(col_name, dtype)
        tag_txt = f" [tags: {', '.join(tags)}]" if tags else ""
        cols.append(f"- {col_name} ({dtype}): {desc}{tag_txt}")

    joins = info.get("joins", [])
    join_txt = "\n".join([f"- {j}" for j in joins]) if joins else "-"

    metrics = info.get("common_metrics", {})
    metric_txt = "\n".join([f"- {k}: {v}" for k, v in metrics.items()]) if metrics else "-"

    pk_txt = ", ".join(info.get("primary_keys", [])) or "-"
    name_txt = ", ".join(info.get("name_columns", [])) or "-"
    date_txt = info.get("date_column") or "-"

    return (
        f"TABLE: {table_name.split('.')[-1]}\n"
        f"ENTITY: {info.get('entity', '-')}\n"
        f"DESC: {info.get('description', '-')}\n"
        f"GRAIN: {info.get('grain', '-')}\n"
        f"DATE_COLUMN: {date_txt}\n"
        f"PRIMARY_KEYS: {pk_txt}\n"
        f"NAME_COLUMNS: {name_txt}\n"
        f"COMMON_METRICS:\n{metric_txt}\n"
        f"JOINS:\n{join_txt}\n"
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
        "SCHEMA DETAILS:\n\n"
        + "\n\n".join(parts)
    ).strip()