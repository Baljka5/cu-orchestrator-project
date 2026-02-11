SCHEMA = {
    "Cluster_Main_Sales": {
        "entity": "Sales in BGF",
        "description": "Борлуулалтын дэлгэрэнгүй бичлэг (дэлгүүр/бараа/өдөр/цаг/промо/татвар).",
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
            ("LCLSS_CD", "major classification code", "String"),
            ("MCLSS_CD", "middle classification code", "String"),
            ("SCLSS_CD", "sub-category code", "String"),
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
            ("REPR_VEN_CD", "Representative customer code", "String"),
            ("ActualCost", "Actual cost", "Decimal(9,2)"),
            ("MUST_HAVE_PROD", "Must-have product", "String"),
        ],
        "common_metrics": {
            "total_net_sales": "sum(NetSale)",
            "total_gross_sales": "sum(GrossSale)",
            "total_qty": "sum(SoldQty)",
            "vat_sum": "sum(Tax_VAT)",
            "city_tax_sum": "sum(City_Tax)",
            "discount_sum": "sum(Discount)",
        },
        "date_column": "SalesDate",
    }
}


def format_schema_for_prompt(table_names: list[str]) -> str:
    parts = []
    for t in table_names:
        info = SCHEMA.get(t)
        if not info:
            continue
        cols = "\n".join([f"- {c} ({dt}): {desc}" for c, desc, dt in info["columns"]])
        parts.append(
            f"TABLE: {t}\n"
            f"ENTITY: {info.get('entity', '')}\n"
            f"DESC: {info.get('description', '')}\n"
            f"COLUMNS:\n{cols}\n"
        )
    return "\n".join(parts).strip()
