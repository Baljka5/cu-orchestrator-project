from typing import Dict

from app.agents.text2sql.intents import (
    ql,
    Intent,
)


def classify_query_domain(query: str) -> Dict[str, str]:
    """
    Central router for query classification.

    Priority:
    1. Intent-based domain inference (primary)
    2. Keyword fallback (safety)
    """

    text = ql(query)

    # -----------------------------
    # 1. intent-based routing (MAIN)
    # -----------------------------
    domain = Intent.infer_domain(query)

    # -----------------------------
    # 2. fallback (only if unknown)
    # -----------------------------
    if domain == "unknown":

        if any(k in text for k in ["борлуул", "sales", "netsale", "grosssale", "soldqty", "орлого"]):
            domain = "sales"

        elif any(k in text for k in ["stock", "inventory", "үлдэгдэл", "агуулах", "on hand"]):
            domain = "inventory"

        elif any(k in text for k in [
            "барааны ангилал", "category", "product master", "item master", "бүтээгдэхүүний мэдээлэл"
        ]):
            domain = "product_master"

        elif any(k in text for k in [
            "store master", "салбарын мэдээлэл", "дэлгүүрийн мэдээлэл", "branch info"
        ]):
            domain = "store_master"

        elif any(k in text for k in ["promo", "promotion", "campaign", "хямдрал"]):
            domain = "promotion"

    # -----------------------------
    # 3. intent extraction (extra)
    # -----------------------------
    intent = "unknown"

    if Intent.is_top_store(query):
        intent = "top_store"

    elif Intent.is_top_product(query):
        intent = "top_product"

    elif Intent.is_bottom_store(query):
        intent = "bottom_store"

    elif Intent.is_bottom_product(query):
        intent = "bottom_product"

    elif Intent.wants_compare(query):
        intent = "compare"

    elif Intent.wants_yoy_growth(query):
        intent = "yoy_growth"

    elif Intent.wants_mom_growth(query):
        intent = "mom_growth"

    elif Intent.wants_total(query):
        intent = "total"

    elif Intent.wants_avg(query):
        intent = "average"

    elif Intent.wants_qty(query):
        intent = "quantity"

    # -----------------------------
    # 4. final response
    # -----------------------------
    return {
        "domain": domain,
        "intent": intent,
    }
