from typing import Dict

from app.agents.text2sql.intents import Intent, ql


def classify_query_domain(query: str) -> Dict[str, str]:
    text = ql(query)

    # ---------------------------------
    # out of domain
    # ---------------------------------
    if Intent.is_out_of_domain(query):
        return {
            "domain": "out_of_domain",
            "reason": "query_is_not_related_to_database_analytics",
        }

    # ---------------------------------
    # sales
    # ---------------------------------
    if Intent.is_sales(query):
        return {
            "domain": "sales",
            "reason": "matched_sales_keywords",
        }

    # ---------------------------------
    # inventory
    # ---------------------------------
    if Intent.is_inventory_query(query):
        return {
            "domain": "inventory",
            "reason": "matched_inventory_keywords",
        }

    # ---------------------------------
    # promotion / event
    # ---------------------------------
    if Intent.is_promotion_query(query):
        return {
            "domain": "promotion",
            "reason": "matched_promotion_keywords",
        }

    # ---------------------------------
    # supplier
    # ---------------------------------
    if Intent.is_supplier_query(query):
        return {
            "domain": "supplier",
            "reason": "matched_supplier_keywords",
        }

    # ---------------------------------
    # brand
    # ---------------------------------
    if Intent.is_brand_query(query):
        return {
            "domain": "brand",
            "reason": "matched_brand_keywords",
        }

    # ---------------------------------
    # category
    # ---------------------------------
    if Intent.is_category_query(query):
        return {
            "domain": "category",
            "reason": "matched_category_keywords",
        }

    # ---------------------------------
    # product master
    # ---------------------------------
    if Intent.is_product_query(query) and not Intent.is_sales(query):
        return {
            "domain": "product_master",
            "reason": "matched_product_keywords_without_sales_intent",
        }

    # ---------------------------------
    # store master
    # ---------------------------------
    if Intent.is_store_query(query) and not Intent.is_sales(query):
        return {
            "domain": "store_master",
            "reason": "matched_store_keywords_without_sales_intent",
        }

    # ---------------------------------
    # schema / table questions
    # ---------------------------------
    if Intent.is_table_question(query) or Intent.is_about_question(query):
        # soft guess using normalized text
        if any(k in text for k in ["борлуул", "sales", "netsale", "grosssale", "soldqty"]):
            return {
                "domain": "sales",
                "reason": "schema_question_about_sales",
            }
        if any(k in text for k in ["stock", "inventory", "үлдэгдэл", "агуулах"]):
            return {
                "domain": "inventory",
                "reason": "schema_question_about_inventory",
            }
        if any(k in text for k in ["product", "item", "бараа", "бүтээгдэхүүн"]):
            return {
                "domain": "product_master",
                "reason": "schema_question_about_product_master",
            }
        if any(k in text for k in ["store", "branch", "дэлгүүр", "салбар"]):
            return {
                "domain": "store_master",
                "reason": "schema_question_about_store_master",
            }
        if any(k in text for k in ["promotion", "campaign", "event", "хямдрал"]):
            return {
                "domain": "promotion",
                "reason": "schema_question_about_promotion",
            }

        return {
            "domain": "unknown",
            "reason": "generic_schema_question_without_clear_domain",
        }

    # ---------------------------------
    # master data generic
    # ---------------------------------
    if Intent.is_master_data_query(query):
        if Intent.is_product_query(query):
            return {
                "domain": "product_master",
                "reason": "generic_master_data_product_query",
            }
        if Intent.is_store_query(query):
            return {
                "domain": "store_master",
                "reason": "generic_master_data_store_query",
            }

    # ---------------------------------
    # recent trend fallback
    # ---------------------------------
    if Intent.is_recent_trend_query(query):
        return {
            "domain": "sales",
            "reason": "recent_trend_queries_default_to_sales",
        }

    # ---------------------------------
    # unknown
    # ---------------------------------
    return {
        "domain": "unknown",
        "reason": "no_clear_domain_match",
    }
