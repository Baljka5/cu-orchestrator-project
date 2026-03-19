from typing import Dict
from app.agents.text2sql.intents import ql


def classify_query_domain(query: str) -> Dict[str, str]:
    text = ql(query)

    if any(k in text for k in ["борлуул", "sales", "netsale", "grosssale", "soldqty", "орлого"]):
        return {"domain": "sales"}

    if any(k in text for k in ["stock", "inventory", "үлдэгдэл", "агуулах", "on hand"]):
        return {"domain": "inventory"}

    if any(k in text for k in
           ["барааны ангилал", "category", "product master", "item master", "бүтээгдэхүүний мэдээлэл"]):
        return {"domain": "product_master"}

    if any(k in text for k in ["store master", "салбарын мэдээлэл", "дэлгүүрийн мэдээлэл", "branch info"]):
        return {"domain": "store_master"}

    if any(k in text for k in ["promo", "promotion", "campaign", "хямдрал"]):
        return {"domain": "promotion"}

    return {"domain": "unknown"}