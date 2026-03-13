# app/agents/text2sql/intents.py
import re
from typing import List, Optional


MIXED_WORD_MAP = {
    "onii": "оны",
    "onii": "оны",
    "jiliin": "жилийн",
    "jil": "жил",
    "niit": "нийт",
    "niiit": "нийт",
    "borluulalt": "борлуулалт",
    "borluulaltiin": "борлуулалтын",
    "orlogo": "орлого",
    "salbar": "салбар",
    "delguur": "дэлгүүр",
    "store": "store",
    "baraa": "бараа",
    "buteegdehuun": "бүтээгдэхүүн",
    "product": "product",
    "item": "item",
    "sar": "сар",
    "sariin": "сарын",
    "monthly": "monthly",
    "uliral": "улирал",
    "q1": "q1",
    "q2": "q2",
    "q3": "q3",
    "q4": "q4",
    "hamgiin": "хамгийн",
    "ih": "их",
    "baga": "бага",
    "top": "top",
    "bottom": "bottom",
    "haritsuulah": "харьцуулах",
    "haritsuul": "харьцуул",
    "compare": "compare",
    "vs": "vs",
    "huvi": "хувь",
    "huv": "хувь",
    "growth": "өсөлт",
    "osolt": "өсөлт",
    "zarsan": "зарагдсан",
    "zaragdsan": "зарагдсан",
    "shirheg": "ширхэг",
    "too": "тоо",
    "quantity": "quantity",
    "soldqty": "soldqty",
    "ner": "нэр",
    "name": "name",
    "yu": "юу",
    "ali": "аль",
}


def normalize_query(query: str) -> str:
    text = (query or "").strip().lower()

    # punctuation normalize
    text = re.sub(r"[^\w\s%]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return text

    tokens = text.split()
    normalized_tokens = [MIXED_WORD_MAP.get(tok, tok) for tok in tokens]
    return " ".join(normalized_tokens)


def ql(query: str) -> str:
    return normalize_query(query)


def has_any(text: str, keywords: List[str]) -> bool:
    return any(k in text for k in keywords)


def extract_years(query: str) -> List[int]:
    years = re.findall(r"\b(20\d{2})\b", query or "")
    return sorted({int(y) for y in years})


def extract_year(query: str) -> Optional[int]:
    years = extract_years(query)
    return years[0] if years else None


def extract_quarter(query: str) -> Optional[int]:
    text = ql(query)
    patterns = [
        r"(\d)\s*[-]?\s*р\s*улирал",
        r"\bq([1-4])\b",
        r"\b([1-4])\b.*улирал",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            q = int(m.group(1))
            if q in (1, 2, 3, 4):
                return q
    return None


class Intent:
    STORE_WORDS = ["салбар", "дэлгүүр", "store"]
    PRODUCT_WORDS = ["бараа", "product", "item", "sku", "бүтээгдэхүүн"]
    SALES_WORDS = ["борлуулалт", "орлого", "sales", "netsale", "grosssale"]
    NAME_WORDS = ["нэр", "name", "product name", "item name", "барааны нэр", "бүтээгдэхүүний нэр"]
    TOTAL_WORDS = ["нийт", "total", "sum"]
    QTY_WORDS = ["ширхэг", "тоо", "quantity", "soldqty", "борлуулсан тоо", "зарагдсан ширхэг"]
    MONTH_WORDS = ["сар", "сарын", "сар бүр", "monthly", "month", "тренд", "trend"]
    QUARTER_WORDS = ["улирал", "quarter", "q1", "q2", "q3", "q4"]
    TOP_WORDS = ["хамгийн их", "top", "их"]
    BOTTOM_WORDS = ["хамгийн бага", "bottom", "бага"]
    MOST_SOLD_WORDS = ["хамгийн их", "most sold", "их зарагдсан", "хамгийн их борлуулалттай"]
    YOY_COMPARE_WORDS = ["харьцуулах", "харьцуул", "vs", "өнгөрсөн", "өссөн", "өсөлт", "compare", "how much increase"]
    PERCENT_WORDS = ["хувь", "%", "percent"]

    @staticmethod
    def wants_group_store(query: str) -> bool:
        return has_any(ql(query), ["дэлгүүрээр", "салбараар", "салбар тус бүр", "store by", "per store"])

    @staticmethod
    def wants_name(query: str) -> bool:
        return has_any(ql(query), Intent.NAME_WORDS)

    @staticmethod
    def is_sales(query: str) -> bool:
        return has_any(ql(query), Intent.SALES_WORDS)

    @staticmethod
    def is_monthly(query: str) -> bool:
        return has_any(ql(query), Intent.MONTH_WORDS)

    @staticmethod
    def is_quarter(query: str) -> bool:
        return has_any(ql(query), Intent.QUARTER_WORDS)

    @staticmethod
    def is_store_query(query: str) -> bool:
        return has_any(ql(query), Intent.STORE_WORDS)

    @staticmethod
    def is_product_query(query: str) -> bool:
        return has_any(ql(query), Intent.PRODUCT_WORDS)

    @staticmethod
    def is_top_store(query: str) -> bool:
        q = ql(query)
        return has_any(q, Intent.STORE_WORDS) and has_any(q, Intent.TOP_WORDS)

    @staticmethod
    def is_bottom_store(query: str) -> bool:
        q = ql(query)
        return has_any(q, Intent.STORE_WORDS) and has_any(q, Intent.BOTTOM_WORDS)

    @staticmethod
    def is_top_product(query: str) -> bool:
        q = ql(query)
        return has_any(q, Intent.PRODUCT_WORDS) and has_any(q, Intent.TOP_WORDS)

    @staticmethod
    def is_most_sold(query: str) -> bool:
        return has_any(ql(query), Intent.MOST_SOLD_WORDS)

    @staticmethod
    def wants_total(query: str) -> bool:
        return has_any(ql(query), Intent.TOTAL_WORDS)

    @staticmethod
    def wants_qty(query: str) -> bool:
        return has_any(ql(query), Intent.QTY_WORDS)

    @staticmethod
    def wants_yoy_growth(query: str) -> bool:
        q = ql(query)
        return (
            has_any(q, Intent.YOY_COMPARE_WORDS)
            and has_any(q, Intent.PERCENT_WORDS)
            and Intent.is_sales(query)
        )