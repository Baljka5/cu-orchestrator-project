# app/agents/text2sql/intents.py
import re
from typing import List, Optional


def ql(query: str) -> str:
    return (query or "").strip().lower()


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
    SALES_WORDS = ["борлуулалт", "орлого", "sales", "netsale", "grosssale"]
    NAME_WORDS = ["нэр", "name", "product name", "item name", "барааны нэр", "бүтээгдэхүүний нэр"]
    TOTAL_WORDS = ["нийт", "total", "sum"]
    QTY_WORDS = ["ширхэг", "тоо", "quantity", "soldqty", "борлуулсан тоо", "зарагдсан ширхэг"]
    MONTH_WORDS = ["сар", "сар бүр", "monthly", "month", "тренд", "trend"]
    QUARTER_WORDS = ["улирал", "quarter", "q1", "q2", "q3", "q4"]
    TOP_WORDS = ["хамгийн их", "top", "их"]
    BOTTOM_WORDS = ["хамгийн бага", "bottom", "бага"]
    MOST_SOLD_WORDS = ["хамгийн их", "most sold", "их зарагдсан"]
    YOY_COMPARE_WORDS = ["харьцуулах", "vs", "өнгөрсөн", "өссөн", "өсөлт", "how much increase"]
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
    def is_most_sold(query: str) -> bool:
        return has_any(ql(query), Intent.MOST_SOLD_WORDS)

    @staticmethod
    def is_top_store(query: str) -> bool:
        q = ql(query)
        return has_any(q, Intent.STORE_WORDS) and has_any(q, Intent.TOP_WORDS)

    @staticmethod
    def is_bottom_store(query: str) -> bool:
        q = ql(query)
        return has_any(q, Intent.STORE_WORDS) and has_any(q, Intent.BOTTOM_WORDS)

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