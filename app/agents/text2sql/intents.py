import re
from typing import Iterable, List, Optional, Set

MIXED_WORD_MAP = {
    # -----------------------------
    # time / period
    # -----------------------------
    "onii": "оны",
    "jiliin": "жилийн",
    "jil": "жил",
    "year": "жил",
    "years": "жилүүд",
    "sar": "сар",
    "sariin": "сарын",
    "month": "сар",
    "monthly": "сар бүр",
    "uliral": "улирал",
    "quarter": "улирал",
    "q1": "q1",
    "q2": "q2",
    "q3": "q3",
    "q4": "q4",

    # -----------------------------
    # aggregation / math
    # -----------------------------
    "niit": "нийт",
    "niiit": "нийт",
    "sum": "sum",
    "dun": "дүн",
    "hemjee": "хэмжээ",
    "too": "тоо",
    "shirheg": "ширхэг",
    "qty": "quantity",
    "quantity": "quantity",
    "count": "count",
    "avg": "average",
    "average": "average",
    "dundaj": "дундаж",
    "max": "max",
    "min": "min",

    # -----------------------------
    # sales
    # -----------------------------
    "borluulalt": "борлуулалт",
    "borluulaltiin": "борлуулалтын",
    "borluulalttai": "борлуулалттай",
    "orlogo": "орлого",
    "sales": "sales",
    "revenue": "орлого",
    "netsale": "netsale",
    "grosssale": "grosssale",
    "soldqty": "soldqty",
    "discount": "discount",
    "tax": "tax",

    # -----------------------------
    # store
    # -----------------------------
    "salbar": "салбар",
    "salbaraar": "салбараар",
    "delguur": "дэлгүүр",
    "delguureer": "дэлгүүрээр",
    "branch": "branch",
    "branches": "branch",
    "store": "store",
    "stores": "store",

    # -----------------------------
    # product
    # -----------------------------
    "baraa": "бараа",
    "baraanii": "барааны",
    "buteegdehuun": "бүтээгдэхүүн",
    "buteegdehuunii": "бүтээгдэхүүний",
    "product": "product",
    "products": "product",
    "item": "item",
    "items": "item",
    "sku": "sku",
    "gds": "gds",

    # -----------------------------
    # product attributes
    # -----------------------------
    "category": "category",
    "categories": "category",
    "angilal": "ангилал",
    "brand": "brand",
    "brands": "brand",
    "vendor": "vendor",
    "supplier": "supplier",
    "suppliers": "supplier",

    # -----------------------------
    # inventory
    # -----------------------------
    "inventory": "inventory",
    "stock": "stock",
    "stocks": "stock",
    "onhand": "on hand",
    "on-hand": "on hand",
    "uldegdel": "үлдэгдэл",
    "aguulah": "агуулах",
    "warehouse": "warehouse",

    # -----------------------------
    # promo
    # -----------------------------
    "promo": "promotion",
    "promotion": "promotion",
    "campaign": "campaign",
    "campaigns": "campaign",
    "hyamdral": "хямдрал",

    # -----------------------------
    # comparison / ranking
    # -----------------------------
    "hamgiin": "хамгийн",
    "ih": "их",
    "baga": "бага",
    "top": "top",
    "bottom": "bottom",
    "highest": "хамгийн их",
    "lowest": "хамгийн бага",
    "most": "хамгийн их",
    "least": "хамгийн бага",
    "best": "хамгийн их",
    "worst": "хамгийн бага",
    "haritsuulah": "харьцуулах",
    "haritsuul": "харьцуул",
    "compare": "compare",
    "comparison": "compare",
    "vs": "vs",
    "growth": "өсөлт",
    "osolt": "өсөлт",
    "increase": "өсөлт",
    "decrease": "бууралт",
    "buuralt": "бууралт",
    "percent": "percent",
    "huvi": "хувь",
    "huv": "хувь",
    "pct": "percent",
    "yoy": "yoy",
    "mom": "mom",

    # -----------------------------
    # sold
    # -----------------------------
    "zarsan": "зарагдсан",
    "zaragdsan": "зарагдсан",
    "sold": "зарагдсан",

    # -----------------------------
    # question words
    # -----------------------------
    "ner": "нэр",
    "name": "name",
    "names": "name",
    "yu": "юу",
    "ali": "аль",
    "hed": "хэд",
    "hezee": "хэзээ",
    "haana": "хаана",
    "which": "аль",
    "what": "юу",
    "when": "хэзээ",
    "where": "хаана",

    # -----------------------------
    # table / schema
    # -----------------------------
    "table": "table",
    "tables": "table",
    "schema": "schema",
    "column": "column",
    "columns": "column",
}


def normalize_query(query: str) -> str:
    text = (query or "").strip().lower()

    if not text:
        return ""

    # punctuation clean, keep % _ -
    text = re.sub(r"[^\w\s%\-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()

    tokens = text.split()
    normalized = [MIXED_WORD_MAP.get(tok, tok) for tok in tokens]
    text = " ".join(normalized)

    # extra normalization
    text = text.replace("  ", " ").strip()
    return text


def ql(query: str) -> str:
    return normalize_query(query)


def _keyword_pattern(keyword: str) -> str:
    """
    Multi-word keyword бол substring байдлаар,
    single word keyword бол word-boundary ашиглаж шалгана.
    """
    kw = re.escape(keyword.strip().lower())
    if " " in keyword.strip():
        return kw
    return rf"(?<!\w){kw}(?!\w)"


def has_any(text: str, keywords: Iterable[str]) -> bool:
    txt = ql(text)
    for kw in keywords:
        if not kw:
            continue
        if re.search(_keyword_pattern(kw), txt, flags=re.IGNORECASE):
            return True
    return False


def has_all(text: str, keywords: Iterable[str]) -> bool:
    txt = ql(text)
    for kw in keywords:
        if not kw:
            continue
        if not re.search(_keyword_pattern(kw), txt, flags=re.IGNORECASE):
            return False
    return True


def extract_years(query: str) -> List[int]:
    years = re.findall(r"\b(20\d{2})\b", query or "")
    return sorted({int(y) for y in years})


def extract_year(query: str) -> Optional[int]:
    years = extract_years(query)
    return years[0] if years else None


def extract_quarter(query: str) -> Optional[int]:
    text = ql(query)
    patterns = [
        r"\bq([1-4])\b",
        r"\b([1-4])\s*[-]?\s*р\s*улирал\b",
        r"\b([1-4])\s*улирал\b",
        r"\b([1-4])-р улирал\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            q = int(m.group(1))
            if q in (1, 2, 3, 4):
                return q
    return None


def extract_month(query: str) -> Optional[int]:
    text = ql(query)

    month_name_map = {
        "1 сар": 1,
        "01 сар": 1,
        "january": 1,
        "jan": 1,
        "2 сар": 2,
        "02 сар": 2,
        "february": 2,
        "feb": 2,
        "3 сар": 3,
        "03 сар": 3,
        "march": 3,
        "mar": 3,
        "4 сар": 4,
        "04 сар": 4,
        "april": 4,
        "apr": 4,
        "5 сар": 5,
        "05 сар": 5,
        "may": 5,
        "6 сар": 6,
        "06 сар": 6,
        "june": 6,
        "jun": 6,
        "7 сар": 7,
        "07 сар": 7,
        "july": 7,
        "jul": 7,
        "8 сар": 8,
        "08 сар": 8,
        "august": 8,
        "aug": 8,
        "9 сар": 9,
        "09 сар": 9,
        "september": 9,
        "sep": 9,
        "10 сар": 10,
        "october": 10,
        "oct": 10,
        "11 сар": 11,
        "november": 11,
        "nov": 11,
        "12 сар": 12,
        "december": 12,
        "dec": 12,
    }

    for k, v in month_name_map.items():
        if k in text:
            return v

    m = re.search(r"\b(1[0-2]|0?[1-9])\s*сар\b", text)
    if m:
        return int(m.group(1))

    return None


class Intent:
    # -----------------------------
    # entity keywords
    # -----------------------------
    STORE_WORDS: Set[str] = {
        "салбар", "салбараар", "дэлгүүр", "дэлгүүрээр", "store", "branch"
    }

    PRODUCT_WORDS: Set[str] = {
        "бараа", "барааны", "бүтээгдэхүүн", "бүтээгдэхүүний",
        "product", "item", "sku", "gds"
    }

    SALES_WORDS: Set[str] = {
        "борлуулалт", "борлуулалтын", "борлуулалттай",
        "орлого", "sales", "revenue", "netsale", "grosssale", "дүн"
    }

    INVENTORY_WORDS: Set[str] = {
        "stock", "inventory", "үлдэгдэл", "агуулах", "on hand", "warehouse"
    }

    PROMOTION_WORDS: Set[str] = {
        "promotion", "promo", "campaign", "хямдрал"
    }

    CATEGORY_WORDS: Set[str] = {
        "category", "ангилал", "төрөл"
    }

    BRAND_WORDS: Set[str] = {
        "brand", "брэнд"
    }

    SUPPLIER_WORDS: Set[str] = {
        "supplier", "vendor", "нийлүүлэгч"
    }

    # -----------------------------
    # question / measure keywords
    # -----------------------------
    NAME_WORDS: Set[str] = {
        "нэр", "name", "product name", "item name",
        "барааны нэр", "бүтээгдэхүүний нэр", "store name", "branch name"
    }

    TOTAL_WORDS: Set[str] = {
        "нийт", "total", "sum", "нийлбэр"
    }

    QTY_WORDS: Set[str] = {
        "ширхэг", "тоо", "quantity", "soldqty",
        "борлуулсан тоо", "зарагдсан ширхэг", "count"
    }

    AVG_WORDS: Set[str] = {
        "average", "avg", "дундаж"
    }

    MAX_WORDS: Set[str] = {
        "max", "хамгийн их", "highest", "most"
    }

    MIN_WORDS: Set[str] = {
        "min", "хамгийн бага", "lowest", "least"
    }

    MONTH_WORDS: Set[str] = {
        "сар", "сарын", "сар бүр", "monthly", "month", "trend", "тренд"
    }

    QUARTER_WORDS: Set[str] = {
        "улирал", "quarter", "q1", "q2", "q3", "q4"
    }

    TOP_WORDS: Set[str] = {
        "хамгийн их", "top", "өндөр", "best", "highest", "most"
    }

    BOTTOM_WORDS: Set[str] = {
        "хамгийн бага", "bottom", "бага", "worst", "lowest", "least"
    }

    COMPARE_WORDS: Set[str] = {
        "харьцуулах", "харьцуул", "compare", "vs", "ялгаа", "difference"
    }

    GROWTH_WORDS: Set[str] = {
        "өсөлт", "өссөн", "growth", "increase", "бууралт", "decrease", "yoy", "mom"
    }

    PERCENT_WORDS: Set[str] = {
        "хувь", "%", "percent", "pct"
    }

    MOST_SOLD_WORDS: Set[str] = {
        "most sold", "их зарагдсан", "хамгийн их борлуулалттай", "best selling", "зарагдсан"
    }

    TABLE_WORDS: Set[str] = {
        "table", "schema", "хүснэгт", "ямар table", "аль table",
        "ямар хүснэгт", "column", "columns"
    }

    ABOUT_WORDS: Set[str] = {
        "ямар дата", "ямар мэдээлэл", "юу байдаг", "ямар багана",
        "тайлбар", "about", "what data", "what is in", "columns"
    }

    # -----------------------------
    # helpers
    # -----------------------------
    @staticmethod
    def _q(query: str) -> str:
        return ql(query)

    @staticmethod
    def _has(query: str, keywords: Iterable[str]) -> bool:
        return has_any(query, keywords)

    @staticmethod
    def _has_all(query: str, keywords: Iterable[str]) -> bool:
        return has_all(query, keywords)

    # -----------------------------
    # grouping
    # -----------------------------
    @staticmethod
    def wants_group_store(query: str) -> bool:
        return has_any(query, [
            "дэлгүүрээр", "салбараар", "салбар тус бүр",
            "per store", "by store", "store by", "branch by"
        ])

    @staticmethod
    def wants_group_product(query: str) -> bool:
        return has_any(query, [
            "бараагаар", "бүтээгдэхүүнээр",
            "per product", "by product", "product by", "item by", "sku by"
        ])

    @staticmethod
    def wants_group_category(query: str) -> bool:
        return has_any(query, [
            "ангиллаар", "category by", "by category", "per category"
        ])

    @staticmethod
    def wants_group_brand(query: str) -> bool:
        return has_any(query, [
            "брэндээр", "brand by", "by brand", "per brand"
        ])

    @staticmethod
    def wants_name(query: str) -> bool:
        return has_any(query, Intent.NAME_WORDS)

    @staticmethod
    def wants_total(query: str) -> bool:
        return has_any(query, Intent.TOTAL_WORDS)

    @staticmethod
    def wants_qty(query: str) -> bool:
        return has_any(query, Intent.QTY_WORDS)

    @staticmethod
    def wants_avg(query: str) -> bool:
        return has_any(query, Intent.AVG_WORDS)

    @staticmethod
    def wants_max(query: str) -> bool:
        return has_any(query, Intent.MAX_WORDS)

    @staticmethod
    def wants_min(query: str) -> bool:
        return has_any(query, Intent.MIN_WORDS)

    @staticmethod
    def wants_percentage(query: str) -> bool:
        return has_any(query, Intent.PERCENT_WORDS)

    @staticmethod
    def wants_compare(query: str) -> bool:
        return has_any(query, Intent.COMPARE_WORDS)

    @staticmethod
    def wants_growth(query: str) -> bool:
        return has_any(query, Intent.GROWTH_WORDS)

    # -----------------------------
    # domain detection
    # -----------------------------
    @staticmethod
    def is_sales(query: str) -> bool:
        return has_any(query, Intent.SALES_WORDS)

    @staticmethod
    def is_monthly(query: str) -> bool:
        return has_any(query, Intent.MONTH_WORDS) or extract_month(query) is not None

    @staticmethod
    def is_quarter(query: str) -> bool:
        return has_any(query, Intent.QUARTER_WORDS) or extract_quarter(query) is not None

    @staticmethod
    def is_store_query(query: str) -> bool:
        return has_any(query, Intent.STORE_WORDS)

    @staticmethod
    def is_product_query(query: str) -> bool:
        return has_any(query, Intent.PRODUCT_WORDS)

    @staticmethod
    def is_inventory_query(query: str) -> bool:
        return has_any(query, Intent.INVENTORY_WORDS)

    @staticmethod
    def is_promotion_query(query: str) -> bool:
        return has_any(query, Intent.PROMOTION_WORDS)

    @staticmethod
    def is_category_query(query: str) -> bool:
        return has_any(query, Intent.CATEGORY_WORDS)

    @staticmethod
    def is_brand_query(query: str) -> bool:
        return has_any(query, Intent.BRAND_WORDS)

    @staticmethod
    def is_supplier_query(query: str) -> bool:
        return has_any(query, Intent.SUPPLIER_WORDS)

    @staticmethod
    def is_table_question(query: str) -> bool:
        return has_any(query, Intent.TABLE_WORDS)

    @staticmethod
    def is_about_question(query: str) -> bool:
        return has_any(query, Intent.ABOUT_WORDS)

    # -----------------------------
    # ranking
    # -----------------------------
    @staticmethod
    def is_top_store(query: str) -> bool:
        return Intent.is_store_query(query) and has_any(query, Intent.TOP_WORDS)

    @staticmethod
    def is_bottom_store(query: str) -> bool:
        return Intent.is_store_query(query) and has_any(query, Intent.BOTTOM_WORDS)

    @staticmethod
    def is_top_product(query: str) -> bool:
        return Intent.is_product_query(query) and has_any(query, Intent.TOP_WORDS)

    @staticmethod
    def is_bottom_product(query: str) -> bool:
        return Intent.is_product_query(query) and has_any(query, Intent.BOTTOM_WORDS)

    @staticmethod
    def is_top_category(query: str) -> bool:
        return Intent.is_category_query(query) and has_any(query, Intent.TOP_WORDS)

    @staticmethod
    def is_bottom_category(query: str) -> bool:
        return Intent.is_category_query(query) and has_any(query, Intent.BOTTOM_WORDS)

    @staticmethod
    def is_most_sold(query: str) -> bool:
        return has_any(query, Intent.MOST_SOLD_WORDS)

    # -----------------------------
    # period filters
    # -----------------------------
    @staticmethod
    def wants_month_filter(query: str) -> bool:
        return extract_month(query) is not None

    @staticmethod
    def wants_year_filter(query: str) -> bool:
        return extract_year(query) is not None

    @staticmethod
    def wants_quarter_filter(query: str) -> bool:
        return extract_quarter(query) is not None

    # -----------------------------
    # compare / growth
    # -----------------------------
    @staticmethod
    def wants_yoy_growth(query: str) -> bool:
        q = ql(query)
        return (
                ("yoy" in q or "өсөлт" in q or "compare" in q or "харьцуул" in q or "харьцуулах" in q)
                and has_any(q, Intent.PERCENT_WORDS)
                and Intent.is_sales(q)
        )

    @staticmethod
    def wants_mom_growth(query: str) -> bool:
        q = ql(query)
        return (
                "mom" in q
                or ("сар" in q and has_any(q, Intent.GROWTH_WORDS))
        )

    # -----------------------------
    # master data
    # -----------------------------
    @staticmethod
    def is_master_data_query(query: str) -> bool:
        q = ql(query)
        return (
                Intent.is_store_query(q)
                or Intent.is_product_query(q)
                or Intent.is_category_query(q)
                or Intent.is_brand_query(q)
                or Intent.is_supplier_query(q)
        ) and not Intent.is_sales(q)

    # -----------------------------
    # domain inference
    # -----------------------------
    @staticmethod
    def infer_domain(query: str) -> str:
        q = ql(query)

        if Intent.is_table_question(q) or Intent.is_about_question(q):
            return "schema"

        if Intent.is_sales(q):
            if Intent.is_store_query(q):
                return "store_sales"
            if Intent.is_product_query(q):
                return "product_sales"
            if Intent.is_category_query(q):
                return "category_sales"
            if Intent.is_brand_query(q):
                return "brand_sales"
            if Intent.is_supplier_query(q):
                return "supplier_sales"
            return "sales"

        if Intent.is_inventory_query(q):
            if Intent.is_product_query(q):
                return "product_inventory"
            if Intent.is_store_query(q):
                return "store_inventory"
            return "inventory"

        if Intent.is_promotion_query(q):
            return "promotion"

        if Intent.is_supplier_query(q):
            return "supplier"

        if Intent.is_brand_query(q):
            return "brand"

        if Intent.is_category_query(q):
            return "category"

        if Intent.is_product_query(q):
            return "product_master"

        if Intent.is_store_query(q):
            return "store_master"

        return "unknown"

    # -----------------------------
    # utility
    # -----------------------------
    @staticmethod
    def summarize(query: str) -> dict:
        """
        Debug / planner-д ашиглахад тохиромжтой.
        """
        return {
            "normalized": ql(query),
            "domain": Intent.infer_domain(query),
            "is_sales": Intent.is_sales(query),
            "is_store_query": Intent.is_store_query(query),
            "is_product_query": Intent.is_product_query(query),
            "is_inventory_query": Intent.is_inventory_query(query),
            "is_promotion_query": Intent.is_promotion_query(query),
            "wants_group_store": Intent.wants_group_store(query),
            "wants_group_product": Intent.wants_group_product(query),
            "wants_name": Intent.wants_name(query),
            "wants_total": Intent.wants_total(query),
            "wants_qty": Intent.wants_qty(query),
            "wants_compare": Intent.wants_compare(query),
            "wants_growth": Intent.wants_growth(query),
            "wants_percentage": Intent.wants_percentage(query),
            "year": extract_year(query),
            "years": extract_years(query),
            "month": extract_month(query),
            "quarter": extract_quarter(query),
        }
