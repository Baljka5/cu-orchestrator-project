import re
from typing import List, Optional

MIXED_WORD_MAP = {
    # year / time
    "onii": "оны",
    "jiliin": "жилийн",
    "jil": "жил",
    "year": "жил",
    "years": "жилүүд",
    "month": "month",
    "monthly": "monthly",
    "sar": "сар",
    "sariin": "сарын",
    "uliral": "улирал",
    "quarter": "quarter",
    "q1": "q1",
    "q2": "q2",
    "q3": "q3",
    "q4": "q4",

    # total / math
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

    # sales
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

    # store
    "salbar": "салбар",
    "salbaraar": "салбараар",
    "delguur": "дэлгүүр",
    "delguureer": "дэлгүүрээр",
    "branch": "branch",
    "branches": "branch",
    "store": "store",
    "stores": "store",

    # product
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

    # product attributes
    "category": "category",
    "categories": "category",
    "angilal": "ангилал",
    "brand": "brand",
    "brands": "brand",
    "vendor": "vendor",
    "supplier": "supplier",
    "suppliers": "supplier",

    # inventory
    "inventory": "inventory",
    "stock": "stock",
    "stocks": "stock",
    "onhand": "on hand",
    "on-hand": "on hand",
    "uldegdel": "үлдэгдэл",
    "aguulah": "агуулах",
    "warehouse": "warehouse",

    # promo
    "promo": "promotion",
    "promotion": "promotion",
    "campaign": "campaign",
    "campaigns": "campaign",
    "hyamdral": "хямдрал",

    # comparisons
    "hamgiin": "хамгийн",
    "ih": "их",
    "baga": "бага",
    "top": "top",
    "bottom": "bottom",
    "highest": "хамгийн их",
    "lowest": "хамгийн бага",
    "most": "хамгийн их",
    "least": "хамгийн бага",
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

    # ranking / sold
    "zarsan": "зарагдсан",
    "zaragdsan": "зарагдсан",
    "sold": "зарагдсан",
    "best": "хамгийн их",
    "worst": "хамгийн бага",

    # question words
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

    # table / schema questions
    "table": "table",
    "tables": "table",
    "schema": "schema",
    "column": "column",
    "columns": "columns",
}


def normalize_query(query: str) -> str:
    text = (query or "").strip().lower()

    # punctuation цэвэрлэхдээ % болон _ хадгална
    text = re.sub(r"[^\w\s%\-]+", " ", text, flags=re.UNICODE)
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
        r"\b([1-4])\b\s*улирал",
        r"\b([1-4])-р улирал\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            q = int(m.group(1))
            if q in (1, 2, 3, 4):
                return q
    return None


def extract_month(query: str) -> Optional[int]:
    text = ql(query)

    month_name_map = {
        "1 сар": 1, "01 сар": 1, "january": 1, "jan": 1,
        "2 сар": 2, "02 сар": 2, "february": 2, "feb": 2,
        "3 сар": 3, "03 сар": 3, "march": 3, "mar": 3,
        "4 сар": 4, "04 сар": 4, "april": 4, "apr": 4,
        "5 сар": 5, "05 сар": 5, "may": 5,
        "6 сар": 6, "06 сар": 6, "june": 6, "jun": 6,
        "7 сар": 7, "07 сар": 7, "july": 7, "jul": 7,
        "8 сар": 8, "08 сар": 8, "august": 8, "aug": 8,
        "9 сар": 9, "09 сар": 9, "september": 9, "sep": 9,
        "10 сар": 10, "october": 10, "oct": 10,
        "11 сар": 11, "november": 11, "nov": 11,
        "12 сар": 12, "december": 12, "dec": 12,
    }

    for k, v in month_name_map.items():
        if k in text:
            return v

    m = re.search(r"\b(1[0-2]|0?[1-9])\s*сар\b", text)
    if m:
        return int(m.group(1))

    return None


class Intent:
    STORE_WORDS = [
        "салбар", "дэлгүүр", "store", "branch"
    ]

    PRODUCT_WORDS = [
        "бараа", "product", "item", "sku", "бүтээгдэхүүн", "gds"
    ]

    SALES_WORDS = [
        "борлуулалт", "борлуулалтын", "орлого", "sales", "netsale", "grosssale", "дүн", "revenue"
    ]

    NAME_WORDS = [
        "нэр", "name", "product name", "item name",
        "барааны нэр", "бүтээгдэхүүний нэр", "store name", "branch name"
    ]

    TOTAL_WORDS = [
        "нийт", "total", "sum", "нийлбэр"
    ]

    QTY_WORDS = [
        "ширхэг", "тоо", "quantity", "soldqty",
        "борлуулсан тоо", "зарагдсан ширхэг", "count"
    ]

    MONTH_WORDS = [
        "сар", "сарын", "сар бүр", "monthly", "month", "тренд", "trend"
    ]

    QUARTER_WORDS = [
        "улирал", "quarter", "q1", "q2", "q3", "q4"
    ]

    TOP_WORDS = [
        "хамгийн их", "top", "их", "өндөр", "best", "highest", "most"
    ]

    BOTTOM_WORDS = [
        "хамгийн бага", "bottom", "бага", "worst", "lowest", "least"
    ]

    MOST_SOLD_WORDS = [
        "хамгийн их", "most sold", "их зарагдсан", "хамгийн их борлуулалттай", "best selling"
    ]

    YOY_COMPARE_WORDS = [
        "харьцуулах", "харьцуул", "vs", "өнгөрсөн",
        "өссөн", "өсөлт", "compare", "how much increase", "yoy", "growth"
    ]

    PERCENT_WORDS = [
        "хувь", "%", "percent", "pct"
    ]

    INVENTORY_WORDS = [
        "stock", "inventory", "үлдэгдэл", "агуулах", "on hand", "warehouse"
    ]

    PROMOTION_WORDS = [
        "promotion", "promo", "campaign", "хямдрал"
    ]

    CATEGORY_WORDS = [
        "category", "ангилал", "төрөл"
    ]

    BRAND_WORDS = [
        "brand", "брэнд"
    ]

    SUPPLIER_WORDS = [
        "supplier", "vendor", "нийлүүлэгч"
    ]

    TABLE_WORDS = [
        "table", "schema", "хүснэгт", "ямар table", "аль table", "ямар хүснэгт", "columns", "column"
    ]

    ABOUT_WORDS = [
        "ямар дата", "ямар мэдээлэл", "юу байдаг", "ямар багана",
        "тайлбар", "about", "what data", "what is in", "columns"
    ]

    @staticmethod
    def wants_group_store(query: str) -> bool:
        return has_any(
            ql(query),
            ["дэлгүүрээр", "салбараар", "салбар тус бүр", "store by", "per store", "by store", "branch by"]
        )

    @staticmethod
    def wants_group_product(query: str) -> bool:
        return has_any(
            ql(query),
            ["бараагаар", "product by", "per product", "by product", "item by", "sku by"]
        )

    @staticmethod
    def wants_name(query: str) -> bool:
        return has_any(ql(query), Intent.NAME_WORDS)

    @staticmethod
    def wants_total(query: str) -> bool:
        return has_any(ql(query), Intent.TOTAL_WORDS)

    @staticmethod
    def wants_qty(query: str) -> bool:
        return has_any(ql(query), Intent.QTY_WORDS)

    @staticmethod
    def wants_percentage(query: str) -> bool:
        return has_any(ql(query), Intent.PERCENT_WORDS)

    @staticmethod
    def wants_compare(query: str) -> bool:
        return has_any(ql(query), ["харьцуулах", "харьцуул", "compare", "vs", "ялгаа", "difference"])

    @staticmethod
    def wants_growth(query: str) -> bool:
        return has_any(ql(query), ["өсөлт", "өссөн", "growth", "increase", "yoy", "mom"])

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
    def is_inventory_query(query: str) -> bool:
        return has_any(ql(query), Intent.INVENTORY_WORDS)

    @staticmethod
    def is_promotion_query(query: str) -> bool:
        return has_any(ql(query), Intent.PROMOTION_WORDS)

    @staticmethod
    def is_category_query(query: str) -> bool:
        return has_any(ql(query), Intent.CATEGORY_WORDS)

    @staticmethod
    def is_brand_query(query: str) -> bool:
        return has_any(ql(query), Intent.BRAND_WORDS)

    @staticmethod
    def is_supplier_query(query: str) -> bool:
        return has_any(ql(query), Intent.SUPPLIER_WORDS)

    @staticmethod
    def is_table_question(query: str) -> bool:
        return has_any(ql(query), Intent.TABLE_WORDS)

    @staticmethod
    def is_about_question(query: str) -> bool:
        return has_any(ql(query), Intent.ABOUT_WORDS)

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
    def is_bottom_product(query: str) -> bool:
        q = ql(query)
        return has_any(q, Intent.PRODUCT_WORDS) and has_any(q, Intent.BOTTOM_WORDS)

    @staticmethod
    def is_most_sold(query: str) -> bool:
        return has_any(ql(query), Intent.MOST_SOLD_WORDS)

    @staticmethod
    def wants_yoy_growth(query: str) -> bool:
        q = ql(query)
        return (
                has_any(q, Intent.YOY_COMPARE_WORDS)
                and has_any(q, Intent.PERCENT_WORDS)
                and Intent.is_sales(query)
        )

    @staticmethod
    def wants_month_filter(query: str) -> bool:
        return extract_month(query) is not None

    @staticmethod
    def wants_year_filter(query: str) -> bool:
        return extract_year(query) is not None

    @staticmethod
    def wants_quarter_filter(query: str) -> bool:
        return extract_quarter(query) is not None

    @staticmethod
    def is_master_data_query(query: str) -> bool:
        q = ql(query)
        return (
                Intent.is_store_query(q)
                or Intent.is_product_query(q)
                or Intent.is_category_query(q)
                or Intent.is_brand_query(q)
                or Intent.is_supplier_query(q)
        ) and not Intent.is_sales(query)

    @staticmethod
    def infer_domain(query: str) -> str:
        q = ql(query)

        if Intent.is_sales(q):
            return "sales"
        if Intent.is_inventory_query(q):
            return "inventory"
        if Intent.is_promotion_query(q):
            return "promotion"
        if Intent.is_supplier_query(q):
            return "supplier"
        if Intent.is_brand_query(q):
            return "brand"
        if Intent.is_category_query(q):
            return "category"
        if Intent.is_product_query(q) and not Intent.is_sales(q):
            return "product_master"
        if Intent.is_store_query(q) and not Intent.is_sales(q):
            return "store_master"
        return "unknown"
