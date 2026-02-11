# app/core/schema_registry.py
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
from openpyxl import load_workbook


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    attr: str


@dataclass
class TableInfo:
    db: str
    division: str
    table: str
    entity: str
    description: str
    columns: List[ColumnInfo]


def _norm(s: Optional[str]) -> str:
    return (str(s).strip() if s is not None else "")


def _canon(s: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9_]+", "", (s or "").lower())


class SchemaRegistry:

    def __init__(self, xlsx_path: str):
        self.xlsx_path = xlsx_path
        self.tables: List[TableInfo] = []
        self._index: List[Tuple[str, TableInfo]] = []

    def load(self) -> None:
        if not os.path.exists(self.xlsx_path):
            raise FileNotFoundError(f"Dictionary xlsx not found: {self.xlsx_path}")

        wb = load_workbook(self.xlsx_path, data_only=True)

        sh_table = wb["Table"]
        sh_col = wb["Column"]

        header = [c.value for c in next(sh_table.iter_rows(min_row=1, max_row=1))]
        col_map = {name: i for i, name in enumerate(header) if name}

        def v(row, key):
            idx = col_map.get(key)
            return (row[idx].value if idx is not None else None)

        table_rows: Dict[str, Dict[str, Any]] = {}
        for row in sh_table.iter_rows(min_row=2):
            db = v(row, "DB")
            div = v(row, "Division of work")
            tname = v(row, "Table Name")
            entity = v(row, "Entity Name") or ""
            desc = v(row, "Description") or ""
            if not db or not tname:
                continue

            dbs = _norm(db)
            tns = _norm(tname)
            key = f"{dbs}::{tns}"
            table_rows[key] = {
                "db": dbs,
                "division": _norm(div),
                "table": tns,
                "entity": _norm(entity),
                "description": _norm(desc),
                "columns": []
            }

        header2 = [c.value for c in next(sh_col.iter_rows(min_row=1, max_row=1))]
        col_map2 = {name: i for i, name in enumerate(header2) if name}

        def v2(row, key):
            idx = col_map2.get(key)
            return (row[idx].value if idx is not None else None)

        for row in sh_col.iter_rows(min_row=2):
            db = v2(row, "DB")
            tname = v2(row, "Table Name")
            cname = v2(row, "Column Name")
            attr = v2(row, "Attribute Name") or ""
            dtype = v2(row, "Datatype") or ""
            if not tname or not cname:
                continue

            if db:
                key = f"{_norm(db)}::{_norm(tname)}"
            else:
                candidates = [k for k in table_rows.keys() if k.endswith(f"::{_norm(tname)}")]
                key = candidates[0] if candidates else None

            if not key or key not in table_rows:
                continue

            table_rows[key]["columns"].append(ColumnInfo(
                name=_norm(cname),
                dtype=_norm(dtype),
                attr=_norm(attr),
            ))

        self.tables = [TableInfo(**t) for t in table_rows.values()]

        self._index = []
        for t in self.tables:
            blob = " ".join([
                t.db, t.division, t.table, t.entity, t.description,
                " ".join([c.name for c in t.columns]),
                " ".join([c.attr for c in t.columns]),
            ]).lower()
            self._index.append((blob, t))

    def highlights(self, t: TableInfo) -> Dict[str, List[str]]:
        cols = [c.name for c in t.columns]
        lc = [x.lower() for x in cols]

        date_cols = [cols[i] for i, x in enumerate(lc) if ("date" in x) or x.endswith("_dt") or x.endswith("dt")]
        store_cols = [cols[i] for i, x in enumerate(lc) if x in ("storeid", "store_id", "bizloc_cd", "location", "locationid")]

        metric_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "netsale", "grosssale", "tax_vat", "discount", "actualcost", "soldqty", "qty",
            "value", "amount"
        )]

        key_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "gds_cd", "item_cd", "promotionid", "evt_cd", "receiptno", "bizloc_cd", "storeid", "cate_cd"
        )]

        name_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "gds_nm", "item_nm", "name", "item_name", "gds_label_nm", "store_nm", "cate_nm", "brand_nm"
        )]

        return {
            "date_cols": date_cols[:6],
            "store_cols": store_cols[:6],
            "metric_cols": metric_cols[:10],
            "key_cols": key_cols[:10],
            "name_cols": name_cols[:10],
        }

    def search(self, query: str, top_k: int = 8) -> List[TableInfo]:
        q = (query or "").lower().strip()
        if not q:
            return []

        tokens = [x for x in re.split(r"[^a-z0-9_]+", q) if x]
        scored: List[Tuple[int, TableInfo]] = []
        for blob, t in self._index:
            score = 0
            for tok in tokens:
                if tok in blob:
                    score += 2
            if t.table.lower() in q:
                score += 10
            scored.append((score, t))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [t for s, t in scored if s > 0][:top_k]

    # -------------------------------
    def build_relationships(self) -> List[Dict[str, Any]]:

        rel: List[Dict[str, Any]] = []

        tbl_cols: Dict[str, List[Dict[str, str]]] = {}
        for t in self.tables:
            tbl_cols[t.table] = [{
                "name": c.name,
                "attr": (c.attr or "").lower(),
                "canon": _canon(c.name),
            } for c in t.columns]

        rel.append({
            "left": "Cluster_Main_Sales.GDS_CD",
            "right": "Dimension_IM.GDS_CD",
            "type": "join_key",
            "label": "product",
            "score": 999
        })
        rel.append({
            "table": "Dimension_IM",
            "name_column": "GDS_NM",
            "type": "name_column",
            "label": "product name",
            "score": 999
        })

        JOIN_CANON = {
            "product": {"gds_cd", "productcode", "product_cd", "gdsid"},
            "item": {"item_cd", "itemcode", "itemid"},
            "store": {"storeid", "store_id", "bizloc_cd", "locationid", "location"},
            "category": {"cate_cd", "categorycode", "category_cd"},
            "receipt": {"receiptno", "receipt_no"},
            "promotion": {"promotionid", "promotion_id"},
            "event": {"evt_cd", "eventcode"},
            "brand": {"brand_cd", "gds_brnd_cd", "repr_brnd_cd"},
            "vendor": {"ven_cd", "repr_ven_cd", "vendor_cd"},
        }

        SEMANTIC_KEYS = {
            "product": ["product code"],
            "item": ["item code"],
            "store": ["store number", "location", "store code"],
            "category": ["category code"],
            "receipt": ["receipt no", "receipt number"],
            "promotion": ["promotion id"],
            "event": ["event code"],
            "brand": ["brand code"],
            "vendor": ["vendor", "customer code"],
        }

        occ_by_group: Dict[str, List[Tuple[str, str, int]]] = {g: [] for g in JOIN_CANON.keys()}

        for tbl, cols in tbl_cols.items():
            for c in cols:
                canon = c["canon"]
                attr = c["attr"]

                for group, canon_set in JOIN_CANON.items():
                    score = 0

                    if canon in canon_set:
                        score += 10
                    else:
                        if canon.endswith("_cd") and any(x.endswith("_cd") for x in canon_set):
                            score += 2

                    for phrase in SEMANTIC_KEYS.get(group, []):
                        if phrase in attr:
                            score += 6

                    if score > 0:
                        occ_by_group[group].append((tbl, c["name"], score))

        edges: List[Dict[str, Any]] = []
        for group, occ in occ_by_group.items():
            occ_sorted = sorted(occ, key=lambda x: x[2], reverse=True)[:20]

            for i in range(len(occ_sorted)):
                for j in range(i + 1, len(occ_sorted)):
                    t1, c1, s1 = occ_sorted[i]
                    t2, c2, s2 = occ_sorted[j]
                    if t1 == t2:
                        continue

                    es = s1 + s2

                    if _canon(c1) == _canon(c2):
                        es += 5

                    edges.append({
                        "left": f"{t1}.{c1}",
                        "right": f"{t2}.{c2}",
                        "type": "join_key",
                        "label": group,
                        "score": es,
                    })

        seen = set()
        edges2 = []
        for e in sorted(edges, key=lambda x: x["score"], reverse=True):
            a = e["left"]
            b = e["right"]
            key = "||".join(sorted([a, b])) + f"::{e['label']}"
            if key in seen:
                continue
            seen.add(key)
            edges2.append(e)

        name_edges: List[Dict[str, Any]] = []
        name_semantics = [
            "product name", "item name", "store name",
            "category name", "brand name", "customer name", "vendor name",
        ]
        name_col_candidates = {"gds_nm", "item_nm", "store_nm", "cate_nm", "brand_nm", "gds_label_nm"}

        for tbl, cols in tbl_cols.items():
            for c in cols:
                canon = c["canon"]
                attr = c["attr"]
                score = 0
                label = None

                if canon in name_col_candidates:
                    score += 8
                for ns in name_semantics:
                    if ns in attr:
                        score += 6
                        label = ns
                        break

                if score > 0:
                    name_edges.append({
                        "table": tbl,
                        "name_column": c["name"],
                        "type": "name_column",
                        "label": label or "name",
                        "score": score,
                    })

        rel.extend(edges2[:120])       # join keys
        rel.extend(sorted(name_edges, key=lambda x: x["score"], reverse=True)[:60])  # name cols

        rel = sorted(rel, key=lambda x: x.get("score", 0), reverse=True)[:140]
        return rel
