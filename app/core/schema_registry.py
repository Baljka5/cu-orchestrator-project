# app/core/schema_registry.py
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
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

        table_rows: Dict[str, Dict] = {}
        for row in sh_table.iter_rows(min_row=2):
            db = v(row, "DB")
            div = v(row, "Division of work")
            tname = v(row, "Table Name")
            entity = v(row, "Entity Name") or ""
            desc = v(row, "Description") or ""
            if not db or not tname:
                continue
            key = f"{str(db).strip()}::{str(tname).strip()}"
            table_rows[key] = {
                "db": str(db).strip(),
                "division": str(div).strip() if div else "",
                "table": str(tname).strip(),
                "entity": str(entity).strip(),
                "description": str(desc).strip(),
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
                key = f"{str(db).strip()}::{str(tname).strip()}"
            else:
                # if DB missing on Column sheet, try match by table name
                candidates = [k for k in table_rows.keys() if k.endswith(f"::{str(tname).strip()}")]
                key = candidates[0] if candidates else None

            if not key or key not in table_rows:
                continue

            table_rows[key]["columns"].append(ColumnInfo(
                name=str(cname).strip(),
                dtype=str(dtype).strip(),
                attr=str(attr).strip()
            ))

        self.tables = [TableInfo(**t) for t in table_rows.values()]

        self._index = []
        for t in self.tables:
            blob = " ".join([
                t.db, t.division, t.table, t.entity, t.description,
                " ".join([c.name for c in t.columns]),
                " ".join([c.attr for c in t.columns])
            ]).lower()
            self._index.append((blob, t))

    def highlights(self, t: TableInfo) -> Dict[str, List[str]]:
        cols = [c.name for c in t.columns]
        lc = [x.lower() for x in cols]

        date_cols = [cols[i] for i, x in enumerate(lc) if ("date" in x) or x.endswith("_dt") or x.endswith("dt")]
        store_cols = [cols[i] for i, x in enumerate(lc) if
                      x in ("storeid", "store_id", "bizloc_cd", "location", "locationid")]

        metric_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "netsale", "grosssale", "tax_vat", "discount", "actualcost", "soldqty", "qty",
            "value", "amount"
        )]

        key_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "gds_cd", "item_cd", "promotionid", "evt_cd", "receiptno", "bizloc_cd", "storeid"
        )]

        # name-like columns (best effort)
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

    def build_relationships(self) -> List[Dict]:
        """
        Build relationships from dictionary semantics (Attribute Name).
        Returns list of:
          - {"left":"T1.COL","right":"T2.COL","type":"join_key","label":"product code"}
          - {"table":"Dimension_IM","name_column":"GDS_NM","type":"name_column","label":"product name"}
        """
        rel: List[Dict] = []

        # table -> [(col, attr_lower)]
        tbl_cols: Dict[str, List[Tuple[str, str]]] = {}
        for t in self.tables:
            tbl_cols[t.table] = [(c.name, (c.attr or "").lower()) for c in t.columns]

        # semantics that often represent join keys
        semantic_keys = [
            "product code",
            "item code",
            "store number",
            "promotion id",
            "event code",
            "receipt no",
            "category code",
            "brand code",
        ]

        for key in semantic_keys:
            occ: List[Tuple[str, str, str]] = []
            for tbl, cols in tbl_cols.items():
                for cname, attr in cols:
                    if key in attr:
                        occ.append((tbl, cname, key))

            for i in range(len(occ)):
                for j in range(i + 1, len(occ)):
                    a = occ[i]
                    b = occ[j]
                    if a[0] != b[0]:
                        rel.append({
                            "left": f"{a[0]}.{a[1]}",
                            "right": f"{b[0]}.{b[1]}",
                            "type": "join_key",
                            "label": key
                        })

        # semantics that represent human readable names
        name_semantics = [
            "product name",
            "item name",
            "store name",
            "category name",
            "brand name",
            "customer name",
            "vendor name",
        ]

        for tbl, cols in tbl_cols.items():
            for cname, attr in cols:
                for ns in name_semantics:
                    if ns in attr:
                        rel.append({
                            "table": tbl,
                            "name_column": cname,
                            "type": "name_column",
                            "label": ns
                        })

        # keep small
        return rel[:80]
