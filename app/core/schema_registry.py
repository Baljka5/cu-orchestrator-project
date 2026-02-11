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

    def highlights(self, t: TableInfo) -> Dict[str, List[str]]:
        cols = [c.name for c in t.columns]
        lc = [x.lower() for x in cols]

        date_cols = [cols[i] for i, x in enumerate(lc) if "date" in x or x.endswith("_dt")]
        store_cols = [cols[i] for i, x in enumerate(lc) if x in ("storeid", "store_id", "bizloc_cd")]
        metric_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "netsale", "grosssale", "tax_vat", "discount", "actualcost", "soldqty"
        )]
        key_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "gds_cd", "item_cd", "promotionid", "evt_cd", "receiptno", "cate_cd"
        )]
        name_cols = [cols[i] for i, x in enumerate(lc) if x in (
            "gds_nm", "item_nm", "store_nm", "cate_nm", "brand_nm", "gds_label_nm"
        )]

        return {
            "date_cols": date_cols[:6],
            "store_cols": store_cols[:6],
            "metric_cols": metric_cols[:10],
            "key_cols": key_cols[:10],
            "name_cols": name_cols[:10],
        }

    def build_relationships(self) -> List[Dict[str, Any]]:
        rel: List[Dict[str, Any]] = []

        # Manual high-confidence overrides
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

        # Build dynamic relationships
        tbl_cols: Dict[str, List[Dict[str, str]]] = {}
        for t in self.tables:
            tbl_cols[t.table] = [{
                "name": c.name,
                "attr": (c.attr or "").lower(),
                "canon": _canon(c.name),
            } for c in t.columns]

        JOIN_CANON = {
            "product": {"gds_cd"},
            "item": {"item_cd"},
            "store": {"storeid", "store_id", "bizloc_cd"},
            "category": {"cate_cd"},
            "receipt": {"receiptno"},
            "promotion": {"promotionid"},
            "event": {"evt_cd"},
        }

        for group, canon_set in JOIN_CANON.items():
            occ = []
            for tbl, cols in tbl_cols.items():
                for c in cols:
                    score = 0
                    if c["canon"] in canon_set:
                        score += 10
                    if group in c["attr"]:
                        score += 5
                    if score > 0:
                        occ.append((tbl, c["name"], score))

            occ = sorted(occ, key=lambda x: x[2], reverse=True)[:20]

            for i in range(len(occ)):
                for j in range(i + 1, len(occ)):
                    t1, c1, s1 = occ[i]
                    t2, c2, s2 = occ[j]
                    if t1 == t2:
                        continue
                    rel.append({
                        "left": f"{t1}.{c1}",
                        "right": f"{t2}.{c2}",
                        "type": "join_key",
                        "label": group,
                        "score": s1 + s2
                    })

        for tbl, cols in tbl_cols.items():
            for c in cols:
                if "name" in c["attr"] or c["canon"] in ("gds_nm", "item_nm"):
                    rel.append({
                        "table": tbl,
                        "name_column": c["name"],
                        "type": "name_column",
                        "label": "name",
                        "score": 10
                    })

        rel.sort(key=lambda x: x.get("score", 0), reverse=True)
        return rel[:120]
