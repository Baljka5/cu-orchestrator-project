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
            key = f"{db}::{tname}"
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
                candidates = [k for k in table_rows.keys() if k.endswith(f"::{str(tname).strip()}")]
                key = candidates[0] if candidates else None

            if not key or key not in table_rows:
                continue

            table_rows[key]["columns"].append(ColumnInfo(
                name=str(cname).strip(),
                dtype=str(dtype).strip(),
                attr=str(attr).strip()
            ))

        self.tables = [
            TableInfo(**t) for t in table_rows.values()
        ]

        self._index = []
        for t in self.tables:
            blob = " ".join([
                t.db, t.division, t.table, t.entity, t.description,
                " ".join([c.name for c in t.columns]),
                " ".join([c.attr for c in t.columns])
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
