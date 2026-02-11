# app/agents/text2sql_agent.py
import re
import json
import clickhouse_connect
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set

from app.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    CH_MAX_ROWS, SCHEMA_DICT_PATH,
)

from app.core.llm import LLMClient
from app.core.schema_registry import SchemaRegistry, TableInfo

llm = LLMClient()

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()
_all_relationships = _registry.build_relationships()


# -------------------------------
def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


# -------------------------------
def _wants_name(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "нэр", "name", "product name", "item name",
        "барааны нэр", "бүтээгдэхүүний нэр"
    ])


def _is_most_sold_intent(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in ["хамгийн их", "most sold", "их зарагдсан"])


# -------------------------------
def _filter_relationships(candidates: List[TableInfo]) -> List[Dict[str, Any]]:
    cand_tables = {t.table for t in candidates[:6]}
    cand_tables.add("Dimension_IM")  # force include

    rel_filtered = []
    for r in _all_relationships:
        if r.get("type") == "join_key":
            lt = r["left"].split(".", 1)[0]
            rt = r["right"].split(".", 1)[0]
            if lt in cand_tables and rt in cand_tables:
                rel_filtered.append(r)
        elif r.get("type") == "name_column":
            if r.get("table") in cand_tables:
                rel_filtered.append(r)

    rel_filtered.sort(key=lambda x: x.get("score", 0), reverse=True)
    return rel_filtered[:60]


# -------------------------------
def _inject_name_join(plan: Dict[str, Any],
                      candidates: List[TableInfo],
                      rel_filtered: List[Dict[str, Any]],
                      query: str) -> Dict[str, Any]:
    if not _wants_name(query):
        return plan

    fact_full = (plan.get("fact_table") or "").strip()
    fact = fact_full.split()[0].split(".")[-1] if fact_full else candidates[0].table

    name_cols = [r for r in rel_filtered if r.get("type") == "name_column"]
    join_keys = [r for r in rel_filtered if r.get("type") == "join_key"]

    if not name_cols:
        return plan

    target = name_cols[0]
    dim_tbl = target["table"]
    name_col = target["name_column"]

    jk = None
    for r in join_keys:
        lt, lc = r["left"].split(".", 1)
        rt, rc = r["right"].split(".", 1)
        if (lt == fact and rt == dim_tbl) or (rt == fact and lt == dim_tbl):
            jk = r
            break

    if not jk:
        return plan

    plan.setdefault("joins", [])
    if not plan["joins"]:
        lt, lc = jk["left"].split(".", 1)
        rt, rc = jk["right"].split(".", 1)
        if lt == fact:
            on = f"f.{lc} = d1.{rc}"
        else:
            on = f"f.{rc} = d1.{lc}"

        plan["joins"].append({
            "type": "LEFT",
            "table": dim_tbl,
            "on": on
        })

    plan.setdefault("select", [])
    plan["select"].insert(0, {"expr": f"d1.{name_col}", "as": "item_name"})

    plan.setdefault("group_by", [])
    plan["group_by"].insert(0, f"d1.{name_col}")

    return plan


# -------------------------------
async def text2sql_answer(query: str) -> Dict[str, Any]:
    candidates = _registry.search(query, top_k=8)
    if not candidates:
        return {"final_answer": "Schema олдсонгүй.", "meta": {"agent": "text2sql"}}

    schema_ctx = []
    for t in candidates[:5]:
        schema_ctx.append({
            "db": t.db,
            "table": t.table,
            "columns": [{"name": c.name, "type": c.dtype} for c in t.columns[:60]],
        })

    allowed_tables: Set[str] = set()
    for t in candidates:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")

    rel_filtered = _filter_relationships(candidates)

    system = "Return ONLY JSON plan."
    user = {
        "question": query,
        "candidates": schema_ctx,
        "relationships": rel_filtered,
    }

    out = await llm.chat([
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False)}
    ], temperature=0.0, max_tokens=600)

    m = re.search(r"\{.*\}", out, re.DOTALL)
    raw = m.group(0) if m else out
    plan = json.loads(raw)

    plan = _inject_name_join(plan, candidates, rel_filtered, query)

    fact = plan.get("fact_table") or f"{candidates[0].db}.{candidates[0].table}"
    sql = f"SELECT * FROM {fact} LIMIT 10"

    if plan.get("select"):
        select_clause = ", ".join(
            f"{x['expr']} AS {x['as']}" if x.get("as") else x["expr"]
            for x in plan["select"]
        )
        sql = f"SELECT {select_clause} FROM {fact} f"

        for idx, j in enumerate(plan.get("joins", []), start=1):
            alias = f"d{idx}"
            sql += f"\nLEFT JOIN {j['table']} {alias} ON {j['on']}"

        if plan.get("group_by"):
            sql += "\nGROUP BY " + ", ".join(plan["group_by"])

        sql += "\nLIMIT 50"

    if not sql.lower().startswith("select"):
        return {"final_answer": "Unsafe SQL", "meta": {"agent": "text2sql"}}

    client = _ch_client()
    res = client.query(sql)

    return {
        "final_answer": "Text2SQL",
        "meta": {
            "agent": "text2sql",
            "sql": sql,
            "data": {"columns": res.column_names, "rows": res.result_rows[:50]}
        }
    }
