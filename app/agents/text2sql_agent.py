# app/agents/text2sql_agent.py
import re
import json
import clickhouse_connect
from typing import Any, Dict, List, Set

from app.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    SCHEMA_DICT_PATH,
)

from app.core.llm import LLMClient
from app.core.schema_registry import SchemaRegistry, TableInfo

llm = LLMClient()

_registry = SchemaRegistry(SCHEMA_DICT_PATH)
_registry.load()
_all_relationships = _registry.build_relationships()


def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def _wants_name(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "нэр", "name", "product name", "item name",
        "барааны нэр", "бүтээгдэхүүний нэр"
    ])


def _filter_relationships(candidates: List[TableInfo]) -> List[Dict[str, Any]]:
    cand_tables = {t.table for t in candidates[:8]}
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
    return rel_filtered[:80]


def _relationships_to_join_map(rels: List[Dict[str, Any]]) -> Dict[str, Any]:
    join_keys = []
    name_cols = []
    for r in rels:
        if r.get("type") == "join_key":
            join_keys.append(r)
        elif r.get("type") == "name_column":
            name_cols.append(r)

    graph: Dict[str, List[Dict[str, str]]] = {}
    for r in join_keys:
        lt, lc = r["left"].split(".", 1)
        rt, rc = r["right"].split(".", 1)
        graph.setdefault(lt, []).append({
            "to": rt,
            "on": f"{lt}.{lc} = {rt}.{rc}",
            "label": r.get("label", "")
        })
        graph.setdefault(rt, []).append({
            "to": lt,
            "on": f"{rt}.{rc} = {lt}.{lc}",
            "label": r.get("label", "")
        })

    return {
        "graph": graph,
        "name_columns": name_cols[:60],
        "join_keys": join_keys[:80]
    }


def _normalize_table_ref(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return t
    # DB.TABLE хэлбэргүй бол default db нэмнэ
    if "." not in t:
        return f"{CLICKHOUSE_DATABASE}.{t}"
    return t


def _safe_table(t: str, allowed: Set[str]) -> bool:
    t = _normalize_table_ref(t)
    base = t.split(".", 1)[-1]
    return (t in allowed) or (base in allowed)


def _inject_name_join(plan: Dict[str, Any],
                      candidates: List[TableInfo],
                      rel_filtered: List[Dict[str, Any]],
                      query: str) -> Dict[str, Any]:
    """
    Хэрвээ хэрэглэгч нэр асуусан бол dimension table-ээс name колонкыг автоматаар join + select хийнэ.
    """
    if not _wants_name(query):
        return plan

    fact_full = (plan.get("fact_table") or "").strip()
    if fact_full:
        fact_tbl = fact_full.split()[0].split(".")[-1]
    else:
        fact_tbl = candidates[0].table

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
        if (lt == fact_tbl and rt == dim_tbl) or (rt == fact_tbl and lt == dim_tbl):
            jk = r
            break

    if not jk:
        return plan

    plan.setdefault("joins", [])
    if not any(j.get("table", "").split(".")[-1] == dim_tbl for j in plan["joins"]):
        alias = f"d{len(plan['joins']) + 1}"

        lt, lc = jk["left"].split(".", 1)
        rt, rc = jk["right"].split(".", 1)
        if lt == fact_tbl:
            on = f"f.{lc} = {alias}.{rc}"
        else:
            on = f"f.{rc} = {alias}.{lc}"

        plan["joins"].append({
            "type": "LEFT",
            "table": _normalize_table_ref(dim_tbl),
            "alias": alias,
            "on": on
        })

        plan.setdefault("select", [])
        plan["select"].insert(0, {"expr": f"{alias}.{name_col}", "as": "item_name"})

        plan.setdefault("group_by", [])
        if plan.get("group_by") is not None:
            plan["group_by"].insert(0, f"{alias}.{name_col}")

    return plan


def _force_product_name_join(plan: Dict[str, Any], query: str) -> Dict[str, Any]:
    """
    Хэрэглэгч 'бүтээгдэхүүний нэр' гэж асуусан бол:
    - fact дээр GDS_CD байвал Dimension_IM join хийж GDS_NM (name) харуулна.
    - 'хамгийн их зарагдсан' бол sum(SoldQty) тооцоолоод ORDER BY хийнэ.
    """
    ql = (query or "").lower()

    wants_name = any(k in ql for k in [
        "нэр", "name", "барааны нэр", "бүтээгдэхүүний нэр", "product name", "item name"
    ])
    if not wants_name:
        return plan

    most_sold = any(k in ql for k in ["хамгийн их", "их зарагдсан", "most sold"])

    plan.setdefault("select", [])
    plan.setdefault("joins", [])
    plan.setdefault("where", [])
    plan.setdefault("group_by", [])
    plan.setdefault("order_by", [])
    plan.setdefault("limit", 50)

    # Dimension_IM join байгаа эсэх
    dim_join = None
    for j in plan["joins"]:
        if (j.get("table", "").split(".")[-1] == "Dimension_IM"):
            dim_join = j
            break

    if not dim_join:
        alias = f"d{len(plan['joins']) + 1}"
        dim_join = {
            "type": "LEFT",
            "table": f"{CLICKHOUSE_DATABASE}.Dimension_IM",
            "alias": alias,
            "on": f"f.GDS_CD = {alias}.GDS_CD"
        }
        plan["joins"].append(dim_join)

    alias = dim_join.get("alias") or "d1"

    # SELECT дээр GDS_NM заавал байна
    has_name = any(
        isinstance(x, dict) and ("GDS_NM" in (x.get("expr") or "") or x.get("as") in ("item_name", "gds_nm"))
        for x in plan["select"]
    )
    if not has_name:
        plan["select"].insert(0, {"expr": f"{alias}.GDS_NM", "as": "item_name"})

    # "хамгийн их зарагдсан" бол зөв aggregate хийж top 1 гаргана
    if most_sold:
        has_qty = any(isinstance(x, dict) and "SoldQty" in (x.get("expr") or "") for x in plan["select"])
        if not has_qty:
            plan["select"].append({"expr": "sum(f.SoldQty)", "as": "total_qty"})

        # group by дээр нэр (мөн давхар GDS_CD хүсвэл нэмэж болно)
        if f"{alias}.GDS_NM" not in plan["group_by"]:
            plan["group_by"].insert(0, f"{alias}.GDS_NM")

        # order by
        plan["order_by"] = ["total_qty DESC"]

        # top1
        plan["limit"] = 1

    return plan


def _extract_json(out: str) -> Dict[str, Any]:
    m = re.search(r"\{.*\}", out, re.DOTALL)
    raw = m.group(0) if m else out
    return json.loads(raw)


async def text2sql_answer(query: str) -> Dict[str, Any]:
    candidates = _registry.search(query, top_k=12)
    if not candidates:
        return {"final_answer": "Schema олдсонгүй.", "meta": {"agent": "text2sql"}}

    # table cards (LLM-д table бүрийг “танилцуулах”)
    table_cards = [_registry.to_table_card(t, max_cols=80) for t in candidates[:8]]

    rel_filtered = _filter_relationships(candidates)
    join_map = _relationships_to_join_map(rel_filtered)

    # allowed tables set
    allowed_tables: Set[str] = set()
    for t in candidates:
        allowed_tables.add(t.table)
        allowed_tables.add(f"{t.db}.{t.table}")
    # force include Dimension_IM
    allowed_tables.add("Dimension_IM")
    allowed_tables.add(f"{CLICKHOUSE_DATABASE}.Dimension_IM")

    system = """
You are a ClickHouse Text-to-SQL planner.
Return ONLY valid JSON (no markdown, no comments).

Hard rules:
- Use only tables in `allowed_tables`.
- If you need names (product/item/store/category names), join to a dimension table using `join_map.graph`.
- Always use aliases: fact table alias is `f`, joined tables are `d1`, `d2`, ...
- Put join conditions using aliases: e.g. "f.GDS_CD = d1.GDS_CD"
- WHERE conditions should also use aliases.

Return JSON with this schema:
{
  "fact_table": "DB.TABLE",
  "select": [{"expr":"...", "as":"..."}],
  "joins": [{"type":"LEFT","table":"DB.TABLE","alias":"d1","on":"f.col = d1.col"}],
  "where": ["..."],
  "group_by": ["..."],
  "order_by": ["..."],
  "limit": 50
}

Tips:
- For totals: use sum(...)
- For time windows: use toDate('YYYY-MM-DD')
"""

    user = {
        "question": query,
        "table_cards": table_cards,
        "join_map": join_map,
        "allowed_tables": sorted(list(allowed_tables))[:120],
    }

    out = await llm.chat([
        {"role": "system", "content": system.strip()},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False)}
    ], temperature=0.0, max_tokens=900)

    try:
        plan = _extract_json(out)
    except Exception as e:
        return {
            "final_answer": "LLM JSON plan parse error",
            "meta": {"agent": "text2sql", "error": str(e), "raw": out[:2000]}
        }

    # validate + normalize plan
    fact = _normalize_table_ref(plan.get("fact_table") or f"{candidates[0].db}.{candidates[0].table}")
    if not _safe_table(fact, allowed_tables):
        return {"final_answer": "Unsafe fact_table", "meta": {"agent": "text2sql", "plan": plan}}

    # optional auto inject name join
    plan = _inject_name_join(plan, candidates, rel_filtered, query)
    plan = _force_product_name_join(plan, query)

    # Build SQL from plan
    select_items = plan.get("select") or [{"expr": "count()", "as": "cnt"}]
    select_clause = ", ".join(
        f"{x['expr']} AS {x['as']}" if x.get("as") else x["expr"]
        for x in select_items
    )
    sql = f"SELECT {select_clause}\nFROM {fact} f"

    # joins
    for j in plan.get("joins", []) or []:
        jtype = (j.get("type") or "LEFT").upper()
        tbl = _normalize_table_ref(j.get("table") or "")
        alias = (j.get("alias") or "").strip() or f"d1"
        on = (j.get("on") or "").strip()

        if not tbl or not on:
            continue
        if not _safe_table(tbl, allowed_tables):
            return {"final_answer": "Unsafe join table", "meta": {"agent": "text2sql", "plan": plan, "sql": sql}}

        sql += f"\n{jtype} JOIN {tbl} {alias} ON {on}"

    # where
    wh = plan.get("where") or []
    wh = [x.strip() for x in wh if isinstance(x, str) and x.strip()]
    if wh:
        sql += "\nWHERE " + " AND ".join(wh)

    # group by
    gb = plan.get("group_by") or []
    gb = [x.strip() for x in gb if isinstance(x, str) and x.strip()]
    if gb:
        sql += "\nGROUP BY " + ", ".join(gb)

    # order by
    ob = plan.get("order_by") or []
    ob = [x.strip() for x in ob if isinstance(x, str) and x.strip()]
    if ob:
        sql += "\nORDER BY " + ", ".join(ob)

    # limit
    try:
        lim = int(plan.get("limit") or 50)
    except Exception:
        lim = 50
    lim = max(1, min(lim, 500))
    sql += f"\nLIMIT {lim}"

    # Execute
    client = _ch_client()
    try:
        res = client.query(sql)
    except Exception as e:
        return {
            "final_answer": "ClickHouse query error",
            "meta": {
                "agent": "text2sql",
                "sql": sql,
                "plan": plan,
                "error": str(e),
            }
        }

    return {
        "final_answer": "Text2SQL",
        "meta": {
            "agent": "text2sql",
            "sql": sql,
            "plan": plan,
            "tables_used": {
                "fact": fact,
                "joins": [j.get("table") for j in (plan.get("joins") or []) if j.get("table")]
            },
            "data": {"columns": res.column_names, "rows": res.result_rows[:lim]}
        }
    }
