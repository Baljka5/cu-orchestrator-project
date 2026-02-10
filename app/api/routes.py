import logging
import re
from typing import Any, Dict, List, Optional

import clickhouse_connect
from fastapi import APIRouter

from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph
from app.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    CH_MAX_ROWS
)

router = APIRouter()
log = logging.getLogger("cu-orchestrator")


def _ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def _is_safe_sql(sql: str) -> bool:
    if not sql:
        return False
    s = sql.strip().lower()
    if not s.startswith("select"):
        return False
    banned = ["insert", "update", "delete", "drop", "truncate", "alter", "create", "attach", "detach"]
    return not any(b in s for b in banned)


def _ensure_limit(sql: str, max_rows: int) -> str:
    s = sql.strip().rstrip(";")
    if re.search(r"\blimit\b", s, flags=re.IGNORECASE):
        return s
    return f"{s}\nLIMIT {max_rows}"


def parse_sql_from_answer(answer: str) -> Optional[str]:
    if not answer:
        return None

    if "SQL:" in answer:
        rest = answer.split("SQL:", 1)[1].strip()
        if "DATA" in rest:
            sql_part = rest.split("DATA", 1)[0].strip()
            return sql_part or None
        return rest or None

    # If answer itself starts with SELECT
    if answer.strip().lower().startswith("select"):
        return answer.strip()

    return None


# ---------------------------
# API: /chat
# ---------------------------
@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    state = OrchestratorState(
        raw_message=req.message,
        forced_agent=req.force_agent
    )

    graph = build_graph()
    result = await graph.ainvoke(state)

    # DEBUG logs
    try:
        if isinstance(result, dict):
            log.info("GRAPH_RESULT_KEYS=%s", list(result.keys()))
            log.info("GRAPH_RESULT_META=%s", result.get("meta"))
        else:
            log.info("GRAPH_RESULT_TYPE=%s", type(result))
            log.info("GRAPH_RESULT_STR=%s", str(result)[:800])
    except Exception:
        log.exception("Failed to log graph result")

    answer: Optional[str] = None
    meta: Dict[str, Any] = {}
    sql: Optional[str] = None
    columns: List[str] = []
    rows: List[List[Any]] = []

    if isinstance(result, dict):
        meta = result.get("meta") or {}
        answer = (
                result.get("final_answer")
                or result.get("answer")
                or result.get("output")
                or result.get("response")
        )

        sql = result.get("sql") or sql
        columns = result.get("columns") or columns
        rows = result.get("rows") or rows

    if not answer:
        answer = f"Хариу үүсээгүй байна. meta={meta}"

    agent = (meta.get("agent") or meta.get("mode") or "").lower()
    forced = (req.force_agent or "").lower()

    if agent == "text2sql" or forced == "text2sql":
        if not sql:
            sql = parse_sql_from_answer(answer)

        if sql and (not rows or not columns):
            try:
                if _is_safe_sql(sql):
                    sql_run = _ensure_limit(sql, min(int(CH_MAX_ROWS), 200))
                    client = _ch_client()
                    res = client.query(sql_run)

                    columns = list(res.column_names or [])
                    rows = [list(r) for r in (res.result_rows or [])][:50]
                    sql = sql_run
                else:
                    meta["sql_blocked"] = True
            except Exception as e:
                meta["ch_error"] = str(e)

    return ChatResponse(
        answer=answer,
        meta=meta,
        sql=sql,
        columns=columns,
        rows=rows,
    )
