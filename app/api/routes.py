import logging
import re
from typing import Any, Dict

from fastapi import APIRouter

from app.core.schemas import ChatRequest, ChatResponse, OrchestratorState
from app.graph.orchestrator import build_graph

router = APIRouter()
log = logging.getLogger("cu-orchestrator")


# ---------------------------------------------------------
# Helper: Text2SQL string → structured (sql / columns / rows)
# ---------------------------------------------------------
def parse_text2sql_answer(answer: str) -> Dict[str, Any]:
    """
    Expected formats handled:
      - "... SQL:\\n<sql>\\n\\nDATA (top N):\\ncol | col\\nval | val"
      - "... SQL:\\n<sql>"
      - plain text (non-text2sql)
    """
    out = {
        "notes": "",
        "sql": None,
        "columns": [],
        "rows": [],
    }

    if not answer:
        return out

    # No SQL section → just notes
    if "SQL:" not in answer:
        out["notes"] = answer.strip()
        return out

    # Split notes / SQL+DATA
    notes, rest = answer.split("SQL:", 1)
    out["notes"] = notes.strip()

    # No DATA section → only SQL
    if "DATA" not in rest:
        out["sql"] = rest.strip()
        return out

    # Split SQL / DATA
    sql_part, data_part = rest.split("DATA", 1)
    out["sql"] = sql_part.strip()

    # Parse table-like data
    lines = [
        l.strip()
        for l in data_part.splitlines()
        if "|" in l
    ]

    if not lines:
        return out

    # Header
    out["columns"] = [c.strip() for c in lines[0].split("|")]

    # Rows
    for line in lines[1:]:
        out["rows"].append([c.strip() for c in line.split("|")])

    return out


# ---------------------------------------------------------
# API: /chat
# ---------------------------------------------------------
@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    # Build initial orchestrator state
    state = OrchestratorState(
        raw_message=req.message,
        forced_agent=req.force_agent
    )

    graph = build_graph()
    result = await graph.ainvoke(state)

    # -------------------------
    # DEBUG logs
    # -------------------------
    try:
        if isinstance(result, dict):
            log.info("GRAPH_RESULT_KEYS=%s", list(result.keys()))
            log.info("GRAPH_RESULT_META=%s", result.get("meta"))
        else:
            log.info("GRAPH_RESULT_TYPE=%s", type(result))
            log.info("GRAPH_RESULT_STR=%s", str(result)[:800])
    except Exception:
        log.exception("Failed to log graph result")

    # -------------------------
    # Normalize output
    # -------------------------
    answer: str | None = None
    meta: Dict[str, Any] = {}
    sql = None
    columns = []
    rows = []

    if isinstance(result, dict):
        meta = result.get("meta") or {}
        answer = (
                result.get("final_answer")
                or result.get("answer")
                or result.get("output")
                or result.get("response")
        )

    if not answer:
        answer = f"Хариу үүсээгүй байна. meta={meta}"

    # -------------------------
    # Text2SQL → structured response
    # -------------------------
    agent = (meta.get("agent") or meta.get("mode") or "").lower()
    forced = (req.force_agent or "").lower()

    if agent == "text2sql" or forced == "text2sql":
        parsed = parse_text2sql_answer(answer)

        # UI дээр notes-ийг human text болгож харуулна
        answer = parsed.get("notes") or answer
        sql = parsed.get("sql")
        columns = parsed.get("columns", [])
        rows = parsed.get("rows", [])

    # -------------------------
    # Final response
    # -------------------------
    return ChatResponse(
        answer=answer,
        meta=meta,
        sql=sql,
        columns=columns,
        rows=rows,
    )
