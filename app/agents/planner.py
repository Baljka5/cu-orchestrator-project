# app/agents/text2sql/planner.py
import json
from typing import Any, Dict, List, Optional, Set

from app.core.llm import LLMClient
from app.agents.text2sql.plan_utils import safe_json_loads, normalize_plan

llm = LLMClient()


def planner_system_prompt() -> str:
    return """
You are a ClickHouse Text-to-SQL planner.
Return ONLY valid JSON. No markdown. No explanation.

Rules:
- Use only tables from allowed_tables
- Fact alias must be f
- Joined aliases must be d1, d2, d3 ...
- For sales-related questions, prefer BI_DB.Cluster_Main_Sales as fact_table
- Use aggregate functions correctly with GROUP BY
- Avoid selecting non-aggregated columns unless grouped
- Prefer concise SQL plan

Return JSON with shape:
{
  "fact_table": "DB.TABLE",
  "select": [{"expr":"...", "as":"..."}],
  "joins": [{"type":"LEFT","table":"DB.TABLE","alias":"d1","on":"f.col = d1.col"}],
  "where": ["..."],
  "group_by": ["..."],
  "order_by": ["..."],
  "limit": 50
}
""".strip()


async def plan_with_llm(
        query: str,
        candidates: List[Any],
        rel_filtered: List[Dict[str, Any]],
        allowed_tables: Set[str],
        registry: Any,
) -> Optional[Dict[str, Any]]:
    table_cards = [registry.to_table_card(t, max_cols=25) for t in candidates[:4]]

    user_payload = {
        "question": query,
        "table_cards": table_cards,
        "allowed_tables": sorted(list(allowed_tables))[:60],
        "relationships": rel_filtered[:20],
    }

    out = await llm.chat(
        [
            {"role": "system", "content": planner_system_prompt()},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        temperature=0.0,
        max_tokens=800,
    )

    plan = safe_json_loads(out)
    if not isinstance(plan, dict):
        return None

    return normalize_plan(plan)
