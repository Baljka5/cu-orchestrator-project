import json
import re
from app.config import GUARD_BLOCKLIST, MAX_INPUT_CHARS
from app.core.llm import LLMClient
from app.core.schemas import OrchestratorState, GuardResult, ClassificationResult
from app.agents.policy_agent import policy_answer
from app.agents.text2sql_agent import text2sql_answer
from app.agents.research_agent import research_answer

llm = LLMClient()

def _clip(text: str) -> str:
    return text[:MAX_INPUT_CHARS]

async def node_guard(state: OrchestratorState) -> OrchestratorState:
    msg = _clip(state.raw_message)

    lowered = msg.lower()
    for bad in GUARD_BLOCKLIST:
        if bad and bad in lowered:
            state.guard = GuardResult(allowed=False, reason=f"Blocked by keyword: {bad}", labels=["blocked_keyword"])
            return state

    prompt = [
        {"role": "system", "content": "You are a safety classifier. Reply ONLY JSON."},
        {"role": "user", "content": json.dumps({
            "task": "Decide if the user request is safe to answer for a corporate assistant.",
            "labels": ["allowed", "disallowed"],
            "rules": [
                "Disallow self-harm, suicide, weapons/explosives instructions, illegal hacking, personal data leakage."
            ],
            "text": msg
        }, ensure_ascii=False)}
    ]
    try:
        out = await llm.chat(prompt, temperature=0.0, max_tokens=120)
        m = re.search(r"\{.*\}", out, re.DOTALL)
        data = json.loads(m.group(0) if m else out)
        allowed = (data.get("label") == "allowed") or (data.get("allowed") is True)
        reason = data.get("reason", "")
        state.guard = GuardResult(allowed=bool(allowed), reason=reason, labels=data.get("labels", []))
        return state
    except Exception:
        state.guard = GuardResult(allowed=True, reason="guard_fallback")
        return state

async def node_reformulate(state: OrchestratorState) -> OrchestratorState:
    msg = _clip(state.raw_message)

    prompt = [
        {"role": "system", "content": "You normalize Mongolian user queries for downstream agents. Keep meaning. Output ONLY the normalized query text."},
        {"role": "user", "content": msg}
    ]
    try:
        state.normalized_message = (await llm.chat(prompt, temperature=0.1, max_tokens=256)).strip()
    except Exception:
        state.normalized_message = msg
    return state

async def node_classify(state: OrchestratorState) -> OrchestratorState:
    q = state.normalized_message or state.raw_message

    schema = {
        "agent": "policy|text2sql|research",
        "confidence": "0..1",
        "rationale": "short"
    }
    prompt = [
        {"role": "system", "content": "You are a router. Choose which agent should handle the query. Reply ONLY JSON."},
        {"role": "user", "content": json.dumps({
            "schema": schema,
            "examples": [
                {"q": "Амралтын хүсэлт гаргах журам?", "agent": "policy"},
                {"q": "Өнгөрсөн 7 хоногийн CU520 борлуулалт хэд вэ?", "agent": "text2sql"},
                {"q": "Салбарын эрсдэлийн тайлангийн гол дүгнэлт юу вэ?", "agent": "research"}
            ],
            "query": q
        }, ensure_ascii=False)}
    ]

    try:
        out = await llm.chat(prompt, temperature=0.0, max_tokens=160)
        m = re.search(r"\{.*\}", out, re.DOTALL)
        data = json.loads(m.group(0) if m else out)
        state.classification = ClassificationResult(
            agent=str(data.get("agent", "research")),
            confidence=float(data.get("confidence", 0.5)),
            rationale=str(data.get("rationale", ""))[:200],
        )
    except Exception:
        state.classification = ClassificationResult(agent="research", confidence=0.2, rationale="classifier_fallback")

    return state

async def node_run_agent(state: OrchestratorState) -> OrchestratorState:
    agent = (state.classification.agent if state.classification else "research").lower()
    q = state.normalized_message or state.raw_message

    if agent == "policy":
        state.agent_result = policy_answer(q)
        state.meta["agent"] = "policy"
    elif agent == "text2sql":
        state.agent_result = await text2sql_answer(q)
        state.meta["agent"] = "text2sql"
    else:
        state.agent_result = research_answer(q)
        state.meta["agent"] = "research"

    return state

async def node_finalize(state: OrchestratorState) -> OrchestratorState:
    if state.guard and not state.guard.allowed:
        state.final_answer = "Уучлаарай, энэ төрлийн хүсэлтэд би хариулах боломжгүй."
        state.meta["guard_reason"] = state.guard.reason
        return state

    q = state.normalized_message or state.raw_message
    agent_out = state.agent_result

    prompt = [
        {"role": "system", "content": "You are CU Orchestrator. Compose a helpful answer in Mongolian. Be concise and actionable."},
        {"role": "user", "content": f"Question:\n{q}\n\nAgent result:\n{agent_out}\n\nReturn final answer."}
    ]
    try:
        state.final_answer = (await llm.chat(prompt, temperature=0.2, max_tokens=500)).strip()
    except Exception:
        state.final_answer = agent_out or "Алдаа гарлаа."
    return state
