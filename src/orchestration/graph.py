from typing import TypedDict
from langgraph.graph import StateGraph, END

from src.ingestion.guard import guard_prompt
from src.ingestion.reformulate import reformulate_query
from src.orchestration.classifier import classify_query
from src.orchestration.router import route as route_fn
from src.orchestration.synthesize import synthesize

from src.agents.policy_agent import run_policy_agent
from src.agents.text2sql_agent import run_text2sql_agent
from src.agents.research_agent import run_research_agent


class State(TypedDict, total=False):
    user_query: str
    normalized_query: str
    allowed: bool
    guard_reason: str
    classification: dict
    route: str
    agent_answer: str
    final_answer: str


async def n_guard(state: State) -> State:
    ok, reason = await guard_prompt(state["user_query"])
    state["allowed"] = ok
    state["guard_reason"] = reason
    return state

async def n_reformulate(state: State) -> State:
    state["normalized_query"] = await reformulate_query(state["user_query"])
    return state

async def n_classify(state: State) -> State:
    state["classification"] = await classify_query(state["normalized_query"])
    return state

async def n_route(state: State) -> State:
    c = state["classification"]
    state["route"] = route_fn(c.get("label", "other"), float(c.get("confidence", 0.0)))
    return state

async def n_call_agent(state: State) -> State:
    q = state["normalized_query"]
    r = state["route"]

    if r == "policy_agent":
        state["agent_answer"] = await run_policy_agent(q)
    elif r == "text2sql_agent":
        state["agent_answer"] = await run_text2sql_agent(q)
    elif r == "research_agent":
        state["agent_answer"] = await run_research_agent(q)
    else:
        state["agent_answer"] = "Таны асуултыг боловсруулах тохирох агент олдсонгүй. Илүү тодорхой асуулт өгнө үү."
    return state

async def n_finalize(state: State) -> State:
    if not state.get("allowed", False):
        state["final_answer"] = state.get("guard_reason", "BLOCK")
        return state

    c = state.get("classification", {})
    label = c.get("label", "other")
    conf = float(c.get("confidence", 0.0) or 0.0)
    state["final_answer"] = synthesize(label, conf, state.get("route", "other"), state.get("agent_answer", ""))
    return state


def build_graph():
    g = StateGraph(State)
    g.add_node("guard", n_guard)
    g.add_node("reformulate", n_reformulate)
    g.add_node("classify", n_classify)
    g.add_node("router", n_route)
    g.add_node("call_agent", n_call_agent)
    g.add_node("finalize", n_finalize)

    g.set_entry_point("guard")

    def guard_cond(state: State) -> str:
        return "blocked" if not state.get("allowed", False) else "ok"

    g.add_conditional_edges("guard", guard_cond, {"blocked": "finalize", "ok": "reformulate"})
    g.add_edge("reformulate", "classify")
    g.add_edge("classify", "route")
    g.add_edge("route", "call_agent")
    g.add_edge("call_agent", "finalize")
    g.add_edge("finalize", END)
    return g.compile()
