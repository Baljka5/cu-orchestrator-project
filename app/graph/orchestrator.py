from langgraph.graph import StateGraph, END
from app.core.schemas import OrchestratorState
from app.graph.nodes import node_guard, node_reformulate, node_classify, node_run_agent, node_finalize

def build_graph():
    g = StateGraph(OrchestratorState)

    g.add_node("n_guard", node_guard)
    g.add_node("reformulate", node_reformulate)
    g.add_node("classify", node_classify)
    g.add_node("run_agent", node_run_agent)
    g.add_node("finalize", node_finalize)

    g.set_entry_point("n_guard")

    def route_after_guard(state: OrchestratorState):
        if state.guard and not state.guard.allowed:
            return "finalize"
        return "reformulate"

    g.add_conditional_edges("n_guard", route_after_guard, {
        "finalize": "finalize",
        "reformulate": "reformulate",
    })

    g.add_edge("reformulate", "classify")
    g.add_edge("classify", "run_agent")
    g.add_edge("run_agent", "finalize")
    g.add_edge("finalize", END)

    return g.compile()
