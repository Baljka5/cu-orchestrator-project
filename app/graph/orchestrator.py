from app.core.schemas import OrchestratorState
from app.graph.nodes import node_classify, node_run_llm

class SimpleGraph:
    async def ainvoke(self, state: OrchestratorState):
        state = await node_classify(state)
        state = await node_run_llm(state)

        # routes.py чинь dict.get(...) гэж авдаг тул dict буцаана
        return {
            "final_answer": state.final_answer,
            "meta": state.meta,
        }

def build_graph():
    return SimpleGraph()
