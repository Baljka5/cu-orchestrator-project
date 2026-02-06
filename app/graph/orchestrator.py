from typing import Any, Dict
from app.core.schemas import OrchestratorState
from app.graph.nodes import node_reformulate, node_classify, node_run_agent, node_finalize


class SimpleGraph:
    async def ainvoke(self, state: OrchestratorState) -> Dict[str, Any]:
        state = await node_reformulate(state)
        state = await node_classify(state)
        state = await node_run_agent(state)
        result = await node_finalize(state)
        return result


def build_graph() -> SimpleGraph:
    return SimpleGraph()
