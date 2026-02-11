from app.core.schemas import OrchestratorState
from app.graph.nodes import node_classify, node_run_text2sql, node_run_llm_general


class Graph:
    async def ainvoke(self, state: OrchestratorState) -> dict:
        state.normalized_message = (state.raw_message or "").strip()

        state = await node_classify(state)

        agent = (state.classification.agent if state.classification else "general")

        if agent == "text2sql":
            state = await node_run_text2sql(state)
        else:
            state = await node_run_llm_general(state)

        return {
            "final_answer": state.final_answer,
            "meta": state.meta,
        }


def build_graph() -> Graph:
    return Graph()
