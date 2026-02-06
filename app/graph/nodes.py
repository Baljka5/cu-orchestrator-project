from app.core.schemas import OrchestratorState

async def node_guard(state: OrchestratorState) -> OrchestratorState:
    # TODO: policy/safety check logic
    return state

async def node_reformulate(state: OrchestratorState) -> OrchestratorState:
    # TODO: query cleanup / normalization
    return state

async def node_run_agent(state: OrchestratorState) -> OrchestratorState:
    # TODO: call selected agent (text2sql/policy/research/general)
    return state

async def node_finalize(state: OrchestratorState) -> OrchestratorState:
    # TODO: finalize response formatting, citations etc.
    return state
