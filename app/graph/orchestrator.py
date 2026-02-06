from app.graph.nodes import node_classify

def build_graph():
    async def run(state):
        return await node_classify(state)
    return run
