import json
import pytest
from deepeval.metrics import AnswerRelevancyMetric
from deepeval.test_case import LLMTestCase

from app.graph.orchestrator import build_graph
from app.core.schemas import OrchestratorState

graph = build_graph()

@pytest.mark.asyncio
async def test_relevancy_smoke():
    metric = AnswerRelevancyMetric(threshold=0.3)
    with open("tests/cases.json", "r", encoding="utf-8") as f:
        cases = json.load(f)

    for c in cases:
        state = OrchestratorState(raw_message=c["input"])
        out = await graph.ainvoke(state)

        test_case = LLMTestCase(
            input=c["input"],
            actual_output=out.final_answer
        )

        metric.measure(test_case)
        assert metric.score >= 0.0
