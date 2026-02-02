import json
import asyncio
from src.ingestion.reformulate import reformulate_query
from src.orchestration.classifier import classify_query

async def run():
    cases = json.load(open("data/eval/sample_cases.json", "r", encoding="utf-8"))
    ok = 0
    total = len(cases)

    for c in cases:
        norm = await reformulate_query(c["q"])
        pred = await classify_query(norm)
        if pred.get("label") == c["expected_label"]:
            ok += 1

    print(f"Classifier accuracy: {ok}/{total} = {ok/total:.2%}")

if __name__ == "__main__":
    asyncio.run(run())
