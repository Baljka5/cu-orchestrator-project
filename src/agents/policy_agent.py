from src.llm.client import llm_client

SYSTEM = """Та CU байгууллагын журам/дотоод процессын туслах.
Хэрэв баримт бичгийн эх сурвалж хэрэгтэй бол 'Эх сурвалж шаардлагатай' гэж хэлээд,
ямар төрлийн баримт (журам, тушаал, гарын авлага гэх мэт) хэрэгтэйг тодруул.
Одоогоор RAG холбогдоогүй тул боломжит хэмжээнд ерөнхий зөвлөмж өг.
"""

async def run_policy_agent(q: str) -> str:
    msg = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": q},
    ]
    return await llm_client.chat(msg, temperature=0.2, max_tokens=400)
