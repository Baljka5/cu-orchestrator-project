from src.llm.client import llm_client

SYSTEM = """Та судалгааны үр дүн/дүн шинжилгээний агент.
Зорилго: өгөгдөл эсвэл тайлан дээр үндэслэн тайлбар, дүгнэлт, санал гаргах.
Хэрэв өгөгдөл дутуу бол ямар өгөгдөл хэрэгтэйг тодорхой асуу.
"""

async def run_research_agent(q: str) -> str:
    msg = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": q},
    ]
    return await llm_client.chat(msg, temperature=0.3, max_tokens=500)
