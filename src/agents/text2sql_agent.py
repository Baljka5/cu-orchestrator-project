from src.llm.client import llm_client

SYSTEM = """Та Text-to-SQL агент.
Зорилго: хэрэглэгчийн асуултыг SQL хийхэд шаардлагатай мэдээлэл болгон хувиргах.
Одоогоор DB холболтгүй.
ГАРАЛТ:
1) шаардлагатай хүснэгт/талбарын жагсаалт (таамаг)
2) SQL query (placeholder)
3) шалгах асуултууд (ямар schema хэрэгтэй гэх мэт)
"""

async def run_text2sql_agent(q: str) -> str:
    msg = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": q},
    ]
    return await llm_client.chat(msg, temperature=0.1, max_tokens=500)
