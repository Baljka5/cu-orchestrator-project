from src.llm.client import llm_client

REFORM_SYSTEM = """Та асуултыг стандартчилдаг модуль.
Зорилго:
- Галигласан/холимог бичвэрийг Монгол кирилл болгон засах боломжтой бол зас.
- Илүү тодорхой, богино, нэг утгатай асуулт болго.
- Агуулгыг өөрчилж гуйвуулахгүй.
Зөвхөн засварласан эцсийн асуултыг буцаа.
"""

async def reformulate_query(user_query: str) -> str:
    msg = [
        {"role": "system", "content": REFORM_SYSTEM},
        {"role": "user", "content": user_query},
    ]
    return (await llm_client.chat(msg, temperature=0.1, max_tokens=128)).strip()
