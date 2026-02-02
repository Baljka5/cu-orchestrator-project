from src.llm.client import llm_client

GUARD_SYSTEM = """Та байгууллагын дотоод AI хамгаалалтын модуль.
Дүрэм:
- Хакердах, нууц түлхүүр/нууц үг/credential авах, exploit хийх, хууль бус үйлдэл зааварлах хүсэлтүүдийг ТАТГАЛЗ.
- Ажилчдын хувийн мэдээлэл, нууцлалтай мэдээлэл шаардах бол ТАТГАЛЗ.
- Хэрэв хориглох бол: 'BLOCK' гэж буцаа, шалтгааныг 1 өгүүлбэрээр тайлбарла.
- Хэрэв зөвшөөрөх бол: 'ALLOW' гэж буцаа.
"""

async def guard_prompt(user_query: str) -> tuple[bool, str]:
    msg = [
        {"role": "system", "content": GUARD_SYSTEM},
        {"role": "user", "content": user_query},
    ]
    out = (await llm_client.chat(msg, temperature=0.0, max_tokens=64)).strip()
    if out.upper().startswith("BLOCK"):
        return False, out
    return True, "ALLOW"
