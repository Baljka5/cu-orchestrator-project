from src.llm.client import llm_client
import json

CLASSIFIER_SYSTEM = """Та хүсэлтийг ангилагч.
Ангиллууд:
- policy: байгууллагын дотоод журам, дүрэм, процесс, HR бодлого
- text2sql: тоон тайлан, дата, KPI, BI, хүснэгтээс гаргах асуулт (SQL хэрэгтэй)
- research: судалгаа/дүн шинжилгээ/тайлангийн дүгнэлт, харьцуулалт, тайлбар
- other: дээрхэд хамаарахгүй

ГАРАЛТ: JSON хэлбэрээр:
{"label": "...", "confidence": 0.0-1.0, "reason": "..."}

Зөвхөн JSON буцаа.
"""

async def classify_query(q: str) -> dict:
    msg = [
        {"role": "system", "content": CLASSIFIER_SYSTEM},
        {"role": "user", "content": q},
    ]
    out = await llm_client.chat(msg, temperature=0.0, max_tokens=128)
    try:
        return json.loads(out)
    except Exception:
        return {"label": "other", "confidence": 0.0, "reason": "parse_failed", "raw": out[:200]}
