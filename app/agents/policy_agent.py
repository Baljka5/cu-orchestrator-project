from pathlib import Path

BASE = Path(__file__).resolve().parents[1] / "data" / "policy"

def policy_answer(query: str) -> str:
    docs = []
    for p in BASE.glob("*.md"):
        docs.append((p.name, p.read_text(encoding="utf-8", errors="ignore")))

    q = query.lower()
    hits = []
    for name, text in docs:
        if any(k in text.lower() for k in q.split()[:4]):
            hits.append((name, text[:1200]))

    if not hits:
        return "Policy knowledge base-д энэ асуултад шууд таарах хэсэг олдсонгүй. Журам/дотоод баримтын нэр эсвэл түлхүүр үгийг тодруулбал илүү сайн хайна."

    out = "Дотоод журам/баримтаас олдсон хэсгүүд:\n"
    for name, chunk in hits[:3]:
        out += f"\n---\n[{name}]\n{chunk}\n"
    return out
