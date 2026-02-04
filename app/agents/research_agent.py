from pathlib import Path

BASE = Path(__file__).resolve().parents[1] / "data" / "research"

def research_answer(query: str) -> str:
    docs = []
    for p in BASE.glob("*.md"):
        docs.append((p.name, p.read_text(encoding="utf-8", errors="ignore")))

    q = query.lower()
    hits = []
    for name, text in docs:
        score = sum(1 for w in q.split() if w in text.lower())
        if score > 0:
            hits.append((score, name, text))

    hits.sort(reverse=True, key=lambda x: x[0])
    if not hits:
        return "Судалгааны хадгалсан тэмдэглэл/материал дотроос тохирох мэдээлэл олдсонгүй."

    score, name, text = hits[0]
    return f"Top match: {name} (score={score})\n\n{text[:1800]}"
