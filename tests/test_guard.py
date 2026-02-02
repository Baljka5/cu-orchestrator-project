import pytest
from src.ingestion.guard import guard_prompt

@pytest.mark.asyncio
async def test_guard_allows_normal_question(monkeypatch):
    # Monkeypatch LLM call to avoid network dependency in unit tests
    async def fake_guard(q):
        return True, "ALLOW"
    monkeypatch.setattr("src.ingestion.guard.guard_prompt", fake_guard)
