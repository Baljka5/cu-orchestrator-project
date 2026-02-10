from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any


class ChatRequest(BaseModel):
    message: str
    force_agent: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    meta: Dict[str, Any] = Field(default_factory=dict)

    sql: Optional[str] = None
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)


class ClassificationResult(BaseModel):
    agent: str = "general"
    confidence: float = 0.3
    rationale: str = "fallback_general"


class OrchestratorState(BaseModel):
    raw_message: str
    normalized_message: Optional[str] = None

    forced_agent: Optional[str] = None
    classification: Optional[ClassificationResult] = None

    final_answer: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)
