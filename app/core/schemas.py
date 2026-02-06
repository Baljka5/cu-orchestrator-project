from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    force_agent: Optional[str] = None

class ChatResponse(BaseModel):
    answer: str
    meta: Dict[str, Any] = {}

class ClassificationResult(BaseModel):
    agent: str
    confidence: float = 0.0
    rationale: str = ""

class OrchestratorState(BaseModel):
    raw_message: str
    forced_agent: Optional[str] = None

    normalized_message: Optional[str] = None
    classification: Optional[ClassificationResult] = None

    final_answer: Optional[str] = None
    meta: Dict[str, Any] = {}
