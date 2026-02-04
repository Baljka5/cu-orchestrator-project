from pydantic import BaseModel
from typing import Optional, Dict, Any

class ChatRequest(BaseModel):
    message: str
    force_agent: Optional[str] = None

class ChatResponse(BaseModel):
    answer: str
    meta: Dict[str, Any] = {}

class GuardResult(BaseModel):
    allowed: bool
    reason: Optional[str] = None

class ClassificationResult(BaseModel):
    agent: str
    confidence: float
    rationale: str

class OrchestratorState(BaseModel):
    raw_message: str
    normalized_message: Optional[str] = None

    forced_agent: Optional[str] = None

    guard: Optional[GuardResult] = None
    classification: Optional[ClassificationResult] = None

    agent_result: Optional[str] = None
    final_answer: Optional[str] = None

    meta: Dict[str, Any] = {}
