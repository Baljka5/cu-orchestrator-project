from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List

class ChatRequest(BaseModel):
    user_id: Optional[str] = None
    message: str = Field(..., min_length=1)

class ChatResponse(BaseModel):
    answer: str
    meta: Dict[str, Any] = Field(default_factory=dict)

class GuardResult(BaseModel):
    allowed: bool
    reason: str = ""
    labels: List[str] = Field(default_factory=list)

class ClassificationResult(BaseModel):
    agent: str
    confidence: float = 0.0
    rationale: str = ""

class OrchestratorState(BaseModel):
    raw_message: str
    normalized_message: str = ""
    guard: GuardResult | None = None
    classification: ClassificationResult | None = None
    agent_result: str = ""
    final_answer: str = ""
    meta: Dict[str, Any] = Field(default_factory=dict)
