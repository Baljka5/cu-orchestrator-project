from pydantic import BaseModel, Field
from typing import Optional, Dict, Any


class ChatRequest(BaseModel):
    message: str
    force_agent: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    meta: Dict[str, Any] = Field(default_factory=dict)


class OrchestratorState(BaseModel):
    raw_message: str
    normalized_message: Optional[str] = None
    forced_agent: Optional[str] = None

    classification: Optional[Dict[str, Any]] = None
    final_answer: Optional[str] = None

    # debugging / extra info
    meta: Dict[str, Any] = Field(default_factory=dict)
