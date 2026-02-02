from pydantic import BaseModel

class AskRequest(BaseModel):
    query: str

class AskResponse(BaseModel):
    answer: str
    route: str | None = None
    label: str | None = None
    confidence: float | None = None
