from src.config import settings

def route(label: str, conf: float) -> str:
    if conf < settings.classifier_min_conf:
        return "other"

    if label == "policy" and settings.enable_policy_agent:
        return "policy_agent"
    if label == "text2sql" and settings.enable_text2sql_agent:
        return "text2sql_agent"
    if label == "research" and settings.enable_research_agent:
        return "research_agent"
    return "other"
