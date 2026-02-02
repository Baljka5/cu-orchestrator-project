from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

class Settings(BaseModel):
    app_name: str = os.getenv("APP_NAME", "CU Orchestrator")
    env: str = os.getenv("ENV", "dev")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    llm_base_url: str = os.getenv("LLM_BASE_URL", "http://localhost:8001")
    llm_model: str = os.getenv("LLM_MODEL", "meta-llama/Llama-3.1-8B-Instruct")
    llm_temperature: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    llm_max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "512"))
    llm_timeout: int = int(os.getenv("LLM_TIMEOUT", "60"))

    classifier_min_conf: float = float(os.getenv("CLASSIFIER_MIN_CONF", "0.55"))

    enable_policy_agent: bool = os.getenv("ENABLE_POLICY_AGENT", "true").lower() == "true"
    enable_text2sql_agent: bool = os.getenv("ENABLE_TEXT2SQL_AGENT", "true").lower() == "true"
    enable_research_agent: bool = os.getenv("ENABLE_RESEARCH_AGENT", "true").lower() == "true"

settings = Settings()
