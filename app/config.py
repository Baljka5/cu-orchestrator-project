import os
from dotenv import load_dotenv

load_dotenv(override=True)

def env(key: str, default: str = "") -> str:
    return os.getenv(key, default)

APP_ENV = env("APP_ENV", "dev")
APP_NAME = env("APP_NAME", "CU Orchestrator")
LOG_LEVEL = env("LOG_LEVEL", "INFO")

LLM_BASE_URL = env("LLM_BASE_URL", "http://localhost:8001/v1")
LLM_API_KEY = env("LLM_API_KEY", "local-key")
LLM_MODEL = env("LLM_MODEL", "meta-llama/Llama-3.1-8B-Instruct")

GUARD_BLOCKLIST = [x.strip() for x in env("GUARD_BLOCKLIST", "").split(",") if x.strip()]
MAX_INPUT_CHARS = int(env("MAX_INPUT_CHARS", "4000"))

SQLITE_PATH = env("SQLITE_PATH", "/app/app/data/demo.db")


SCHEMA_DICT_PATH = env("SCHEMA_DICT_PATH", "/app/app/data/dict/dictionary.xlsx")

CLICKHOUSE_HOST = env("CLICKHOUSE_HOST", "")
CLICKHOUSE_PORT = int(env("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = env("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = env("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = env("CLICKHOUSE_DATABASE", "")

CH_MAX_ROWS = int(env("CH_MAX_ROWS", "200"))
