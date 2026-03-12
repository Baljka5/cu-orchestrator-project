# app/db/chat_history.py
import json
import logging
from typing import Any, Dict, Optional

import mysql.connector
from mysql.connector import pooling

from app.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    MYSQL_POOL_NAME,
    MYSQL_POOL_SIZE,
)

logger = logging.getLogger(__name__)

_pool: Optional[pooling.MySQLConnectionPool] = None


def get_mysql_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name=MYSQL_POOL_NAME,
            pool_size=MYSQL_POOL_SIZE,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset="utf8mb4",
            use_unicode=True,
            autocommit=False,
        )
    return _pool


def _safe_json_dumps(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def save_chat_history(
        *,
        user_query: str,
        answer_text: Optional[str] = None,
        generated_sql: Optional[str] = None,
        agent_name: str = "text2sql",
        mode: Optional[str] = None,
        rule_name: Optional[str] = None,
        error_code: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
) -> Optional[int]:
    conn = None
    cur = None
    try:
        pool = get_mysql_pool()
        conn = pool.get_connection()
        cur = conn.cursor()

        sql = """
        INSERT INTO llm_chat_history (
            session_id,
            user_query,
            answer_text,
            generated_sql,
            agent_name,
            mode,
            rule_name,
            error_code,
            meta_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cur.execute(
            sql,
            (
                session_id,
                user_query,
                answer_text,
                generated_sql,
                agent_name,
                mode,
                rule_name,
                error_code,
                _safe_json_dumps(meta or {}),
            ),
        )
        conn.commit()
        return cur.lastrowid

    except Exception as e:
        logger.exception("Failed to save chat history: %s", e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return None

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
