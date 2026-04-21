"""
Postgres 공통 헬퍼. dashboard/loader/notifier 모두 여기를 사용.
"""
import os
import json
import psycopg
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_CFG = os.path.join(BASE_DIR, "config.json")


def get_dsn() -> str:
    """환경변수 우선, 없으면 config.json"""
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn
    if os.path.exists(APP_CFG):
        with open(APP_CFG, "r", encoding="utf-8") as f:
            return json.load(f).get("database_url", "")
    return ""


def connect(**kwargs) -> psycopg.Connection:
    dsn = get_dsn()
    if not dsn:
        raise RuntimeError("DATABASE_URL이 설정되지 않았습니다.")
    return psycopg.connect(dsn, **kwargs)


def query_df(sql: str, params=None, conn=None) -> pd.DataFrame:
    """쿼리를 DataFrame으로 반환"""
    close = False
    if conn is None:
        conn = connect(autocommit=True)
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if cur.description is None:
                return pd.DataFrame()
            cols = [d.name for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        if close:
            conn.close()
