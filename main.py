import os
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from psycopg2.pool import SimpleConnectionPool
import psycopg2.extras

from pathlib import Path
from dotenv import load_dotenv


load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_DB   = os.getenv("PG_DB", "reviews")
PG_USER = os.getenv("PG_USER", "sgunnam")
PG_PASS = os.getenv("PG_PASS", "sushi")
PG_MIN  = int(os.getenv("PG_MIN_CONN", "1"))
PG_MAX  = int(os.getenv("PG_MAX_CONN", "5"))

pool = SimpleConnectionPool(
    PG_MIN, PG_MAX,
    host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS
)

app = FastAPI(title="Review Analytics API", version="1.0.0")

class Review(BaseModel):
    id: int
    review_id: Optional[str]
    product_id: str
    review_text: Optional[str]
    sentiment: Optional[str]
    keywords: Optional[list]
    entities: Optional[list]
    ts_utc: datetime

class TrendPoint(BaseModel):
    bucket_utc: datetime
    positive: int
    neutral: int
    negative: int

class KeywordStat(BaseModel):
    keyword: str
    count: int

def fetchall(sql: str, params: tuple = ()):
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        pool.putconn(conn)

@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/healthz")
def healthz():
    rows = fetchall("SELECT 1 AS ok;")
    return {"ok": rows[0]["ok"] == 1}

@app.get("/reviews/{product_id}", response_model=List[Review])
def latest_reviews(
    product_id: str,
    limit: int = Query(20, ge=1, le=200)
):
    sql = """
    SELECT id, review_id, product_id, review_text, sentiment,
           COALESCE(keywords, '[]'::jsonb) AS keywords,
           COALESCE(entities, '[]'::jsonb) AS entities,
           ts_utc
    FROM reviews
    WHERE product_id = %s
    ORDER BY ts_utc DESC
    LIMIT %s;
    """
    return fetchall(sql, (product_id, limit))

@app.get("/sentiment_trend/{product_id}", response_model=List[TrendPoint])
def sentiment_trend(
    product_id: str,
    hours: int = Query(24, ge=1, le=168),           # last N hours
    bucket_minutes: int = Query(10, ge=1, le=60)    # bucket width
):
    # Use time buckets; count per sentiment per bucket
    # Build dynamic interval safely
    sql = f"""
    WITH base AS (
      SELECT
        to_timestamp(floor(extract(epoch from ts_utc)/(%s*60))*(%s*60))::timestamptz AS bucket_utc,
        sentiment
      FROM reviews
      WHERE product_id = %s
        AND ts_utc >= now() - (%s || ' hours')::interval
        AND sentiment IN ('positive','neutral','negative')
    ),
    agg AS (
      SELECT bucket_utc,
             sum( (sentiment='positive')::int ) AS positive,
             sum( (sentiment='neutral')::int )  AS neutral,
             sum( (sentiment='negative')::int ) AS negative
      FROM base
      GROUP BY bucket_utc
    )
    SELECT bucket_utc, positive, neutral, negative
    FROM agg
    ORDER BY bucket_utc;
    """
    return fetchall(sql, (bucket_minutes, bucket_minutes, product_id, str(hours)))

@app.get("/keywords/{product_id}", response_model=List[KeywordStat])
def recent_keywords(
    product_id: str,
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(25, ge=1, le=200)
):
    # Explode keywords jsonb array and aggregate
    sql = """
    WITH exploded AS (
      SELECT lower(trim(k::text, '\"')) AS kw
      FROM reviews
      WHERE product_id = %s
        AND ts_utc >= now() - (%s || ' hours')::interval
        AND keywords IS NOT NULL
      CROSS JOIN LATERAL jsonb_array_elements_text(keywords) AS k
    )
    SELECT kw AS keyword, count(*)::int AS count
    FROM exploded
    WHERE kw <> ''
    GROUP BY kw
    ORDER BY count DESC, kw
    LIMIT %s;
    """
    return fetchall(sql, (product_id, str(hours), limit))
