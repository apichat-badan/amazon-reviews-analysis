from __future__ import annotations
import sqlite3, json, math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel, Field

DB_PATH = "reviews.db"

# ---------- Pydantic models ----------
class Review(BaseModel):
    id: int
    review_id: Optional[str]
    product_id: str
    review_text: str
    sentiment: Optional[str]
    keywords: List[str] = Field(default_factory=list)
    entities: List[str] = Field(default_factory=list)
    ts_utc: str

class TrendPoint(BaseModel):
    bucket_utc: str
    positive: int
    neutral: int
    negative: int

class KeywordStat(BaseModel):
    keyword: str
    count: int

# ---------- DB dependency ----------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # keep reads reliable under concurrent writer/alert process
    conn.execute("PRAGMA busy_timeout=30000;")
    yield conn
    conn.close()

# ---------- helpers ----------
def parse_json_list(txt: Optional[str]) -> List[str]:
    if not txt:
        return []
    try:
        v = json.loads(txt)
        if isinstance(v, list):
            return [str(x) for x in v]
    except Exception:
        pass
    return []

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def floor_to_bucket(ts: datetime, bucket_minutes: int) -> datetime:
    sec = int(ts.timestamp())
    b = bucket_minutes * 60
    floored = (sec // b) * b
    return datetime.fromtimestamp(floored, tz=timezone.utc)

# ---------- FastAPI ----------
app = FastAPI(title="Amazon Reviews API", version="1.0")

@app.get("/healthz")
def healthz(conn: sqlite3.Connection = Depends(get_conn)) -> Dict[str, Any]:
    # simple read to prove the DB is accessible
    _ = conn.execute("SELECT 1").fetchone()
    return {"ok": True}

# 1) Latest reviews for a product
@app.get("/reviews/{product_id}", response_model=List[Review])
def get_reviews(
    product_id: str,
    limit: int = Query(50, ge=1, le=500),
    since_minutes: int = Query(1440, ge=1, description="Lookback window in minutes")
, conn: sqlite3.Connection = Depends(get_conn)):
    now = utcnow()
    start = now - timedelta(minutes=since_minutes)
    rows = conn.execute(
        """
        SELECT id, review_id, product_id, review_text, sentiment, keywords, entities, ts_utc
        FROM reviews
        WHERE product_id = ?
          AND datetime(ts_utc) BETWEEN datetime(?) AND datetime(?)
        ORDER BY datetime(ts_utc) DESC
        LIMIT ?
        """,
        (product_id, start.strftime("%Y-%m-%dT%H:%M:%SZ"), now.strftime("%Y-%m-%dT%H:%M:%SZ"), limit),
    ).fetchall()

    out: List[Review] = []
    for r in rows:
        out.append(Review(
            id=r["id"],
            review_id=r["review_id"],
            product_id=r["product_id"],
            review_text=r["review_text"],
            sentiment=r["sentiment"],
            keywords=parse_json_list(r["keywords"]),
            entities=parse_json_list(r["entities"]),
            ts_utc=r["ts_utc"],
        ))
    return out

# 2) Sentiment trend in time buckets
@app.get("/sentiment_trend/{product_id}", response_model=List[TrendPoint])
def get_sentiment_trend(
    product_id: str,
    window_minutes: int = Query(120, ge=5, le=24*60),
    bucket_minutes: int = Query(5, ge=1, le=60)
, conn: sqlite3.Connection = Depends(get_conn)):
    now = utcnow()
    start = now - timedelta(minutes=window_minutes)

    rows = conn.execute(
        """
        SELECT sentiment, ts_utc
        FROM reviews
        WHERE product_id = ?
          AND sentiment IN ('positive','neutral','negative')
          AND datetime(ts_utc) BETWEEN datetime(?) AND datetime(?)
        """,
        (product_id, start.strftime("%Y-%m-%dT%H:%M:%SZ"), now.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ).fetchall()

    # pre-build empty buckets
    buckets: Dict[str, Dict[str, int]] = {}
    t = floor_to_bucket(start, bucket_minutes)
    end_bucket = floor_to_bucket(now, bucket_minutes)
    while t <= end_bucket:
        key = t.strftime("%Y-%m-%dT%H:%M:%SZ")
        buckets[key] = {"positive": 0, "neutral": 0, "negative": 0}
        t += timedelta(minutes=bucket_minutes)

    # fill buckets
    for r in rows:
        try:
            ts = datetime.strptime(r["ts_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            # skip malformed timestamps
            continue
        key = floor_to_bucket(ts, bucket_minutes).strftime("%Y-%m-%dT%H:%M:%SZ")
        if key in buckets:
            buckets[key][r["sentiment"]] += 1

    # format
    out: List[TrendPoint] = []
    for key in sorted(buckets.keys()):
        counts = buckets[key]
        out.append(TrendPoint(
            bucket_utc=key,
            positive=counts["positive"],
            neutral=counts["neutral"],
            negative=counts["negative"],
        ))
    return out

# 3) Recent keywords (frequency) for a product
@app.get("/keywords/{product_id}", response_model=List[KeywordStat])
def get_keywords(
    product_id: str,
    since_minutes: int = Query(1440, ge=5, le=7*24*60),
    topk: int = Query(20, ge=1, le=200)
, conn: sqlite3.Connection = Depends(get_conn)):
    now = utcnow()
    start = now - timedelta(minutes=since_minutes)
    rows = conn.execute(
        """
        SELECT keywords
        FROM reviews
        WHERE product_id = ?
          AND datetime(ts_utc) BETWEEN datetime(?) AND datetime(?)
        """,
        (product_id, start.strftime("%Y-%m-%dT%H:%M:%SZ"), now.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ).fetchall()

    freq: Dict[str, int] = {}
    for r in rows:
        for kw in parse_json_list(r["keywords"]):
            k = kw.strip().lower()
            if not k:
                continue
            freq[k] = freq.get(k, 0) + 1

    # top-k by count desc, then alphabetically
    items = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))[:topk]
    return [KeywordStat(keyword=k, count=v) for k, v in items]

# 4) Optional: recent alerts
@app.get("/alerts/{product_id}", response_model=List[Dict[str, Any]])
def get_alerts(
    product_id: str,
    limit: int = Query(20, ge=1, le=200)
, conn: sqlite3.Connection = Depends(get_conn)):
    rows = conn.execute(
        """
        SELECT id, product_id, rule, window_start_utc, window_end_utc, count, created_at_utc
        FROM alerts
        WHERE product_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (product_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]
