# inject_negatives.py
import sqlite3, datetime as dt, json, uuid

DB = "reviews.db"
PRODUCT = "P001"   # must match DEFAULT_PRODUCT used in backfill
N = 7

now = dt.datetime.utcnow()
start = now - dt.timedelta(minutes=9)

rows = []
for i in range(N):
    ts = start + dt.timedelta(seconds=30*i)
    rows.append((
        f"inj-{uuid.uuid4().hex[:8]}",
        PRODUCT,
        "Battery died in a week. Refund refused.",
        "negative",
        json.dumps(["battery","refund","poor quality"]),
        "[]",
        ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    ))

conn = sqlite3.connect(DB)
conn.execute("PRAGMA busy_timeout=30000;")
conn.executemany("""
INSERT INTO reviews (review_id, product_id, review_text, sentiment, keywords, entities, ts_utc)
VALUES (?, ?, ?, ?, ?, ?, ?)
""", rows)
conn.commit(); conn.close()
print(f"Injected {N} negatives for product {PRODUCT}")

