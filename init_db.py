import sqlite3

DB = "reviews.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

cur.execute("PRAGMA journal_mode=WAL;")
cur.execute("PRAGMA synchronous=NORMAL;")
cur.execute("PRAGMA busy_timeout=30000;")
cur.execute("PRAGMA foreign_keys=ON;")

cur.execute("""
CREATE TABLE IF NOT EXISTS reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  review_id TEXT,
  product_id TEXT NOT NULL,
  review_text TEXT NOT NULL,
  sentiment TEXT CHECK (sentiment IN ('positive','neutral','negative')),
  keywords TEXT,
  entities TEXT,
  ts_utc TEXT NOT NULL    -- ISO8601 UTC, e.g. 2025-08-15T18:30:00Z
);
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_product_ts ON reviews(product_id, ts_utc);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_sentiment_ts ON reviews(sentiment, ts_utc);")

cur.execute("""
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_id TEXT NOT NULL,
  rule TEXT NOT NULL,            -- e.g. 'neg>=5_in_10m'
  window_start_utc TEXT NOT NULL,
  window_end_utc TEXT NOT NULL,
  count INTEGER NOT NULL,
  created_at_utc TEXT NOT NULL
);
""")

# optional: prevent duplicate alerts for same product+window_end
cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_alert ON alerts(product_id, rule, window_end_utc);")

conn.commit(); conn.close()
print("Created", DB)
