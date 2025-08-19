# ingest_worker.py
import csv, json, time, sqlite3, ast
from datetime import datetime

DB = "reviews.db"
ISO = "%Y-%m-%dT%H:%M:%SZ"
ALLOWED = {"positive","neutral","negative"}

def open_db():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=30000;")
    return c

def parse_list(val):
    if not val: return []
    s = str(val).strip()
    # try strict JSON
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, list) else []
    except Exception:
        pass
    # try Python literal lists like "['a', 'b']"
    try:
        obj = ast.literal_eval(s)
        return obj if isinstance(obj, list) else []
    except Exception:
        pass
    # fallback: comma-separated string
    return [x.strip() for x in s.split(",") if x.strip()]

def norm_sentiment(s):
    s = (s or "").strip().lower()
    return s if s in ALLOWED else "neutral"

def insert_review(c, r):
    c.execute("""
        INSERT INTO reviews (review_id, product_id, review_text, sentiment, keywords, entities, ts_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        r["review_id"],
        r["product_id"],
        r["review_text"],
        r["sentiment"],
        json.dumps(r.get("keywords", []), ensure_ascii=False),
        json.dumps(r.get("entities", []), ensure_ascii=False),
        r["ts_utc"],
    ))
    c.commit()

def loop_from_csv(csv_path="stream_output.csv", product_id="P001", sleep_sec=5):
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        i = 0
        for raw in rdr:
            review_text = raw.get("reviewText") or raw.get("review_text") or ""
            if not review_text:
                continue
            row = {
                "review_id": f"sim-{i}",
                "product_id": product_id,
                "review_text": review_text,
                "sentiment": norm_sentiment(raw.get("sentiment")),
                "keywords": parse_list(raw.get("keywords")),
                "entities": parse_list(raw.get("entities")),
            }
            rows.append(row); i += 1

    if not rows:
        print("No usable rows in CSV. Exiting."); return

    c = open_db()
    i = 0
    while True:
        r = rows[i % len(rows)].copy()
        r["review_id"] = f"sim-{i}-{int(time.time())}"
        r["ts_utc"] = datetime.utcnow().strftime(ISO)
        try:
            insert_review(c, r)
            print(f"inserted {r['review_id']} {r['sentiment']}")
        except Exception as e:
            print("insert error:", e)
        i += 1
        time.sleep(sleep_sec)

if __name__ == "__main__":
    loop_from_csv()
