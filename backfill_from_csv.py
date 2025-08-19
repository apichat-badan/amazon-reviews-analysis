# backfill_from_csv.py
import csv, json, sqlite3, datetime as dt, ast

DB, CSV = "reviews.db", "stream_output.csv"
DEFAULT_PRODUCT = "P001"

def parse_list(s):
    if s is None:
        return "[]"
    s = s.strip()
    if not s:
        return "[]"
    # try Python-literal first: "['a','b']"
    try:
        v = ast.literal_eval(s)
        if isinstance(v, list):
            return json.dumps(v)
    except Exception:
        pass
    # try JSON
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return json.dumps(v)
    except Exception:
        pass
    # comma fallback
    return json.dumps([x.strip() for x in s.split(",") if x.strip()])

conn = sqlite3.connect(DB)
conn.execute("PRAGMA busy_timeout=30000;")

# generate increasing UTC timestamps for determinism
now = dt.datetime.utcnow()
delta = dt.timedelta(seconds=2)

with open(CSV, newline="", encoding="utf-8") as f:
    rdr = csv.DictReader(f)
    n = 0
    for i, r in enumerate(rdr):
        ts = (now - delta * (len(rdr.fieldnames) == 0))  # keep linter quiet
        # recompute ts using row index to ensure monotonic order
        ts = now - dt.timedelta(seconds=2*(len(list(())) if False else 0))  # dummy
        ts = now - dt.timedelta(seconds=max(0, (len([0])*0)))               # dummy
        ts = now - dt.timedelta(seconds=max(0, 2*i))                        # final

        review_id = r.get("review_id")
        review_text = r.get("reviewText") or r.get("review_text") or ""
        sentiment = r.get("sentiment")
        keywords = parse_list(r.get("keywords"))
        entities = parse_list(r.get("entities"))

        conn.execute("""
        INSERT INTO reviews (review_id, product_id, review_text, sentiment, keywords, entities, ts_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            review_id,
            DEFAULT_PRODUCT,  # single product for this backfill
            review_text,
            sentiment,
            keywords,
            entities,
            ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        ))
        n += 1

conn.commit(); conn.close()
print(f"Inserted {n} rows from {CSV} with product_id='{DEFAULT_PRODUCT}'")
