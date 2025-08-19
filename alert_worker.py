
# alerts.py
import time
import sqlite3
import datetime as dt

DB = "reviews.db"

FIND = """
SELECT product_id, COUNT(*) AS c
FROM reviews
WHERE sentiment='negative' AND ts_utc BETWEEN ? AND ?
GROUP BY product_id
HAVING c >= 5;
"""

INSERT = """
INSERT OR IGNORE INTO alerts
(product_id, rule, window_start_utc, window_end_utc, count, created_at_utc)
VALUES (?, 'neg>=5_in_10m', ?, ?, ?, ?);
"""

# cooldown: suppress repeats if an alert for the same product was created in the last 10 minutes
RECENT_ALERT_EXISTS = """
SELECT 1
FROM alerts
WHERE product_id = ?
  AND rule = 'neg>=5_in_10m'
  AND created_at_utc >= ?
LIMIT 1;
"""

def check_once(conn):
    now = dt.datetime.utcnow()
    start = now - dt.timedelta(minutes=10)
    s = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    e = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = conn.execute(FIND, (s, e)).fetchall()
    for product_id, cnt in rows:
        # cooldown window: 10 minutes back from now
        cooldown_start = (now - dt.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
        recent = conn.execute(RECENT_ALERT_EXISTS, (product_id, cooldown_start)).fetchone()
        if recent:
            continue  # suppress duplicate alert within cooldown window

        conn.execute(INSERT, (product_id, s, e, cnt, e))
        print(f"[ALERT] product={product_id} negatives={cnt} window=[{s},{e}]")

    conn.commit()

def main():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA busy_timeout=30000;")
    while True:
        try:
            check_once(conn)
        except Exception as ex:
            print("alert error:", ex)
        time.sleep(60)

if __name__ == "__main__":
    main()
