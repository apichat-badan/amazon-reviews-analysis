# dash_app.py
import json
import sqlite3
import os
from datetime import datetime, timedelta

import pandas as pd
from dash import Dash, dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
from flask import jsonify

# ---------- config ----------
DB = os.path.abspath("reviews.db")
ISO = "%Y-%m-%dT%H:%M:%SZ"

# ---------- SQL helper ----------
def q(sql: str, params=()):
    # open/close per call; WAL + busy timeout for safety
    conn = sqlite3.connect(DB)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.row_factory = sqlite3.Row
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    finally:
        conn.close()

# ---------- data access ----------
def get_products():
    # never fail the layout if DB happens to be missing/locked
    try:
        if not os.path.exists(DB):
            return ["P001"]
        df = q("SELECT DISTINCT product_id FROM reviews ORDER BY product_id")
        vals = df["product_id"].dropna().astype(str).tolist()
        return vals or ["P001"]
    except Exception:
        return ["P001"]

def get_trend(product_id: str, window_minutes=120, bucket_minutes=5):
    end_s = datetime.utcnow().strftime(ISO)
    start_s = (datetime.utcnow() - timedelta(minutes=window_minutes)).strftime(ISO)

    sql = """
    SELECT ts_utc, sentiment
    FROM reviews
    WHERE product_id = ?
      AND sentiment IN ('positive','neutral','negative')
      AND ts_utc BETWEEN ? AND ?
    """
    df = q(sql, (product_id, start_s, end_s))
    if df.empty:
        return pd.DataFrame(columns=["bucket_utc","positive","neutral","negative"])

    ts = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df = df.assign(ts=ts).dropna(subset=["ts"])
    if df.empty:
        return pd.DataFrame(columns=["bucket_utc","positive","neutral","negative"])

    bucket = f"{int(bucket_minutes)}min"
    df["bucket_utc"] = df["ts"].dt.floor(bucket).dt.strftime(ISO)

    agg = (
        df.groupby(["bucket_utc", "sentiment"])
          .size()
          .unstack(fill_value=0)
          .reindex(columns=["positive","neutral","negative"], fill_value=0)
          .reset_index()
          .sort_values("bucket_utc")
    )
    return agg

def get_keywords(product_id: str, since_minutes=1440, topk=20):
    end_s = datetime.utcnow().strftime(ISO)
    start_s = (datetime.utcnow() - timedelta(minutes=since_minutes)).strftime(ISO)

    sql = """
    SELECT keywords
    FROM reviews
    WHERE product_id = ? AND ts_utc BETWEEN ? AND ?
    """
    df = q(sql, (product_id, start_s, end_s))
    freq = {}
    for _, r in df.iterrows():
        try:
            arr = json.loads(r["keywords"] or "[]")
        except Exception:
            arr = []
        for kw in arr:
            k = str(kw).strip().lower()
            if k:
                freq[k] = freq.get(k, 0) + 1
    return pd.DataFrame(sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))[:topk],
                        columns=["keyword","count"])

def get_recent_reviews(product_id: str, limit=50):
    sql = """
    SELECT ts_utc, sentiment, substr(review_text, 1, 160) AS snippet
    FROM reviews
    WHERE product_id = ?
    ORDER BY ts_utc DESC
    LIMIT ?
    """
    return q(sql, (product_id, limit))

# ---------- app ----------
app = Dash(
    __name__,
    serve_locally=False,                  # ensure local asset serving
    suppress_callback_exceptions=True,   # avoid early callback wiring errors)
)

server = app.server  # for healthz
app.title = "Review Monitor"

# health endpoint so you can curl it
@server.route("/healthz")
def healthz():
    ok = os.path.exists(DB)
    return jsonify({"ok": ok, "db": DB})

products = get_products()

app.layout = html.Div([
    html.H1("Review Monitor"),
    html.Div([
        html.Label("Product"),
        dcc.Dropdown(options=products, value=products[0], id="product"),

        html.Label("Trend window (minutes)"),
        dcc.Slider(min=60, max=43200, step=60, value=10080,
                   marks={i: str(i) for i in range(60, 721, 60)},
                   id="win"),

        html.Label("Bucket (minutes)"),
        dcc.Slider(min=1, max=30, step=1, value=5,
                   marks={i: str(i) for i in range(1, 31, 5)},
                   id="bucket"),
    ], style={"maxWidth": "600px"}),
    dcc.Interval(id="refresh", interval=30_000, n_intervals=0),

    dcc.Graph(id="trend_graph"),
    dcc.Graph(id="kw_graph"),
    html.H3("Recent reviews"),
    dcc.Loading(html.Div(id="table_div")),
])

# ---------- callback ----------
@app.callback(
    Output("trend_graph", "figure"),
    Output("kw_graph", "figure"),
    Output("table_div", "children"),
    Input("product", "value"),
    Input("win", "value"),
    Input("bucket", "value"),
    Input("refresh","n_intervals"),
)
def update(product_id, window_minutes, bucket_minutes, _n):
    try:
        tdf = get_trend(product_id, window_minutes, bucket_minutes)
        kdf = get_keywords(product_id, since_minutes=window_minutes)
        rdf = get_recent_reviews(product_id, limit=50)

        # trend
        if tdf.empty:
            fig_t = px.line(title="Sentiment Trend (no data)")
        else:
            tdf_m = tdf.melt(id_vars=["bucket_utc"],
                             value_vars=["positive","neutral","negative"],
                             var_name="sentiment", value_name="count")
            fig_t = px.line(tdf_m, x="bucket_utc", y="count", color="sentiment",
                            title="Sentiment Trend")

        # keywords
        fig_k = px.bar(kdf, x="keyword", y="count",
                       title="Top Keywords") if not kdf.empty else px.bar(title="Top Keywords (no data)")

        # table
        rows = [
            html.Tr([html.Td(r["ts_utc"]), html.Td(r["sentiment"]), html.Td(r["snippet"])])
            for _, r in rdf.iterrows()
        ]
        table = html.Table(
            [html.Thead(html.Tr([html.Th("ts_utc"), html.Th("sentiment"), html.Th("snippet")]))] +
            [html.Tbody(rows)]
        )

        return fig_t, fig_k, table

    except Exception as ex:
        # render the error instead of hanging
        return px.line(title="Error"), px.bar(title="Error"), html.Pre(str(ex))

# ---------- main ----------
if __name__ == "__main__":
    print(f"[dash] DB: {DB} exists={os.path.exists(DB)} size={os.path.getsize(DB) if os.path.exists(DB) else 'NA'}")
    port = int(os.environ.get("PORT", "8052"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
