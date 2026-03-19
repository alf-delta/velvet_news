"""
NOIR — Web interface for Media Monitor
Run: python app.py
Open: http://localhost:5000
"""

import threading
from datetime import datetime
from flask import Flask, jsonify, request, render_template
import sqlite3

from monitor import DB_PATH, init_db, run_pipeline, SOURCES

app = Flask(__name__)

fetch_state = {"running": False, "last_run": None, "last_new": 0, "error": None}


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ── Pages ─────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── API ───────────────────────────────────────────────────────

@app.route("/api/articles")
def api_articles():
    topic   = request.args.get("topic")
    source  = request.args.get("source")
    fmt     = request.args.get("format")
    unread   = request.args.get("unread") == "1"
    limit    = min(int(request.args.get("limit", 30)), 100)
    offset   = int(request.args.get("offset", 0))
    after_id = int(request.args.get("after_id", 0))

    q = """SELECT id, source, url, title, date, topic, format_type,
                  length_est, snippet, fetched_at, read_flag
           FROM articles WHERE 1=1"""
    params = []

    if topic:
        q += " AND topic = ?"
        params.append(topic)
    if source:
        q += " AND source = ?"
        params.append(source)
    if fmt:
        q += " AND format_type = ?"
        params.append(fmt)
    if unread:
        q += " AND read_flag = 0"
    if after_id:
        q += " AND id > ?"
        params.append(after_id)

    q += " ORDER BY id ASC LIMIT ? OFFSET ?" if after_id else " ORDER BY fetched_at DESC, date DESC LIMIT ? OFFSET ?"
    params += [limit, offset]

    conn = get_conn()
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/articles/<int:article_id>/read", methods=["POST"])
def api_mark_read(article_id):
    conn = get_conn()
    conn.execute("UPDATE articles SET read_flag = 1 WHERE id = ?", (article_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/read_all", methods=["POST"])
def api_read_all():
    data   = request.get_json(silent=True) or {}
    topic  = data.get("topic")
    conn   = get_conn()
    if topic:
        conn.execute("UPDATE articles SET read_flag = 1 WHERE topic = ?", (topic,))
    else:
        conn.execute("UPDATE articles SET read_flag = 1")
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/stats")
def api_stats():
    conn = get_conn()

    topics = conn.execute("""
        SELECT topic,
               COUNT(*) as total,
               SUM(CASE WHEN read_flag = 0 THEN 1 ELSE 0 END) as unread
        FROM articles
        GROUP BY topic
        ORDER BY total DESC
    """).fetchall()

    totals = conn.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN read_flag = 0 THEN 1 ELSE 0 END),
               COALESCE(MAX(id), 0)
        FROM articles
    """).fetchone()

    sources = conn.execute("""
        SELECT source, COUNT(*) as total
        FROM articles
        GROUP BY source
        ORDER BY total DESC
    """).fetchall()

    conn.close()

    return jsonify({
        "total":   totals[0] or 0,
        "unread":  totals[1] or 0,
        "max_id":  totals[2] or 0,
        "topics":  [dict(r) for r in topics],
        "sources": [dict(r) for r in sources],
        "fetch":   fetch_state,
    })


@app.route("/api/fetch", methods=["POST"])
def api_fetch():
    if fetch_state["running"]:
        return jsonify({"ok": False, "message": "Already running"})

    def do_fetch():
        fetch_state["running"] = True
        fetch_state["error"]   = None
        try:
            new = run_pipeline()
            fetch_state["last_new"] = new
            fetch_state["last_run"] = datetime.now().strftime("%H:%M")
        except Exception as e:
            fetch_state["error"] = str(e)
        finally:
            fetch_state["running"] = False

    threading.Thread(target=do_fetch, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/sources")
def api_sources():
    return jsonify([s["name"] for s in SOURCES])


# ── Main ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    init_db()
    port = int(os.environ.get("PORT", 8080))
    print(f"\n  NOIR is running → http://localhost:{port}\n")
    app.run(debug=False, host="0.0.0.0", port=port)
