import os
import json
from flask import Flask, render_template, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# ── DATABASE CONNECTION ──
def get_db():
    conn = psycopg2.connect(
        os.environ.get("DATABASE_URL"),
        cursor_factory=RealDictCursor
    )
    return conn

# ── CREATE TABLE IF IT DOESN'T EXIST ──
def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            date TEXT NOT NULL,
            description TEXT,
            lat REAL NOT NULL,
            lng REAL NOT NULL
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

# ── ROUTES ──
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/sales", methods=["GET"])
def get_sales():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales ORDER BY id DESC")
    sales = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([dict(s) for s in sales])

@app.route("/api/sales", methods=["POST"])
def add_sale():
    data = request.get_json()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sales (title, date, description, lat, lng)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING *
    """, (
        data["title"],
        data["date"],
        data.get("description", ""),
        data["lat"],
        data["lng"]
    ))
    new_sale = dict(cur.fetchone())
    conn.commit()
    cur.close()
    conn.close()
    return jsonify(new_sale), 201

@app.route("/api/sales/<int:sale_id>", methods=["DELETE"])
def delete_sale(sale_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM sales WHERE id = %s", (sale_id,))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Deleted!"})

# ── START ──
with app.app_context():
    init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

# ── CREATE TABLE ON STARTUP ──
init_db()