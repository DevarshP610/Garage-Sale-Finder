import os
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, jsonify, request, redirect, url_for, session
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")

oauth = OAuth(app)

google = oauth.register(
    name='google',
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

def get_db():
    return psycopg2.connect(
        os.environ.get("DATABASE_URL"),
        cursor_factory=RealDictCursor
    )

def init_db():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            date TEXT NOT NULL,
            description TEXT,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            user_email TEXT,
            user_name TEXT,
            user_picture TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.route("/")
def home():
    user = session.get("user")
    return render_template("index.html", user=user)

@app.route("/login")
def login():
    redirect_uri = url_for("auth_callback", _external=True, _scheme="https")
    return google.authorize_redirect(redirect_uri)

@app.route("/auth/callback")
def auth_callback():
    token = google.authorize_access_token()
    user  = token.get("userinfo")
    session["user"] = {
        "email":   user["email"],
        "name":    user["name"],
        "picture": user["picture"]
    }
    return redirect("/")

@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect("/")

@app.route("/api/me")
def me():
    return jsonify(session.get("user"))

@app.route("/api/sales", methods=["GET"])
def get_sales():
    conn = get_db()
    cur  = conn.cursor()
    # Only return sales where the date is today or in the future
    # Also delete expired ones automatically
    cur.execute("DELETE FROM sales WHERE date < CURRENT_DATE::text")
    cur.execute("SELECT * FROM sales ORDER BY id DESC")
    sales = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return jsonify([dict(s) for s in sales])

@app.route("/api/sales", methods=["POST"])
def add_sale():
    user = session.get("user")
    if not user:
        return jsonify({"error": "Login required"}), 401
    data = request.get_json()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO sales (title, date, description, lat, lng, user_email, user_name, user_picture)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (
        data["title"],
        data["date"],
        data.get("description", ""),
        data["lat"],
        data["lng"],
        user["email"],
        user["name"],
        user["picture"]
    ))
    new_sale = dict(cur.fetchone())
    conn.commit()
    cur.close()
    conn.close()
    return jsonify(new_sale), 201

@app.route("/api/scrape", methods=["POST"])
def trigger_scrape():
    # Only you can trigger this
    secret = request.headers.get("X-Secret")
    if secret != os.environ.get("SCRAPE_SECRET", "mysecret"):
        return jsonify({"error": "Unauthorized"}), 401
    
    from scraper import run
    run("winnipeg")
    return jsonify({"message": "Scrape complete!"})

@app.route("/api/sales/<int:sale_id>", methods=["DELETE"])
def delete_sale(sale_id):
    user = session.get("user")
    if not user:
        return jsonify({"error": "Login required"}), 401
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        "DELETE FROM sales WHERE id = %s AND user_email = %s",
        (sale_id, user["email"])
    )
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Deleted!"})

with app.app_context():
    init_db()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)