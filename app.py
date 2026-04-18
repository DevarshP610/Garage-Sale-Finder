import json
import os
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

DATA_FILE = "data.json"

# ── Helpers first, BEFORE app.run() ──
def load_sales():
    try:
        if not os.path.exists(DATA_FILE):
            save_sales([])
            return []
        with open(DATA_FILE, "r") as file:
            content = file.read().strip()
            if not content:
                return []
            return json.loads(content)
    except Exception:
        return []
    

def save_sales(data):
    with open(DATA_FILE, "w") as file:
        json.dump(data, file, indent=2)

# ── Routes ──
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/sales", methods=["GET"])
def get_sales():
    return jsonify(load_sales())   # ← reads from file, not fake data

@app.route("/api/sales", methods=["POST"])
def add_sale():
    new_sale = request.get_json()
    sales = load_sales()
    new_sale["id"] = len(sales) + 1
    sales.append(new_sale)
    save_sales(sales)
    return jsonify(new_sale), 201

@app.route("/api/sales/<int:sale_id>", methods=["DELETE"])
def delete_sale(sale_id):
    sales = load_sales()
    updated = [s for s in sales if s["id"] != sale_id]
    save_sales(updated)
    return jsonify({"message": "Deleted!"})

# ── Start the server LAST ──
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)