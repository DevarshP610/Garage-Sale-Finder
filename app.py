# 1. Imports are grouped cleanly at the very top
from flask import Flask, render_template, jsonify
import json

app = Flask(__name__)

# 2. Our mock data
sales_data = [
    {"id": 1, "title": "Vintage Records", "lat": 43.65, "lng": -79.38}
]

# 3. All our routes go next
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/sales", methods=["GET"])
def get_sales():
    return jsonify(sales_data)

# 4. Turn the key!
# What exact two lines of code go here to start the server?

if __name__ == "__main__":
    app.run(debug=True)

def load_sales():
    with open("sales.json") as file:
        return json.load(file)
    
def save_sales(data):
    with open("data.json", "w") as file:
        json.dump(data, file)