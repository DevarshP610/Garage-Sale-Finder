import requests
import json
import os
import sys
import time
import urllib.parse
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from google import genai

# ── ENVIRONMENT CHECK ──
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not GEMINI_API_KEY or not DATABASE_URL:
    print("🚨 ERROR: Missing Environment Variables!")
    sys.exit(1)

try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"🚨 ERROR initializing Gemini: {e}")
    sys.exit(1)

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

# ── GEOCODER ──
def geocode_location(address, city):
    fallbacks = {
        "winnipeg": (49.8951, -97.1384),
        "brandon": (49.8485, -99.9501)
    }
    city_lower = city.lower()
    default_lat, default_lng = fallbacks.get(city_lower, (49.8951, -97.1384))

    if not address or address.lower() in ["none", "n/a", "unknown", ""]:
        return default_lat, default_lng

    try:
        search_query = f"{address}, {city}, Manitoba"
        encoded_query = urllib.parse.quote(search_query)
        url = f"https://nominatim.openstreetmap.org/search?q={encoded_query}&format=json&limit=1"
        
        headers = {"User-Agent": "GarageSaleFinder/1.0"}
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        time.sleep(1) 
        
        if data:
            print(f"📍 Found GPS for '{address}'")
            return float(data[0]["lat"]), float(data[0]["lon"])
        else:
            print(f"⚠️ No exact GPS for '{address}'. Using city center.")
    except Exception as e:
        print(f"🚨 Geocoding failed: {e}")

    return default_lat, default_lng

# ── SCRAPE KIJIJI ──
def scrape_kijiji(city):
    url = f"https://www.kijiji.ca/b-garage-sale/{city}/k0l0"
    headers = {"User-Agent": "Mozilla/5.0"}
    print(f"Scraping Kijiji for {city}...")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
    except: return []

    listings = []
    cards = soup.find_all("li", {"class": lambda c: c and "regular-ad" in c})
    for card in cards[:10]:
        try:
            title = card.find("div", {"class": lambda c: c and "title" in c})
            desc  = card.find("div", {"class": lambda c: c and "description" in c})
            listing = {
                "title": title.get_text(strip=True) if title else "",
                "desc": desc.get_text(strip=True) if desc else ""
            }
            if listing["title"]: listings.append(listing)
        except: continue
    return listings

# ── SCRAPE CRAIGSLIST ──
def scrape_craigslist(city):
    url = f"https://{city}.craigslist.org/search/gss"
    headers = {"User-Agent": "Mozilla/5.0"}
    print(f"Scraping Craigslist for {city}...")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200: return [] 
        soup = BeautifulSoup(response.text, "html.parser")
    except: return []

    listings = []
    cards = soup.find_all("li", {"class": "cl-static-search-result"})
    for card in cards[:10]:
        try:
            title = card.find("div", {"class": "title"})
            listing = {
                "title": title.get_text(strip=True) if title else "",
                "desc": ""
            }
            if listing["title"]: listings.append(listing)
        except: continue
    return listings

# ── USE GEMINI TO EXTRACT ──
def extract_with_ai(listings, city):
    if not listings: return []
    prompt = f"""You are a data extractor for {city}, Manitoba.
Extract structured data. For 'street_address', find the exact street or intersection. 
Return JSON array of objects:
- title: string
- description: string
- date: MUST be YYYY-MM-DD. If missing, use "2026-04-25"
- street_address: string
ONLY return valid JSON array."""
    
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt + "\n" + json.dumps(listings)
        )
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"🚨 AI error: {e}")
        return []

# ── SAVE TO DATABASE ──
def save_to_db(sales):
    if not sales: return
    conn = get_db()
    cur  = conn.cursor()

    print("🧹 Clearing old scraped listings...")
    cur.execute("DELETE FROM sales WHERE user_email = 'scraper@auto.com'")
    conn.commit()

    saved = 0
    for sale in sales:
        try:
            # 🛡️ BULLETPROOF FIX 1: Force a valid future date so app.py doesn't auto-delete it!
            sale_date = sale.get("date", "")
            if not sale_date or not sale_date.startswith("202"):
                sale_date = "2026-04-25"

            cur.execute("""
                INSERT INTO sales (title, date, description, lat, lng, user_email, user_name, user_picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                sale.get("title", "Garage Sale")[:250],
                sale_date,
                sale.get("description", ""),
                float(sale.get("lat", 49.8951)),
                float(sale.get("lng", -97.1384)),
                "scraper@auto.com",
                "Auto Listed",
                ""
            ))
            # 🛡️ BULLETPROOF FIX 2: Commit EACH sale individually!
            conn.commit()
            saved += 1
        except Exception as e:
            print(f"🚨 Skipping a broken listing: {e}")
            conn.rollback() 
            continue

    cur.close()
    conn.close()
    print(f"✅ Saved {saved} permanent sales to database!")

# ── RUN ──
def run():
    cities = ["winnipeg", "brandon"]
    all_structured = []

    for city in cities:
        raw = scrape_kijiji(city) + scrape_craigslist(city)
        if raw:
            structured = extract_with_ai(raw, city.capitalize())
            for sale in structured:
                lat, lng = geocode_location(sale.get("street_address", ""), city)
                sale["lat"], sale["lng"] = lat, lng
            all_structured.extend(structured)

    save_to_db(all_structured)
    print("Done!")

if __name__ == "__main__":
    run()