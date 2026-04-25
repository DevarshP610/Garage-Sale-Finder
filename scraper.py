import requests
import json
import os
import sys
import time
import urllib.parse
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from google import genai


#SCRAPER WONT WORK, CANT FIND THE EXACT LOCATION
#SOMEONE FIX PLZ





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

# ── GLOBAL EXACT GEOCODER ──
def geocode_location(address, city):
    if not address or address.lower() in ["none", "n/a", "unknown", "", "tbd"]:
        return None, None

    try:
        search_query = f"{address}, {city}"
        encoded_query = urllib.parse.quote(search_query)
        url = f"https://nominatim.openstreetmap.org/search?q={encoded_query}&format=json&limit=1"
        
        headers = {"User-Agent": "GarageSaleFinderGlobal/1.0"}
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        time.sleep(1) 
        
        if data:
            lat, lng = float(data[0]["lat"]), float(data[0]["lon"])
            print(f"📍 Mapped: '{address}, {city}' -> {lat}, {lng}")
            return lat, lng
        else:
            print(f"🗑️ Geocoder rejected '{address}'. Trashing.")
            return None, None
    except Exception as e:
        print(f"🚨 Geocoding error: {e}")
        return None, None

# ── SCRAPE KIJIJI (CANADA ONLY) ──
def scrape_kijiji(city):
    url = f"https://www.kijiji.ca/b-garage-sale/{city}/k0l0"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
    except: return []

    listings = []
    cards = soup.find_all("li", {"class": lambda c: c and "regular-ad" in c})
    for card in cards[:15]:
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

# ── SCRAPE CRAIGSLIST (GLOBAL) ──
def scrape_craigslist(city):
    url = f"https://{city}.craigslist.org/search/gss"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200: return [] 
        soup = BeautifulSoup(response.text, "html.parser")
    except: return []

    listings = []
    cards = soup.find_all("li", {"class": "cl-static-search-result"})
    for card in cards[:15]:
        try:
            title = card.find("div", {"class": "title"})
            listing = {
                "title": title.get_text(strip=True) if title else "",
                "desc": ""
            }
            if listing["title"]: listings.append(listing)
        except: continue
    return listings

# ── USE GEMINI TO EXTRACT & FORMAT ──
def extract_with_ai(listings, city):
    if not listings: return []
    prompt = f"""You are an elite data extraction and location parsing AI for {city}.
Analyze the raw listings and extract structured data.

🔥 CRITICAL INSTRUCTION FOR 'street_address': 🔥
Extract ONLY the number and street name, OR intersection, OR postal/zip code. 
STRIP OUT all garbage words. DO NOT include the city name, state, or province.
If absolutely no precise location data is in the text, return "".

Return a JSON array of objects with these exact keys:
- title: string
- description: string
- date: MUST be YYYY-MM-DD. If missing, use "2026-04-25"
- street_address: highly clean string for geocoding"""
    
    try:
        # 🔥 THE FIX: Force JSON mime type so Gemini CANNOT format it incorrectly
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt + "\nRaw Data:\n" + json.dumps(listings),
            config={"response_mime_type": "application/json"}
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"🚨 AI error in {city}: {e}")
        return []

# ── SAVE TO DATABASE ──
def save_to_db(sales):
    if not sales: 
        print("⚠️ No valid mapped sales to save this run.")
        return
    conn = get_db()
    cur  = conn.cursor()

    print("🧹 Clearing old scraped listings globally...")
    cur.execute("DELETE FROM sales WHERE user_email = 'scraper@auto.com'")
    conn.commit()

    saved = 0
    for sale in sales:
        try:
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
                float(sale["lat"]),
                float(sale["lng"]),
                "scraper@auto.com",
                "Auto Listed",
                ""
            ))
            conn.commit()
            saved += 1
        except Exception as e:
            conn.rollback() 
            continue

    cur.close()
    conn.close()
    print(f"✅ FINAL: Saved {saved} elite global sales to database!")

# ── BACKGROUND WORKER ──
def run_scraper_background():
    print("🚀 GLOBAL SCRAPE INITIATED in background...")
    cities = [
        "winnipeg", "brandon", "toronto", "vancouver", "calgary", "ottawa", "halifax",
        "newyork", "losangeles", "chicago", "houston", "miami", "seattle", "austin"
    ]
    
    valid_sales = []

    for city in cities:
        print(f"✈️ Moving to {city.capitalize()}...")
        raw = scrape_kijiji(city) + scrape_craigslist(city)
        if raw:
            structured = extract_with_ai(raw, city.capitalize())
            for sale in structured:
                lat, lng = geocode_location(sale.get("street_address", ""), city)
                if lat is not None and lng is not None:
                    sale["lat"], sale["lng"] = lat, lng
                    valid_sales.append(sale)

    save_to_db(valid_sales)
    print("🏁 GLOBAL SCRAPE COMPLETE!")

# ── TRIGGER FUNCTION ──
def run():
    thread = threading.Thread(target=run_scraper_background)
    thread.start()
    return "Scraper started in the background! Check Railway logs for progress."

if __name__ == "__main__":
    run_scraper_background()
 
