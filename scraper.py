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

# ── RUTHLESS EXACT GEOCODER ──
def geocode_location(address, city):
    # NO MORE FALLBACKS. If we don't have an address, we return None.
    if not address or address.lower() in ["none", "n/a", "unknown", "", "tbd"]:
        print("🗑️ No address clues found in text. Trashing listing.")
        return None, None

    try:
        search_query = f"{address}, {city}, Manitoba"
        encoded_query = urllib.parse.quote(search_query)
        url = f"https://nominatim.openstreetmap.org/search?q={encoded_query}&format=json&limit=1"
        
        headers = {"User-Agent": "GarageSaleFinderElite/1.0"}
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        time.sleep(1) # Respect rate limits
        
        if data:
            lat = float(data[0]["lat"])
            lng = float(data[0]["lon"])
            print(f"📍 Precision Lock: '{address}' -> {lat}, {lng}")
            return lat, lng
        else:
            print(f"🗑️ API rejected '{address}'. Trashing listing.")
            return None, None
    except Exception as e:
        print(f"🚨 Geocoding error for '{address}': {e}")
        return None, None

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
            loc   = card.find("div", {"class": lambda c: c and "location" in c})
            listing = {
                "title": title.get_text(strip=True) if title else "",
                "desc": desc.get_text(strip=True) if desc else "",
                "loc": loc.get_text(strip=True) if loc else ""
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
                "desc": "",
                "loc": ""
            }
            if listing["title"]: listings.append(listing)
        except: continue
    return listings

# ── USE GEMINI TO EXTRACT & FORMAT ──
def extract_with_ai(listings, city):
    if not listings: return []
    prompt = f"""You are an elite data extraction and location parsing AI for {city}, Manitoba.
Analyze the raw listings (title, description, loc) and extract structured data.

🔥 CRITICAL INSTRUCTION FOR 'street_address': 🔥
Scan the entire listing for location clues. You MUST format the 'street_address' so an OpenStreetMap geocoder can read it flawlessly.
- If it's a normal address, return ONLY the number and street name (e.g., "456 Portage Ave").
- If it mentions an intersection, use an ampersand (e.g., "Osborne St & River Ave").
- If it has a postal code, return just the postal code (e.g., "R3B 2A1").
- STRIP OUT garbage words like "near", "corner of", "beside", "behind", "in back of".
- DO NOT include the city name or "Manitoba".
- If there is absolutely zero location data in the text, return "".

Return a JSON array of objects:
- title: string
- description: string
- date: MUST be YYYY-MM-DD. If missing, use "2026-04-25"
- street_address: optimized string for geocoding
ONLY return a valid JSON array."""
    
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt + "\nRaw Data:\n" + json.dumps(listings)
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
            print(f"🚨 Skipping a broken listing: {e}")
            conn.rollback() 
            continue

    cur.close()
    conn.close()
    print(f"✅ Saved {saved} elite sales to database!")

# ── RUN ──
def run():
    cities = ["winnipeg", "brandon"]
    valid_sales = []

    for city in cities:
        raw = scrape_kijiji(city) + scrape_craigslist(city)
        if raw:
            structured = extract_with_ai(raw, city.capitalize())
            for sale in structured:
                # Get the strict coordinates
                lat, lng = geocode_location(sale.get("street_address", ""), city)
                
                # ONLY keep the sale if the coordinates are successfully found
                if lat is not None and lng is not None:
                    sale["lat"] = lat
                    sale["lng"] = lng
                    valid_sales.append(sale)

    save_to_db(valid_sales)
    print("Done!")

if __name__ == "__main__":
    run()