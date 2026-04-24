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

if not GEMINI_API_KEY:
    print("🚨 ERROR: GEMINI_API_KEY missing!")
    sys.exit(1)

if not DATABASE_URL:
    print("🚨 ERROR: DATABASE_URL missing!")
    sys.exit(1)

# ── GEMINI CLIENT ──
try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"🚨 ERROR initializing Gemini: {e}")
    sys.exit(1)

# ── DATABASE ──
def get_db():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )

# ── GEOCODER (CONVERTS ADDRESS TO GPS) ──
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
        # FIX: Properly URL-encode the address so the API doesn't break on spaces
        search_query = f"{address}, {city}, Manitoba"
        encoded_query = urllib.parse.quote(search_query)
        url = f"https://nominatim.openstreetmap.org/search?q={encoded_query}&format=json&limit=1"
        
        headers = {"User-Agent": "GarageSaleFinder/1.0"}
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        
        time.sleep(1) # Respect API rate limits
        
        if data:
            print(f"📍 Geocoded '{address}' -> {data[0]['lat']}, {data[0]['lon']}")
            return float(data[0]["lat"]), float(data[0]["lon"])
        else:
            print(f"⚠️ Geocoder found nothing for '{search_query}'. Using default.")
    except Exception as e:
        print(f"🚨 Geocoding failed for {address}: {e}")

    return default_lat, default_lng

# ── SCRAPE KIJIJI ──
def scrape_kijiji(city):
    url = f"https://www.kijiji.ca/b-garage-sale/{city}/k0l0"
    headers = {"User-Agent": "Mozilla/5.0"}
    print(f"Scraping Kijiji for {city}...")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"Error fetching Kijiji: {e}")
        return []

    listings = []
    cards = soup.find_all("li", {"class": lambda c: c and "regular-ad" in c})

    for card in cards[:10]:
        try:
            title = card.find("div", {"class": lambda c: c and "title" in c})
            desc  = card.find("div", {"class": lambda c: c and "description" in c})
            loc   = card.find("div", {"class": lambda c: c and "location" in c})
            date  = card.find("span", {"class": lambda c: c and "date-posted" in c})

            listing = {
                "title":    title.get_text(strip=True) if title else "",
                "desc":     desc.get_text(strip=True)  if desc  else "",
                "location": loc.get_text(strip=True)   if loc   else "",
                "date":     date.get_text(strip=True)  if date  else ""
            }
            if listing["title"]:
                listings.append(listing)
        except:
            continue
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
    except Exception as e:
        return []

    listings = []
    cards = soup.find_all("li", {"class": "cl-static-search-result"})

    for card in cards[:10]:
        try:
            title = card.find("div", {"class": "title"})
            loc   = card.find("div", {"class": "location"})
            listing = {
                "title":    title.get_text(strip=True) if title else "",
                "desc":     "",
                "location": loc.get_text(strip=True)   if loc   else city,
                "date":     ""
            }
            if listing["title"]:
                listings.append(listing)
        except:
            continue
    return listings

# ── USE GEMINI TO EXTRACT STRUCTURED DATA ──
def extract_with_ai(listings, city):
    if not listings: return []

    listings_text = json.dumps(listings, indent=2)

    prompt = f"""You are a data extractor. Given these raw garage sale listings from {city}, Manitoba:
1. Extract structured data for each.
2. For 'street_address', hunt for exact street names, postal codes, or specific intersections. If none, provide the neighborhood name. Leave blank if absolutely no location is mentioned.

Return a JSON array of objects:
- title: clean title
- description: what is being sold
- date: YYYY-MM-DD
- street_address: best guess at specific location
- user_name: "Kijiji Listing" or "Craigslist Listing"
- user_email: "scraper@auto.com"
- user_picture: ""

ONLY return a valid JSON array.
Raw:
{listings_text}"""

    print(f"Sending {city} to Gemini...")
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"🚨 AI error: {e}")
        return []

# ── SAVE TO DATABASE ──
def save_to_db(sales):
    if not sales: return

    try:
        conn = get_db()
        cur  = conn.cursor()
    except Exception as e:
        print(f"🚨 DB connection error: {e}")
        return

    # FIX: Delete all old auto-scraped listings before inserting to prevent duplicates!
    print("🧹 Clearing old scraped listings from database...")
    try:
        cur.execute("DELETE FROM sales WHERE user_email = 'scraper@auto.com'")
        conn.commit()
    except Exception as e:
        print(f"🚨 Error clearing old data: {e}")
        conn.rollback()

    saved = 0
    for sale in sales:
        try:
            cur.execute("""
                INSERT INTO sales (title, date, description, lat, lng, user_email, user_name, user_picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                sale.get("title", "Garage Sale"),
                sale.get("date", ""),
                sale.get("description", ""),
                float(sale.get("lat", 49.8951)),
                float(sale.get("lng", -97.1384)),
                sale.get("user_email", "scraper@auto.com"),
                sale.get("user_name", "Auto Listed"),
                sale.get("user_picture", "")
            ))
            saved += 1
        except Exception as e:
            conn.rollback() 
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Saved {saved} new, unique sales to database!")

# ── RUN THE SCRAPER ──
def run():
    cities = ["winnipeg", "brandon"]
    all_structured = []

    for city in cities:
        city_listings = scrape_kijiji(city) + scrape_craigslist(city)
        if city_listings:
            structured = extract_with_ai(city_listings, city.capitalize())
            for sale in structured:
                address = sale.get("street_address", "")
                lat, lng = geocode_location(address, city)
                sale["lat"], sale["lng"] = lat, lng
            all_structured.extend(structured)

    save_to_db(all_structured)
    print("Done!")

if __name__ == "__main__":
    run()