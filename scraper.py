import requests
import json
import os
import sys
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from google import genai

# ── ENVIRONMENT CHECK ──
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not GEMINI_API_KEY:
    print("🚨 ERROR: GEMINI_API_KEY environment variable is missing!")
    sys.exit(1)

if not DATABASE_URL:
    print("🚨 ERROR: DATABASE_URL environment variable is missing!")
    sys.exit(1)

# ── GEMINI CLIENT ──
try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"🚨 ERROR initializing Gemini Client: {e}")
    sys.exit(1)

# ── DATABASE ──
def get_db():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )

# ── GEOCODER (CONVERTS ADDRESS TO GPS) ──
def geocode_location(address, city):
    # Fallback center points if the seller didn't provide a real address
    fallbacks = {
        "winnipeg": (49.8951, -97.1384),
        "brandon": (49.8485, -99.9501)
    }
    
    city_lower = city.lower()
    default_lat, default_lng = fallbacks.get(city_lower, (49.8951, -97.1384))

    if not address or address.lower() in ["none", "n/a", "unknown", ""]:
        return default_lat, default_lng

    try:
        # Using OpenStreetMap's free geocoding API
        url = f"https://nominatim.openstreetmap.org/search?q={address}, {city}, Manitoba&format=json&limit=1"
        headers = {"User-Agent": "GarageSaleFinder/1.0"}
        
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        
        # Respect the free API rate limit (1 request per second)
        time.sleep(1) 
        
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"Geocoding failed for {address}: {e}")
        pass

    return default_lat, default_lng

# ── SCRAPE KIJIJI ──
def scrape_kijiji(city):
    url = f"https://www.kijiji.ca/b-garage-sale/{city}/k0l0"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

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

    print(f"Found {len(listings)} Kijiji listings in {city}")
    return listings

# ── SCRAPE CRAIGSLIST ──
def scrape_craigslist(city):
    # Craigslist usually focuses on major hubs, but we try the URL anyway
    url = f"https://{city}.craigslist.org/search/gss"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    print(f"Scraping Craigslist for {city}...")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return [] # Skip if the city doesn't have a dedicated CL board
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"Error fetching Craigslist: {e}")
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

    print(f"Found {len(listings)} Craigslist listings in {city}")
    return listings

# ── USE GEMINI TO EXTRACT STRUCTURED DATA ──
def extract_with_ai(listings, city):
    if not listings:
        return []

    listings_text = json.dumps(listings, indent=2)

    prompt = f"""You are a data extractor. Given these raw garage sale listings from {city}, Manitoba, extract structured data for each one.

For each listing return a JSON array with objects containing:
- title: clean title of the sale
- description: what is being sold
- date: in YYYY-MM-DD format (if no date found use next Saturday which is 2026-04-25)
- street_address: the specific street address, intersection, or neighborhood mentioned in the description or location. If none exists, leave empty string "".
- user_name: "Kijiji Listing" or "Craigslist Listing"
- user_email: "scraper@auto.com"
- user_picture: ""

ONLY return a valid JSON array, no other text, no markdown, no code blocks.

Raw listings:
{listings_text}"""

    print(f"Sending {city} listings to Gemini...")
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        
        text = response.text.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"🚨 AI extraction error: {e}")
        return []

# ── SAVE TO DATABASE ──
def save_to_db(sales):
    if not sales:
        print("No sales to save")
        return

    try:
        conn = get_db()
        cur  = conn.cursor()
    except Exception as e:
        print(f"🚨 Database connection error: {e}")
        return

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
            print(f"Error saving sale '{sale.get('title', 'Unknown')}': {e}")
            conn.rollback() 
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Saved {saved} total sales to database!")

# ── RUN THE SCRAPER ──
def run():
    print("Starting scraper for Manitoba...")
    
    # Loop through major Manitoba cities
    cities = ["winnipeg", "brandon"]
    all_structured_sales = []

    for city in cities:
        kijiji_listings = scrape_kijiji(city)
        craigslist_listings = scrape_craigslist(city)

        city_listings = kijiji_listings + craigslist_listings
        print(f"Total raw listings for {city}: {len(city_listings)}")

        if city_listings:
            structured = extract_with_ai(city_listings, city.capitalize())
            
            # Convert addresses to exact lat/lng GPS coordinates
            print(f"Geocoding exact coordinates for {city}...")
            for sale in structured:
                address = sale.get("street_address", "")
                lat, lng = geocode_location(address, city)
                sale["lat"] = lat
                sale["lng"] = lng
                
            all_structured_sales.extend(structured)

    save_to_db(all_structured_sales)
    print("Done!")

if __name__ == "__main__":
    run()