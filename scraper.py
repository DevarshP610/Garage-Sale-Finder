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
def geocode_location(address, city, ai_lat=None, ai_lng=None):
    if address and address.lower() not in ["none", "n/a", "unknown", "", "tbd"]:
        try:
            # Clean up the address for Nominatim
            clean_address = address.split(" at ")[-1].strip()
            search_query = f"{clean_address}, {city}"
            encoded_query = urllib.parse.quote(search_query)
            url = f"https://nominatim.openstreetmap.org/search?q={encoded_query}&format=json&limit=1"
            
            headers = {"User-Agent": "GarageSaleFinderGlobal/2.0"}
            response = requests.get(url, headers=headers, timeout=10)
            data = response.json()
            time.sleep(1.2) # Be nice to OpenStreetMap servers
            
            if data:
                lat, lng = float(data[0]["lat"]), float(data[0]["lon"])
                print(f"📍 Mapped (Nominatim): '{address}, {city}' -> {lat}, {lng}")
                return lat, lng
            else:
                print(f"🗑️ Nominatim rejected '{address}'. Trying AI fallback...")
        except Exception as e:
            print(f"🚨 Geocoding error: {e}")
            
    # Fallback to AI estimated coordinates if Nominatim fails or no clean address
    if ai_lat is not None and ai_lng is not None:
        try:
            lat, lng = float(ai_lat), float(ai_lng)
            if lat != 0.0 and lng != 0.0:
                print(f"📍 Mapped (AI Estimate): '{address}, {city}' -> {lat}, {lng}")
                return lat, lng
        except:
            pass
            
    return None, None

# ── SCRAPE KIJIJI (CANADA ONLY) ──
def scrape_kijiji(city):
    url = f"https://www.kijiji.ca/b-garage-sale/{city}/k0l0"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"Kijiji scrape failed for {city}: {e}")
        return []

    listings = []
    cards = soup.find_all("div", class_=lambda c: bool(c and "search-item" in c))
    if not cards:
         cards = soup.find_all("li", class_=lambda c: bool(c and "regular-ad" in c))
         
    for card in cards[:10]:
        try:
            title_elem = card.find("a", class_=lambda c: bool(c and "title" in c))
            if not title_elem:
                title_elem = card.find("a")
            
            title = title_elem.get_text(strip=True) if title_elem else ""
            desc_elem = card.find("div", class_=lambda c: bool(c and "description" in c))
            desc = desc_elem.get_text(strip=True) if desc_elem else ""
            
            link_val = title_elem.get("href") if title_elem else None
            link = ""
            if link_val:
                link = str(link_val[0]) if isinstance(link_val, list) else str(link_val)
                
            if link:
                if not link.startswith("http"):
                    link = "https://www.kijiji.ca" + link
                try:
                    time.sleep(1) # Be nice to Kijiji to avoid bans
                    ad_resp = requests.get(link, headers=headers, timeout=10)
                    ad_soup = BeautifulSoup(ad_resp.text, "html.parser")
                    full_desc_elem = ad_soup.find("div", {"itemprop": "description"}) or ad_soup.find("div", class_=lambda c: bool(c and "description" in c))
                    if full_desc_elem:
                        desc = full_desc_elem.get_text(" ", strip=True)
                except:
                    pass
            
            title_lower = title.lower() if title else ""
            desc_lower = desc.lower() if desc else ""
            if any(kw in title_lower for kw in ["garage", "yard", "moving", "estate", "sale"]) or \
               any(kw in desc_lower for kw in ["garage", "yard", "moving", "estate", "sale"]):
                listings.append({"title": title, "desc": desc})
        except: continue
    return listings

# ── SCRAPE CRAIGSLIST (GLOBAL) USING RSS FOR FULL TEXT ──
def scrape_craigslist(city):
    url = f"https://{city}.craigslist.org/search/gms?format=rss"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200: return [] 
        soup = BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        print(f"Craigslist scrape failed for {city}: {e}")
        return []

    listings = []
    items = soup.find_all("item")
    for item in items[:15]:
        try:
            title = item.title.get_text(strip=True) if item.title else ""
            desc = item.description.get_text(" ", strip=True) if item.description else ""
            
            title_lower = title.lower() if title else ""
            desc_lower = desc.lower() if desc else ""
            if any(kw in title_lower for kw in ["garage", "yard", "moving", "estate", "sale"]) or \
               any(kw in desc_lower for kw in ["garage", "yard", "moving", "estate", "sale"]):
                listings.append({
                    "title": title,
                    "desc": desc
                })
        except: continue
    return listings

# ── USE GEMINI TO EXTRACT & FORMAT ──
def extract_with_ai(listings, city):
    if not listings: return []
    prompt = f"""You are an elite data extraction and location parsing AI for {city}.
Analyze the raw listings and extract structured data for Garage Sales, Yard Sales, Estate Sales, and Moving Sales.

🔥 CRITICAL INSTRUCTIONS 🔥
1. EXTRACT THE WHOLE DESCRIPTION. Do not summarize it. Provide the full details.
2. Extract the EXACT date of the sale in YYYY-MM-DD format based on the text. If no specific date is found, use "2026-04-25".
3. For 'street_address': Extract ONLY the house number and street name, OR intersection. STRIP OUT all garbage words (like "sale at", "corner of"). DO NOT include the city name, state, or province. Make it highly suitable for exact OpenStreetMap geocoding. If absolutely no location is found, return "".
4. Estimate the exact precise 'lat' and 'lng' for this location in {city}. Provide them as numbers. If you cannot estimate, use null.

Return a JSON array of objects with these exact keys:
- title: string
- description: string
- date: string (YYYY-MM-DD)
- street_address: string
- lat: number or null
- lng: number or null
"""
    
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt + "\nRaw Data:\n" + json.dumps(listings),
            config={"response_mime_type": "application/json"}
        )
        
        raw_text = (response.text or "[]").strip()
        # Strip Markdown formatting if the AI sneaks it in
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:]
        elif raw_text.startswith("```"):
            raw_text = raw_text[3:]
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]
            
        return json.loads(raw_text.strip())
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
                lat, lng = geocode_location(
                    sale.get("street_address", ""), 
                    city, 
                    sale.get("lat"), 
                    sale.get("lng")
                )
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
 
