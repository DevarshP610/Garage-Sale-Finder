import requests
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from google import genai

# ── GEMINI CLIENT ──
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

# ── DATABASE ──
def get_db():
    return psycopg2.connect(
        os.environ.get("DATABASE_URL"),
        cursor_factory=RealDictCursor
    )

# ── SCRAPE KIJIJI ──
def scrape_kijiji(city="winnipeg"):
    url = f"https://www.kijiji.ca/b-garage-sale/{city}/k0l0"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    print(f"Scraping Kijiji for {city}...")
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

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

    print(f"Found {len(listings)} Kijiji listings")
    return listings

# ── SCRAPE CRAIGSLIST ──
def scrape_craigslist(city="winnipeg"):
    url = "https://winnipeg.craigslist.org/search/gss"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    print("Scraping Craigslist...")
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    listings = []
    cards = soup.find_all("li", {"class": "cl-static-search-result"})

    for card in cards[:10]:
        try:
            title = card.find("div", {"class": "title"})
            loc   = card.find("div", {"class": "location"})

            listing = {
                "title":    title.get_text(strip=True) if title else "",
                "desc":     "",
                "location": loc.get_text(strip=True)   if loc   else "winnipeg",
                "date":     ""
            }

            if listing["title"]:
                listings.append(listing)
        except:
            continue

    print(f"Found {len(listings)} Craigslist listings")
    return listings

# ── USE GEMINI TO EXTRACT STRUCTURED DATA ──
def extract_with_ai(listings, city="Winnipeg"):
    if not listings:
        return []

    listings_text = json.dumps(listings, indent=2)

    prompt = f"""You are a data extractor. Given these raw garage sale listings from {city}, 
extract structured data for each one.

For each listing return a JSON array with objects containing:
- title: clean title of the sale
- description: what is being sold
- date: in YYYY-MM-DD format (if no date found use next Saturday which is 2026-04-26)
- lat: latitude for the location in {city} (estimate based on neighborhood or address)
- lng: longitude for the location in {city} (estimate based on neighborhood or address)
- user_name: "Kijiji Listing" or "Craigslist Listing"
- user_email: "scraper@auto.com"
- user_picture: ""

ONLY return a valid JSON array, no other text, no markdown, no code blocks.

Raw listings:
{listings_text}"""

    print("Sending to Gemini...")
    response = client.models.generate_content(
        model="gemini-1.5-flash-8b",
        contents=prompt
    )

    try:
        text = response.text.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"AI extraction error: {e}")
        print(f"Raw response: {response.text}")
        return []

# ── SAVE TO DATABASE ──
def save_to_db(sales):
    if not sales:
        print("No sales to save")
        return

    conn = get_db()
    cur  = conn.cursor()
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
            print(f"Error saving sale: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {saved} sales to database!")

# ── RUN THE SCRAPER ──
def run(city="winnipeg"):
    print("Starting scraper...")

    kijiji_listings     = scrape_kijiji(city)
    craigslist_listings = scrape_craigslist(city)

    all_listings = kijiji_listings + craigslist_listings
    print(f"Total raw listings: {len(all_listings)}")

    if not all_listings:
        print("No listings found!")
        return

    print("Running Gemini AI extraction...")
    structured = extract_with_ai(all_listings, city.capitalize())
    print(f"Gemini extracted {len(structured)} sales")

    save_to_db(structured)
    print("Done!")

if __name__ == "__main__":
    run("winnipeg")