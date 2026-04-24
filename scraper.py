import requests
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from google import genai

client = genai.Client(api_key=os.environ.get('GEMINI_API_KEY'))

def get_db():
    return psycopg2.connect(os.environ.get('DATABASE_URL'), cursor_factory=RealDictCursor)

def scrape_kijiji(city='winnipeg'):
    url = f'https://www.kijiji.ca/b-garage-sale/{city}/k0l0'
    headers = {'User-Agent': 'Mozilla/5.0'}
    print(f'Scraping Kijiji for {city}...')
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    listings = []
    cards = soup.find_all('li', {'class': lambda c: c and 'regular-ad' in c})
    for card in cards[:10]:
        try:
            title = card.find('div', {'class': lambda c: c and 'title' in c})
            desc  = card.find('div', {'class': lambda c: c and 'description' in c})
            loc   = card.find('div', {'class': lambda c: c and 'location' in c})
            date  = card.find('span', {'class': lambda c: c and 'date-posted' in c})
            listing = {'title': title.get_text(strip=True) if title else '', 'desc': desc.get_text(strip=True) if desc else '', 'location': loc.get_text(strip=True) if loc else '', 'date': date.get_text(strip=True) if date else ''}
            if listing['title']:
                listings.append(listing)
        except:
            continue
    print(f'Found {len(listings)} Kijiji listings')
    return listings

def scrape_craigslist(city='winnipeg'):
    url = 'https://winnipeg.craigslist.org/search/gss'
    headers = {'User-Agent': 'Mozilla/5.0'}
    print('Scraping Craigslist...')
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    listings = []
    cards = soup.find_all('li', {'class': 'cl-static-search-result'})
    for card in cards[:10]:
        try:
            title = card.find('div', {'class': 'title'})
            loc   = card.find('div', {'class': 'location'})
            listing = {'title': title.get_text(strip=True) if title else '', 'desc': '', 'location': loc.get_text(strip=True) if loc else city, 'date': ''}
            if listing['title']:
                listings.append(listing)
        except:
            continue
    print(f'Found {len(listings)} Craigslist listings')
    return listings

def extract_with_ai(listings, city='Winnipeg'):
    if not listings:
        return []
    listings_text = json.dumps(listings, indent=2)
    prompt = f'You are a data extractor. Given these raw garage sale listings from {city}, extract structured data. Return a JSON array only, no markdown, no code blocks. Each object must have: title, description, date (YYYY-MM-DD format, use 2026-04-26 if unknown), lat (estimate for {city}), lng (estimate for {city}), user_name (Kijiji Listing or Craigslist Listing), user_email (scraper@auto.com), user_picture (empty string). Raw listings: {listings_text}'
    print('Sending to Gemini...')
    response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
    try:
        text = response.text.strip().replace('\\json', '').replace('\\', '').strip()
        return json.loads(text)
    except Exception as e:
        print(f'AI error: {e}')
        print(f'Raw: {response.text}')
        return []

def save_to_db(sales):
    if not sales:
        print('No sales to save')
        return
    conn = get_db()
    cur  = conn.cursor()
    saved = 0
    for sale in sales:
        try:
            cur.execute('INSERT INTO sales (title, date, description, lat, lng, user_email, user_name, user_picture) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (sale.get('title', 'Garage Sale'), sale.get('date', ''), sale.get('description', ''), float(sale.get('lat', 49.8951)), float(sale.get('lng', -97.1384)), sale.get('user_email', 'scraper@auto.com'), sale.get('user_name', 'Auto Listed'), sale.get('user_picture', '')))
            saved += 1
        except Exception as e:
            print(f'Error: {e}')
    conn.commit()
    cur.close()
    conn.close()
    print(f'Saved {saved} sales!')

def run(city='winnipeg'):
    print('Starting scraper...')
    all_listings = scrape_kijiji(city) + scrape_craigslist(city)
    print(f'Total: {len(all_listings)}')
    if not all_listings:
        print('No listings found!')
        return
    structured = extract_with_ai(all_listings, city.capitalize())
    print(f'Extracted {len(structured)} sales')
    save_to_db(structured)
    print('Done!')

if __name__ == '__main__':
    run('winnipeg')
