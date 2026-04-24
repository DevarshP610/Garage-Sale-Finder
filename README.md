# 🏷️ Garage Sale Finder

A full-stack web app where anyone can pin garage sales on an interactive map so buyers in the area can find them easily.

Live App: https://garage-sale-finder-production.up.railway.app

---

## What It Does

- 📍 Click anywhere on the map to drop a pin at your garage sale location
- 📝 Add a title, date, and description of what's for sale
- 🗺️ See all nearby garage sales on an interactive map
- 👤 Sign in with Google to add and manage your own listings
- 🔍 Search sales by keyword
- 📱 Works on mobile and desktop

---

## Built With

- Python + Flask — Backend web server
- PostgreSQL — Database for storing sales
- Leaflet.js — Interactive map
- OpenStreetMap — Map tiles
- Google OAuth — User authentication
- Railway — Hosting and deployment

---

## Run It Locally

1. Clone the repo
   git clone https://github.com/DevarshP610/Garage-Sale-Finder.git
   cd Garage-Sale-Finder

2. Create a virtual environment
   python -m venv venv
   venv\Scripts\activate

3. Install dependencies
   pip install -r requirements.txt

4. Set environment variables
   Create a .env file with:
   DATABASE_URL=your_postgresql_url
   GOOGLE_CLIENT_ID=your_google_client_id
   GOOGLE_CLIENT_SECRET=your_google_client_secret
   SECRET_KEY=your_secret_key

5. Run the app
   python app.py
   Open http://127.0.0.1:5000

---

## Project Structure

garage-sale-finder/
├── app.py              - Flask backend and API routes
├── requirements.txt    - Python dependencies
├── templates/
│   └── index.html      - Frontend UI
└── README.md

---

## Roadmap

- [x] Pin garage sales on a map
- [x] Persistent PostgreSQL database
- [x] Google login
- [x] Mobile responsive
- [x] Search and filter sales
- [ ] Auto-expire old listings
- [ ] Email notifications
- [ ] Sale categories and tags

---

Built by @DevarshP610
