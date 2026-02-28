"""
Zentrale Konfiguration fuer den APPOYNT Lead Scraper.

Liest den Google API Key aus der .env-Datei und definiert
alle wichtigen Parameter wie Rate Limits, Timeouts usw.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Projektverzeichnis bestimmen (ein Ordner ueber /config)
BASE_DIR = Path(__file__).resolve().parent.parent

# .env-Datei laden (dort liegt dein API Key)
load_dotenv(BASE_DIR / ".env")

# --- Google Places API ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

# Radius in Metern fuer die Places-Suche rund um das Stadtzentrum.
# 30km deckt auch groessere Staedte gut ab.
SEARCH_RADIUS_METERS = 30000

# --- Rate Limiting ---
# Sekunden zwischen zwei Requests (1.0 = max 1 Request/Sek.)
REQUEST_DELAY_SECONDS = 1.0

# --- Retry-Logik ---
MAX_RETRIES = 3
# Wartezeit in Sekunden, die sich pro Retry verdoppelt (Exponential Backoff)
RETRY_BACKOFF_SECONDS = 2.0

# --- Timeouts ---
# Max. Wartezeit in Sekunden fuer einen einzelnen HTTP-Request
HTTP_TIMEOUT_SECONDS = 15

# --- Pfade ---
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
CHECKPOINT_DIR = BASE_DIR / "output" / "checkpoints"

CITIES_FILE = BASE_DIR / "config" / "cities.json"
CATEGORIES_FILE = BASE_DIR / "config" / "categories.json"

# --- Output-Dateien ---
LEADS_WITH_EMAIL_CSV = OUTPUT_DIR / "leads_with_email.csv"
LEADS_NO_EMAIL_CSV = OUTPUT_DIR / "leads_no_email.csv"
