from __future__ import annotations

"""
Haupt-Scraper: Nutzt die Google Places API um Businesses zu finden und
extrahiert dann E-Mails von deren Websites.

Ablauf pro Stadt + Kategorie:
1. Google Places Text Search -> Liste von Businesses
2. Pro Business: Details abrufen (Adresse, Telefon, Website, Rating)
3. Website besuchen und E-Mail extrahieren
4. Lead speichern (mit Checkpoint fuer Wiederaufnahme)

Verwendet die Google Places API (New) mit dem textsearch Endpoint.
Docs: https://developers.google.com/maps/documentation/places/web-service/text-search
"""

import json
import logging

from config.settings import (
    GOOGLE_API_KEY,
    SEARCH_RADIUS_METERS,
    CITIES_FILE,
    CATEGORIES_FILE,
)
from src.utils import retry_request, make_lead_id, CheckpointManager
from src.email_extractor import extract_email


# Google Places API Endpoints
TEXTSEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def load_cities() -> list[dict]:
    """Laedt die Staedteliste aus config/cities.json."""
    with open(CITIES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["cities"]


def load_categories() -> dict:
    """Laedt die Kategorie-Definitionen aus config/categories.json."""
    with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["categories"]


def _search_places(
    query: str,
    place_type: str | None,
    logger: logging.Logger,
) -> list[dict]:
    """
    Sucht Businesses ueber die Google Places Text Search API.

    Die Text Search API ist flexibler als Nearby Search, weil sie natuerlichsprachige
    Suchanfragen versteht (z.B. "Friseur Berlin Mitte"). Sie gibt bis zu 60 Ergebnisse
    zurueck (20 pro Seite, 3 Seiten max).

    Args:
        query: Suchbegriff z.B. "Friseur Berlin"
        place_type: Google Place Type z.B. "hair_care" (optional, schraenkt Ergebnisse ein)
        logger: Logger fuer Statusmeldungen

    Returns:
        Liste von Place-Dictionaries mit place_id, name, etc.
    """
    all_results = []

    params = {
        "query": query,
        "key": GOOGLE_API_KEY,
        "language": "de",
        "region": "de",
    }

    if place_type:
        params["type"] = place_type

    logger.info(f"Suche: '{query}'" + (f" (type={place_type})" if place_type else ""))

    # Erste Seite abrufen
    response = retry_request(TEXTSEARCH_URL, params=params, logger=logger)
    if not response:
        return []

    data = response.json()
    status = data.get("status", "UNKNOWN")

    if status != "OK":
        if status == "ZERO_RESULTS":
            logger.info(f"Keine Ergebnisse fuer '{query}'")
        else:
            logger.warning(f"Places API Fehler: {status} - {data.get('error_message', '')}")
        return []

    all_results.extend(data.get("results", []))
    logger.info(f"Seite 1: {len(data.get('results', []))} Ergebnisse")

    # Weitere Seiten abrufen (Google gibt max 3 Seiten mit je 20 Ergebnissen)
    page = 2
    while "next_page_token" in data and page <= 3:
        # Google braucht eine kurze Pause bevor der next_page_token gueltig wird
        import time
        time.sleep(2)

        next_params = {
            "pagetoken": data["next_page_token"],
            "key": GOOGLE_API_KEY,
        }
        response = retry_request(TEXTSEARCH_URL, params=next_params, logger=logger)
        if not response:
            break

        data = response.json()
        if data.get("status") != "OK":
            break

        all_results.extend(data.get("results", []))
        logger.info(f"Seite {page}: {len(data.get('results', []))} Ergebnisse")
        page += 1

    logger.info(f"Insgesamt {len(all_results)} Ergebnisse fuer '{query}'")
    return all_results


def _get_place_details(place_id: str, logger: logging.Logger) -> dict | None:
    """
    Ruft detaillierte Informationen zu einem Business ab.

    Die Details API liefert Daten die in der Text Search nicht enthalten sind,
    insbesondere: formatierte Adresse, Telefonnummer, Website, Oeffnungszeiten.

    Args:
        place_id: Google Place ID (eindeutige Kennung)
        logger: Logger

    Returns:
        Dictionary mit Place-Details oder None bei Fehler
    """
    params = {
        "place_id": place_id,
        "key": GOOGLE_API_KEY,
        "language": "de",
        "fields": (
            "name,formatted_address,formatted_phone_number,"
            "website,rating,user_ratings_total,"
            "address_components,business_status"
        ),
    }

    response = retry_request(DETAILS_URL, params=params, logger=logger)
    if not response:
        return None

    data = response.json()
    if data.get("status") != "OK":
        logger.debug(f"Details-Fehler fuer {place_id}: {data.get('status')}")
        return None

    return data.get("result")


def _parse_address_components(components: list[dict]) -> dict:
    """
    Zerlegt die Google Address Components in einzelne Felder.

    Google liefert die Adresse als Liste von Komponenten, z.B.:
    [{"long_name": "Berlin", "types": ["locality"]}, ...]

    Diese Funktion extrahiert daraus: Strasse, Hausnummer, PLZ, Stadt, Bundesland.
    """
    result = {
        "street": "",
        "street_number": "",
        "postal_code": "",
        "city": "",
        "state": "",
    }

    if not components:
        return result

    for comp in components:
        types = comp.get("types", [])
        name = comp.get("long_name", "")

        if "route" in types:
            result["street"] = name
        elif "street_number" in types:
            result["street_number"] = name
        elif "postal_code" in types:
            result["postal_code"] = name
        elif "locality" in types:
            result["city"] = name
        elif "administrative_area_level_1" in types:
            result["state"] = name

    return result


def scrape_leads(
    cities: list[dict] | None = None,
    categories: dict | None = None,
    specific_city: str | None = None,
    specific_category: str | None = None,
    logger: logging.Logger | None = None,
) -> list[dict]:
    """
    Hauptfunktion: Scrapt Leads fuer die angegebenen Staedte und Kategorien.

    Args:
        cities: Liste der Staedte (aus cities.json). Wenn None, wird die Datei geladen.
        categories: Kategorie-Definitionen. Wenn None, wird die Datei geladen.
        specific_city: Optional - nur diese eine Stadt scrapen
        specific_category: Optional - nur diese eine Kategorie scrapen
        logger: Logger

    Returns:
        Liste von Lead-Dictionaries
    """
    if not logger:
        from src.utils import setup_logging
        logger = setup_logging()

    if not GOOGLE_API_KEY:
        logger.error(
            "Kein Google API Key gefunden! "
            "Kopiere .env.example nach .env und trage deinen Key ein."
        )
        return []

    # Daten laden
    if cities is None:
        cities = load_cities()
    if categories is None:
        categories = load_categories()

    # Filtern falls spezifische Stadt/Kategorie angegeben
    if specific_city:
        cities = [c for c in cities if c["name"].lower() == specific_city.lower()]
        if not cities:
            logger.error(f"Stadt '{specific_city}' nicht in cities.json gefunden!")
            return []

    if specific_category:
        if specific_category not in categories:
            logger.error(
                f"Kategorie '{specific_category}' nicht gefunden! "
                f"Verfuegbar: {', '.join(categories.keys())}"
            )
            return []
        categories = {specific_category: categories[specific_category]}

    # Checkpoint laden (fuer Wiederaufnahme nach Abbruch)
    checkpoint = CheckpointManager()
    total_cities = len(cities)
    total_categories = len(categories)
    total_combos = total_cities * total_categories

    logger.info(
        f"Start: {total_cities} Staedte x {total_categories} Kategorien = {total_combos} Kombinationen"
    )

    combo_count = 0

    for city_info in cities:
        city_name = city_info["name"]
        bundesland = city_info["bundesland"]

        for cat_key, cat_data in categories.items():
            combo_count += 1
            label = cat_data["label"]

            # Schon abgearbeitet? -> Ueberspringen
            if checkpoint.is_processed(city_name, cat_key):
                logger.info(
                    f"[{combo_count}/{total_combos}] {city_name} / {label} -> bereits erledigt, ueberspringe"
                )
                continue

            logger.info(
                f"[{combo_count}/{total_combos}] Scrape: {city_name} / {label}"
            )

            # Fuer jeden Suchbegriff der Kategorie suchen
            seen_place_ids = set()

            for search_term in cat_data["search_terms"]:
                query = f"{search_term} in {city_name}"
                places = _search_places(query, cat_data.get("place_type"), logger)

                for place in places:
                    place_id = place.get("place_id")
                    if not place_id or place_id in seen_place_ids:
                        continue
                    seen_place_ids.add(place_id)

                    name = place.get("name", "")
                    lead_id = make_lead_id(name, city_name)

                    # Duplikat-Check
                    if checkpoint.is_duplicate(lead_id):
                        logger.debug(f"Duplikat uebersprungen: {name} ({city_name})")
                        continue

                    # Details abrufen
                    details = _get_place_details(place_id, logger)
                    if not details:
                        continue

                    # Nur aktive Businesses
                    biz_status = details.get("business_status", "OPERATIONAL")
                    if biz_status != "OPERATIONAL":
                        logger.debug(f"Uebersprungen (Status: {biz_status}): {name}")
                        continue

                    # Adresse zerlegen
                    addr = _parse_address_components(
                        details.get("address_components", [])
                    )

                    # Website und E-Mail
                    website = details.get("website", "")
                    email = None
                    if website:
                        logger.debug(f"Extrahiere E-Mail von: {website}")
                        email = extract_email(website, logger)

                    # Strasse + Hausnummer zusammenfuegen
                    street_full = addr["street"]
                    if addr["street_number"]:
                        street_full = f"{addr['street']} {addr['street_number']}"

                    # Lead-Objekt erstellen
                    lead = {
                        "business_name": name,
                        "category_key": cat_key,
                        "category_label": label,
                        "street_address": street_full,
                        "postal_code": addr["postal_code"],
                        "city": addr["city"] or city_name,
                        "state": addr["state"] or bundesland,
                        "phone": details.get("formatted_phone_number", ""),
                        "website": website,
                        "email": email or "",
                        "google_rating": place.get("rating", ""),
                        "google_reviews": place.get("user_ratings_total", ""),
                    }

                    checkpoint.add_lead(lead, lead_id)
                    status_email = f"E-Mail: {email}" if email else "keine E-Mail"
                    logger.info(f"  + {name} ({status_email})")

            # Kombination als erledigt markieren
            checkpoint.mark_processed(city_name, cat_key)

    all_leads = checkpoint.get_leads()
    leads_with_email = [l for l in all_leads if l["email"]]
    leads_no_email = [l for l in all_leads if not l["email"]]

    logger.info(
        f"\nFertig! {len(all_leads)} Leads gesamt: "
        f"{len(leads_with_email)} mit E-Mail, {len(leads_no_email)} ohne E-Mail"
    )

    return all_leads
