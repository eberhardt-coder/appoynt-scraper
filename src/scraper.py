from __future__ import annotations

"""
Haupt-Scraper: Nutzt die Google Places API (New) um Businesses zu finden und
extrahiert dann E-Mails von deren Websites.

Ablauf pro Stadt + Kategorie:
1. Google Places Text Search (New) -> Liste von Businesses mit Details
2. Website besuchen und E-Mail extrahieren
3. Lead speichern (mit Checkpoint fuer Wiederaufnahme)

Verwendet die Google Places API (New) – die aktuelle Version.
Die neue API liefert alle Details direkt in der Text Search Antwort,
dadurch brauchen wir keinen separaten Details-Request mehr (spart Kosten!).

Docs: https://developers.google.com/maps/documentation/places/web-service/text-search
"""

import json
import logging
import time

from config.settings import (
    GOOGLE_API_KEY,
    SEARCH_RADIUS_METERS,
    CITIES_FILE,
    CATEGORIES_FILE,
)
from src.utils import retry_request, make_lead_id, CheckpointManager
from src.email_extractor import extract_email


# Google Places API (New) Endpoint
TEXTSEARCH_URL = "https://places.googleapis.com/v1/places:searchText"


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
    Sucht Businesses ueber die Google Places Text Search API (New).

    Die neue API verwendet POST-Requests und liefert alle Details
    (Name, Adresse, Telefon, Website, Rating) direkt in einer Antwort.
    Das spart einen separaten Details-Request pro Business.

    Paginierung: Die neue API nutzt pageToken im Response-Body.
    Pro Seite kommen bis zu 20 Ergebnisse, maximal 3 Seiten (= 60 Ergebnisse).

    Args:
        query: Suchbegriff z.B. "Friseur Berlin"
        place_type: Google Place Type z.B. "hair_care" (optional)
        logger: Logger fuer Statusmeldungen

    Returns:
        Liste von Place-Dictionaries
    """
    all_results = []

    # Header: API Key + welche Felder wir zurueck haben wollen (spart Kosten)
    headers = {
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "places.id,"
            "places.displayName,"
            "places.formattedAddress,"
            "places.addressComponents,"
            "places.nationalPhoneNumber,"
            "places.internationalPhoneNumber,"
            "places.websiteUri,"
            "places.rating,"
            "places.userRatingCount,"
            "places.businessStatus,"
            "nextPageToken"
        ),
        "Content-Type": "application/json",
    }

    # Request Body
    body = {
        "textQuery": query,
        "languageCode": "de",
        "regionCode": "DE",
        "pageSize": 20,
    }

    # Optional: Place Type Filter (z.B. nur "hair_care" Ergebnisse)
    if place_type:
        body["includedType"] = place_type

    logger.info(f"Suche: '{query}'" + (f" (type={place_type})" if place_type else ""))

    # Erste Seite abrufen
    response = retry_request(
        TEXTSEARCH_URL,
        logger=logger,
        method="POST",
        json_body=body,
        headers=headers,
    )
    if not response:
        return []

    data = response.json()

    # Fehler pruefen
    if "error" in data:
        error = data["error"]
        logger.warning(
            f"Places API Fehler: {error.get('code')} - {error.get('message', '')}"
        )
        return []

    places = data.get("places", [])
    all_results.extend(places)

    if not places:
        logger.info(f"Keine Ergebnisse fuer '{query}'")
        return []

    logger.info(f"Seite 1: {len(places)} Ergebnisse")

    # Weitere Seiten abrufen (max 3 Seiten)
    page = 2
    while "nextPageToken" in data and page <= 3:
        time.sleep(2)  # Google braucht kurz bevor der Token gueltig wird

        body_next = {
            "textQuery": query,
            "languageCode": "de",
            "regionCode": "DE",
            "pageSize": 20,
            "pageToken": data["nextPageToken"],
        }
        if place_type:
            body_next["includedType"] = place_type

        response = retry_request(
            TEXTSEARCH_URL,
            logger=logger,
            method="POST",
            json_body=body_next,
            headers=headers,
        )
        if not response:
            break

        data = response.json()
        places = data.get("places", [])
        if not places:
            break

        all_results.extend(places)
        logger.info(f"Seite {page}: {len(places)} Ergebnisse")
        page += 1

    logger.info(f"Insgesamt {len(all_results)} Ergebnisse fuer '{query}'")
    return all_results


def _parse_address_components(components: list[dict]) -> dict:
    """
    Zerlegt die Google Address Components (neues Format) in einzelne Felder.

    Die neue API liefert die Adresse in einem anderen Format als die alte:
    [{"types": ["route"], "longText": "Hauptstraße"}, ...]

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
        # Neue API nutzt "longText" statt "long_name"
        name = comp.get("longText", comp.get("long_name", ""))

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
                    # Place ID aus der neuen API (Format: "places/ChIJ...")
                    place_id = place.get("id", "")
                    if not place_id or place_id in seen_place_ids:
                        continue
                    seen_place_ids.add(place_id)

                    # Name aus displayName-Objekt
                    display_name = place.get("displayName", {})
                    name = display_name.get("text", "") if isinstance(display_name, dict) else str(display_name)

                    if not name:
                        continue

                    lead_id = make_lead_id(name, city_name)

                    # Duplikat-Check
                    if checkpoint.is_duplicate(lead_id):
                        logger.debug(f"Duplikat uebersprungen: {name} ({city_name})")
                        continue

                    # Nur aktive Businesses
                    biz_status = place.get("businessStatus", "OPERATIONAL")
                    if biz_status != "OPERATIONAL":
                        logger.debug(f"Uebersprungen (Status: {biz_status}): {name}")
                        continue

                    # Adresse zerlegen
                    addr = _parse_address_components(
                        place.get("addressComponents", [])
                    )

                    # Website und E-Mail
                    website = place.get("websiteUri", "")
                    email = None
                    if website:
                        logger.debug(f"Extrahiere E-Mail von: {website}")
                        email = extract_email(website, logger)

                    # Strasse + Hausnummer zusammenfuegen
                    street_full = addr["street"]
                    if addr["street_number"]:
                        street_full = f"{addr['street']} {addr['street_number']}"

                    # Telefon: nationale Nummer bevorzugen, sonst internationale
                    phone = place.get("nationalPhoneNumber", "")
                    if not phone:
                        phone = place.get("internationalPhoneNumber", "")

                    # Lead-Objekt erstellen
                    lead = {
                        "business_name": name,
                        "category_key": cat_key,
                        "category_label": label,
                        "street_address": street_full,
                        "postal_code": addr["postal_code"],
                        "city": addr["city"] or city_name,
                        "state": addr["state"] or bundesland,
                        "phone": phone,
                        "website": website,
                        "email": email or "",
                        "google_rating": place.get("rating", ""),
                        "google_reviews": place.get("userRatingCount", ""),
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
