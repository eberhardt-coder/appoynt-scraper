from __future__ import annotations

"""
Formatiert die gesammelten Leads als CSV im HubSpot-Import-Format.

HubSpot erwartet bestimmte Spaltenbezeichnungen beim Import. Diese Datei
kuemmert sich um das korrekte Mapping und erstellt zwei separate CSVs:
- leads_with_email.csv: Leads mit E-Mail (sofort fuer Outreach nutzbar)
- leads_cold_calling.csv: Leads ohne E-Mail aber mit Telefon (fuer Cold Calling)
"""

import csv
import logging
from datetime import date
from pathlib import Path

from config.settings import OUTPUT_DIR, LEADS_WITH_EMAIL_CSV, LEADS_COLD_CALLING_CSV, LEADS_WHATSAPP_READY_CSV


# Mapping: Interner Feldname -> HubSpot-Spaltenname
# Diese Spaltennamen werden von HubSpot beim CSV-Import automatisch erkannt.
HUBSPOT_COLUMNS = [
    ("business_name", "Company name"),
    ("category_label", "Industry"),
    ("street_address", "Street address"),
    ("postal_code", "Zip"),
    ("city", "City"),
    ("state", "State/Region"),
    ("phone", "Phone number"),
    ("website", "Website URL"),
    ("email", "Email"),
    ("_empty", "Number of employees"),       # bleibt leer wie gewuenscht
    ("google_rating", "google_rating"),       # Custom Property
    ("google_reviews", "google_reviews"),     # Custom Property
    ("_lead_source", "lead_source"),          # Custom Property, immer "Scraper"
    ("category_label", "branche"),            # Custom Property
    # Sales-Intelligence-Felder (Custom Properties)
    ("has_whatsapp", "has_whatsapp"),              # "true" / "false" (HubSpot Checkbox)
    ("whatsapp_number", "whatsapp_number"),        # z.B. "491234567890"
    ("booking_system", "booking_system"),          # z.B. "Calendly", "Treatwell"
    ("booking_url", "booking_url"),                # URL zur Buchungsseite
    ("sales_opener", "sales_opener"),              # Personalisierter Opener-Text
    ("_scrape_date", "scrape_date"),               # Datum des Scrapes (YYYY-MM-DD)
]


def _write_csv(leads: list[dict], filepath: Path, logger: logging.Logger) -> None:
    """
    Schreibt eine Liste von Leads in eine CSV-Datei im HubSpot-Format.

    Args:
        leads: Liste von Lead-Dictionaries
        filepath: Pfad zur CSV-Datei
        logger: Logger
    """
    if not leads:
        logger.info(f"Keine Leads fuer {filepath.name} -> Datei wird nicht erstellt")
        return

    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Header-Zeile: Die HubSpot-Spaltennamen
    headers = [col_name for _, col_name in HUBSPOT_COLUMNS]

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        # utf-8-sig schreibt ein BOM (Byte Order Mark) an den Anfang.
        # Das sorgt dafuer, dass Excel die Umlaute korrekt anzeigt.
        writer = csv.writer(f, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)

        for lead in leads:
            row = []
            for field_key, _ in HUBSPOT_COLUMNS:
                if field_key == "_empty":
                    row.append("")
                elif field_key == "_lead_source":
                    row.append("Scraper")
                elif field_key == "_scrape_date":
                    row.append(date.today().isoformat())
                else:
                    value = lead.get(field_key, "")
                    # Booleans in HubSpot-kompatible Strings umwandeln
                    # HubSpot erkennt "true"/"false" als Checkbox-Werte
                    if isinstance(value, bool):
                        value = "true" if value else "false"
                    row.append(value)
            writer.writerow(row)

    logger.info(f"{len(leads)} Leads geschrieben -> {filepath}")


def export_to_hubspot_csv(leads: list[dict], logger: logging.Logger) -> tuple[Path, Path]:
    """
    Hauptfunktion: Teilt Leads in drei Kategorien auf und schreibt drei CSVs.

    - leads_with_email.csv: Leads mit E-Mail (fuer Cold Email Outreach)
    - leads_cold_calling.csv: Leads ohne E-Mail, aber mit Telefon (fuer Cold Calling)
    - leads_whatsapp_ready.csv: Leads die WhatsApp nutzen (heisseste Leads)

    Leads ohne E-Mail UND ohne Telefon werden verworfen (nicht nutzbar).

    Args:
        leads: Alle gesammelten Leads
        logger: Logger

    Returns:
        Tuple mit Pfaden zu (leads_with_email.csv, leads_cold_calling.csv)
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    leads_with_email = [l for l in leads if l.get("email")]

    # Cold Calling: Keine E-Mail, aber Telefonnummer vorhanden -> anrufbar
    leads_cold_calling = [
        l for l in leads if not l.get("email") and l.get("phone")
    ]

    # WhatsApp-ready: Leads die bereits WhatsApp nutzen (heisseste Leads fuer APPOYNT)
    leads_whatsapp_ready = [l for l in leads if l.get("has_whatsapp")]

    # Leads ohne E-Mail UND ohne Telefon sind nutzlos -> zaehlen aber loggen
    leads_discarded = [
        l for l in leads if not l.get("email") and not l.get("phone")
    ]

    logger.info(
        f"Export: {len(leads_with_email)} Leads mit E-Mail, "
        f"{len(leads_cold_calling)} Leads fuer Cold Calling (nur Telefon), "
        f"{len(leads_whatsapp_ready)} Leads mit WhatsApp"
    )
    if leads_discarded:
        logger.info(
            f"Verworfen: {len(leads_discarded)} Leads ohne E-Mail und ohne Telefon"
        )

    _write_csv(leads_with_email, LEADS_WITH_EMAIL_CSV, logger)
    _write_csv(leads_cold_calling, LEADS_COLD_CALLING_CSV, logger)
    _write_csv(leads_whatsapp_ready, LEADS_WHATSAPP_READY_CSV, logger)

    return LEADS_WITH_EMAIL_CSV, LEADS_COLD_CALLING_CSV
