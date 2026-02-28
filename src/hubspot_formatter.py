from __future__ import annotations

"""
Formatiert die gesammelten Leads als CSV im HubSpot-Import-Format.

HubSpot erwartet bestimmte Spaltenbezeichnungen beim Import. Diese Datei
kuemmert sich um das korrekte Mapping und erstellt zwei separate CSVs:
- leads_with_email.csv: Leads mit E-Mail (sofort fuer Outreach nutzbar)
- leads_no_email.csv: Leads ohne E-Mail (fuer manuellen Check)
"""

import csv
import logging
from pathlib import Path

from config.settings import OUTPUT_DIR, LEADS_WITH_EMAIL_CSV, LEADS_NO_EMAIL_CSV


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
                else:
                    row.append(lead.get(field_key, ""))
            writer.writerow(row)

    logger.info(f"{len(leads)} Leads geschrieben -> {filepath}")


def export_to_hubspot_csv(leads: list[dict], logger: logging.Logger) -> tuple[Path, Path]:
    """
    Hauptfunktion: Teilt Leads in mit/ohne E-Mail auf und schreibt zwei CSVs.

    Args:
        leads: Alle gesammelten Leads
        logger: Logger

    Returns:
        Tuple mit Pfaden zu (leads_with_email.csv, leads_no_email.csv)
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    leads_with_email = [l for l in leads if l.get("email")]
    leads_no_email = [l for l in leads if not l.get("email")]

    logger.info(
        f"Export: {len(leads_with_email)} Leads mit E-Mail, "
        f"{len(leads_no_email)} Leads ohne E-Mail"
    )

    _write_csv(leads_with_email, LEADS_WITH_EMAIL_CSV, logger)
    _write_csv(leads_no_email, LEADS_NO_EMAIL_CSV, logger)

    return LEADS_WITH_EMAIL_CSV, LEADS_NO_EMAIL_CSV
