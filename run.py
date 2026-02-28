#!/usr/bin/env python3
"""
APPOYNT Lead Scraper - CLI Einstiegspunkt

Verwendung:
    # Einzelne Stadt + Kategorie scrapen:
    python run.py --category friseur --city Berlin

    # Nur eine Kategorie, alle Staedte:
    python run.py --category friseur

    # Nur eine Stadt, alle Kategorien:
    python run.py --city Berlin

    # Alles scrapen (alle Staedte x alle Kategorien):
    python run.py --all

    # Checkpoint zuruecksetzen (bei Neustart von vorne):
    python run.py --all --reset
"""

import argparse
import sys
import json
from pathlib import Path

# Projektverzeichnis zum Python-Pfad hinzufuegen,
# damit die Imports aus config/ und src/ funktionieren
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.utils import setup_logging, CheckpointManager
from src.scraper import scrape_leads, load_categories
from src.hubspot_formatter import export_to_hubspot_csv
from config.settings import CATEGORIES_FILE


def main():
    parser = argparse.ArgumentParser(
        description="APPOYNT Lead Scraper - Sammelt Business-Leads von Google Maps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python run.py --category friseur --city Berlin
  python run.py --category kosmetik
  python run.py --city Hamburg
  python run.py --all
  python run.py --all --reset
        """,
    )

    parser.add_argument(
        "--category",
        type=str,
        help="Kategorie-Key (z.B. friseur, kosmetik, piercing, tattoo)",
    )
    parser.add_argument(
        "--city",
        type=str,
        help="Stadtname (z.B. Berlin, Hamburg, Muenchen)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Alle Staedte und Kategorien scrapen (Batch-Modus)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Checkpoint zuruecksetzen und von vorne beginnen",
    )

    args = parser.parse_args()

    # Mindestens eine Option muss angegeben sein
    if not args.all and not args.category and not args.city:
        parser.print_help()
        print("\nFehler: Bitte --all, --category und/oder --city angeben.")
        sys.exit(1)

    # Logger starten
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("APPOYNT Lead Scraper gestartet")
    logger.info("=" * 60)

    # Verfuegbare Kategorien anzeigen
    categories = load_categories()
    logger.info(f"Verfuegbare Kategorien: {', '.join(categories.keys())}")

    # Kategorie validieren falls angegeben
    if args.category and args.category not in categories:
        logger.error(
            f"Unbekannte Kategorie: '{args.category}'. "
            f"Verfuegbar: {', '.join(categories.keys())}"
        )
        sys.exit(1)

    # Checkpoint zuruecksetzen?
    if args.reset:
        checkpoint = CheckpointManager()
        checkpoint.reset()
        logger.info("Checkpoint zurueckgesetzt - starte von vorne")

    # Modus anzeigen
    if args.all:
        logger.info("Modus: BATCH (alle Staedte x alle Kategorien)")
    else:
        city_str = args.city or "alle Staedte"
        cat_str = args.category or "alle Kategorien"
        logger.info(f"Modus: {city_str} / {cat_str}")

    # Scraping starten
    leads = scrape_leads(
        specific_city=args.city,
        specific_category=args.category,
        logger=logger,
    )

    # Ergebnisse als CSV exportieren
    if leads:
        csv_email, csv_no_email = export_to_hubspot_csv(leads, logger)
        logger.info("=" * 60)
        logger.info("Export abgeschlossen!")
        logger.info(f"  Leads mit E-Mail:  {csv_email}")
        logger.info(f"  Leads ohne E-Mail: {csv_no_email}")
        logger.info("=" * 60)
    else:
        logger.warning("Keine Leads gefunden. Pruefe deinen API Key und die Suchbegriffe.")

    logger.info("Scraper beendet.")


if __name__ == "__main__":
    main()
