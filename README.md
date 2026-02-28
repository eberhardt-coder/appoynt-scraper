# APPOYNT Lead Scraper

Lead-Scraper fuer APPOYNT: Sammelt Business-Kontaktdaten (inkl. E-Mail) von Google Maps fuer Cold-Email-Outreach.

## Setup

### 1. Python-Abhaengigkeiten installieren

```bash
cd appoynt-scraper
pip install -r requirements.txt
```

### 2. Google API Key einrichten

1. Gehe zu [Google Cloud Console](https://console.cloud.google.com/)
2. Erstelle ein Projekt (oder waehle ein bestehendes)
3. Aktiviere die **Places API** unter APIs & Services > Library
4. Erstelle einen API Key unter APIs & Services > Credentials
5. Kopiere `.env.example` nach `.env` und trage deinen Key ein:

```bash
cp .env.example .env
# Dann .env bearbeiten und GOOGLE_API_KEY eintragen
```

## Verwendung

### Einzelne Stadt + Kategorie

```bash
python run.py --category friseur --city Berlin
```

### Nur eine Kategorie, alle Staedte

```bash
python run.py --category kosmetik
```

### Nur eine Stadt, alle Kategorien

```bash
python run.py --city Hamburg
```

### Batch-Modus (alles scrapen)

```bash
python run.py --all
```

### Checkpoint zuruecksetzen

```bash
python run.py --all --reset
```

## Verfuegbare Kategorien

| Key | Beschreibung |
|-----|-------------|
| `friseur` | Friseure / Friseursalons |
| `kosmetik` | Kosmetikstudios / Beauty Salons |
| `piercing` | Piercing Studios |
| `tattoo` | Tattoo Studios |

## Output

Nach dem Scraping findest du zwei CSV-Dateien im `output/`-Ordner:

- `leads_with_email.csv` - Leads mit E-Mail-Adresse (HubSpot-Import-Format)
- `leads_no_email.csv` - Leads ohne E-Mail (fuer manuellen Check)

Die CSVs koennen direkt in HubSpot importiert werden.

## Projektstruktur

```
appoynt-scraper/
├── config/
│   ├── cities.json          # Top 100 deutsche Staedte
│   ├── categories.json      # Suchbegriffe pro Branche
│   └── settings.py          # API Keys, Rate Limits, etc.
├── src/
│   ├── scraper.py           # Haupt-Scraper (Google Places API)
│   ├── email_extractor.py   # E-Mail von Websites extrahieren
│   ├── hubspot_formatter.py # CSV im HubSpot-Format
│   └── utils.py             # Logging, Retry, Checkpoints
├── output/
│   ├── leads_with_email.csv
│   ├── leads_no_email.csv
│   └── checkpoints/         # Fortschritt-Dateien
├── logs/                    # Log-Dateien
├── run.py                   # CLI Einstiegspunkt
├── requirements.txt
├── .env.example
└── README.md
```

## Konfiguration anpassen

### Staedte aendern
Bearbeite `config/cities.json` - fuege Staedte hinzu oder entferne sie.

### Kategorien aendern
Bearbeite `config/categories.json` - fuege neue Branchen oder Suchbegriffe hinzu.

### Rate Limits aendern
Bearbeite `config/settings.py` - passe `REQUEST_DELAY_SECONDS` an.
