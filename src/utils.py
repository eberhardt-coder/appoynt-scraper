from __future__ import annotations

"""
Hilfsfunktionen: Logging, Retry-Logik, Checkpoint-System.

- setup_logging():  Erstellt einen Logger der in Datei UND Konsole schreibt
- retry_request():  Fuehrt HTTP-Requests mit automatischem Retry + Backoff durch
- CheckpointManager: Speichert/laedt den Scraper-Fortschritt als JSON-Datei
"""

import logging
import time
import json
import hashlib
from datetime import datetime
from pathlib import Path

import requests

from config.settings import (
    LOG_DIR,
    CHECKPOINT_DIR,
    MAX_RETRIES,
    RETRY_BACKOFF_SECONDS,
    HTTP_TIMEOUT_SECONDS,
    REQUEST_DELAY_SECONDS,
)


def setup_logging(name: str = "appoynt_scraper") -> logging.Logger:
    """
    Erstellt einen Logger der gleichzeitig in eine Datei und in die Konsole schreibt.

    Die Log-Datei bekommt einen Zeitstempel im Namen, damit du alte Logs nicht
    ueberschreibst. Beispiel: appoynt_scraper_20240115_143022.log
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{name}_{timestamp}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Verhindere doppelte Handler wenn die Funktion mehrfach aufgerufen wird
    if logger.handlers:
        return logger

    # Datei-Handler: Alles loggen (DEBUG und hoeher)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_format)

    # Konsolen-Handler: Nur INFO und hoeher anzeigen (weniger Rauschen)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(levelname)-8s | %(message)s")
    console_handler.setFormatter(console_format)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Log-Datei: {log_file}")
    return logger


# Globaler Zeitstempel fuer Rate Limiting:
# Speichert wann der letzte Request gesendet wurde.
_last_request_time = 0.0


def retry_request(
    url: str,
    params: dict | None = None,
    logger: logging.Logger | None = None,
    method: str = "GET",
    json_body: dict | None = None,
    headers: dict | None = None,
) -> requests.Response | None:
    """
    Fuehrt einen HTTP-Request mit automatischem Rate Limiting und Retry durch.

    So funktioniert's:
    1. Wartet mindestens REQUEST_DELAY_SECONDS seit dem letzten Request (Rate Limiting)
    2. Sendet den Request (GET oder POST)
    3. Bei Fehler: wartet RETRY_BACKOFF_SECONDS * 2^versuch und versucht es nochmal
    4. Nach MAX_RETRIES Versuchen gibt die Funktion None zurueck

    Args:
        url: Die URL fuer den Request
        params: Query-Parameter (fuer GET-Requests)
        logger: Optional - Logger fuer Statusmeldungen
        method: HTTP-Methode ("GET" oder "POST")
        json_body: JSON-Body (fuer POST-Requests)
        headers: Zusaetzliche HTTP-Header

    Returns:
        Response-Objekt bei Erfolg, None bei endgueltigem Fehler
    """
    global _last_request_time

    for attempt in range(1, MAX_RETRIES + 1):
        # Rate Limiting: Warte bis genug Zeit seit dem letzten Request vergangen ist
        elapsed = time.time() - _last_request_time
        if elapsed < REQUEST_DELAY_SECONDS:
            time.sleep(REQUEST_DELAY_SECONDS - elapsed)

        try:
            _last_request_time = time.time()

            if method.upper() == "POST":
                response = requests.post(
                    url, json=json_body, headers=headers, timeout=HTTP_TIMEOUT_SECONDS
                )
            else:
                response = requests.get(
                    url, params=params, headers=headers, timeout=HTTP_TIMEOUT_SECONDS
                )

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            wait_time = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            if logger:
                logger.warning(
                    f"Request fehlgeschlagen (Versuch {attempt}/{MAX_RETRIES}): {e}"
                )
                if attempt < MAX_RETRIES:
                    logger.info(f"Warte {wait_time:.1f}s vor erneutem Versuch...")
            if attempt < MAX_RETRIES:
                time.sleep(wait_time)

    if logger:
        logger.error(f"Request endgueltig fehlgeschlagen nach {MAX_RETRIES} Versuchen: {url}")
    return None


def make_lead_id(name: str, city: str) -> str:
    """
    Erstellt eine eindeutige ID aus Business-Name + Stadt.
    Wird fuer die Duplikat-Erkennung verwendet.

    Beispiel: "Hair Lounge" + "Berlin" -> "a3f8b2c1..."
    """
    raw = f"{name.strip().lower()}|{city.strip().lower()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


class CheckpointManager:
    """
    Speichert den Scraper-Fortschritt in einer JSON-Datei.

    Warum? Wenn der Scraper abstuerzt oder du ihn abbrichst, geht nichts verloren.
    Beim naechsten Start laedt er den Checkpoint und macht dort weiter wo er
    aufgehoert hat.

    Der Checkpoint speichert:
    - processed_keys: Welche Stadt+Kategorie-Kombinationen schon abgearbeitet sind
    - leads: Alle bisher gesammelten Leads
    - seen_ids: IDs aller Businesses die schon gefunden wurden (fuer Duplikat-Check)
    """

    def __init__(self, checkpoint_name: str = "scraper_checkpoint"):
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        self.filepath = CHECKPOINT_DIR / f"{checkpoint_name}.json"
        self.data = self._load()
        # Permanentes Dedup-Register einmal beim Start in den Speicher laden
        self._permanent_ids = self._load_permanent_dedup()

    def _load(self) -> dict:
        """Laedt einen bestehenden Checkpoint oder erstellt einen neuen."""
        if self.filepath.exists():
            with open(self.filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        return {
            "processed_keys": [],  # z.B. ["Berlin|friseur", "Hamburg|friseur"]
            "leads": [],
            "seen_ids": [],
        }

    def save(self) -> None:
        """Speichert den aktuellen Fortschritt auf die Festplatte."""
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def finalize(self) -> None:
        """Am Ende eines Runs aufrufen: sichert alle IDs ins permanente Register."""
        self._save_to_permanent_dedup(self.data["seen_ids"])

    def is_processed(self, city: str, category: str) -> bool:
        """Prueft ob eine Stadt+Kategorie-Kombination schon abgearbeitet wurde."""
        key = f"{city}|{category}"
        return key in self.data["processed_keys"]

    def mark_processed(self, city: str, category: str) -> None:
        """Markiert eine Stadt+Kategorie-Kombination als abgearbeitet."""
        key = f"{city}|{category}"
        if key not in self.data["processed_keys"]:
            self.data["processed_keys"].append(key)
            self.save()

    def is_duplicate(self, lead_id: str) -> bool:
        """Prueft ob ein Business schon in den Ergebnissen ist."""
        return lead_id in self.data["seen_ids"]

    def add_lead(self, lead: dict, lead_id: str) -> None:
        """Fuegt einen neuen Lead hinzu und speichert den Checkpoint.
        Schreibt die ID auch ins permanente Dedup-Register."""
        if not self.is_duplicate(lead_id):
            self.data["seen_ids"].append(lead_id)
            self._permanent_ids.add(lead_id)
            self.data["leads"].append(lead)
            self.save()
            # Permanent speichern alle 50 Leads (batch fuer Performance)
            if len(self.data["seen_ids"]) % 50 == 0:
                self._save_to_permanent_dedup(self.data["seen_ids"])

    def get_leads(self) -> list[dict]:
        """Gibt alle bisher gesammelten Leads zurueck."""
        return self.data["leads"]

    def reset(self) -> None:
        """Setzt den Checkpoint zurueck (Leads + Fortschritt werden geloescht).
        WICHTIG: seen_ids bleiben erhalten, damit kein Lead jemals doppelt generiert wird.
        Das permanente Dedup-Register ueberlebt jeden Reset.
        """
        # Alle bisherigen IDs ins permanente Register sichern
        self._save_to_permanent_dedup(self.data.get("seen_ids", []))

        # Checkpoint zuruecksetzen: Leads und Fortschritt weg, aber seen_ids bleiben
        self.data = {"processed_keys": [], "leads": [], "seen_ids": []}
        self.save()

        # Permanentes Register neu ins Memory laden
        self._permanent_ids = self._load_permanent_dedup()

    def _get_permanent_dedup_path(self) -> Path:
        """Pfad zur permanenten Dedup-Datei (ueberlebt Checkpoint-Resets)."""
        return CHECKPOINT_DIR / "seen_leads_permanent.json"

    def _load_permanent_dedup(self) -> set:
        """Laedt alle jemals gesehenen Lead-IDs aus der permanenten Datei."""
        path = self._get_permanent_dedup_path()
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        return set()

    def _save_to_permanent_dedup(self, ids: list) -> None:
        """Speichert Lead-IDs in die permanente Dedup-Datei (mergt mit bestehenden)."""
        path = self._get_permanent_dedup_path()
        existing = self._load_permanent_dedup()
        existing.update(ids)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted(existing), f)

    def is_duplicate(self, lead_id: str) -> bool:
        """Prueft ob ein Business schon jemals gescrapt wurde.
        Checkt sowohl den aktuellen Checkpoint als auch das permanente Register."""
        return lead_id in self.data["seen_ids"] or lead_id in self._permanent_ids
