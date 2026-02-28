from __future__ import annotations

"""
E-Mail-Extraktion von Business-Websites.

Strategie:
1. Hauptseite laden und nach E-Mails suchen (mailto-Links + Regex)
2. Falls nichts gefunden: typische Unterseiten pruefen (/kontakt, /impressum, etc.)
3. Generische E-Mails (info@, noreply@, etc.) werden niedriger priorisiert
"""

import re
import logging
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import HTTP_TIMEOUT_SECONDS, REQUEST_DELAY_SECONDS
import time


# Regex fuer E-Mail-Adressen
# Findet Muster wie: name@domain.de, info@salon-hamburg.com
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Typische Unterseiten auf denen deutsche Businesses ihre E-Mail zeigen
CONTACT_PATHS = [
    "/kontakt",
    "/contact",
    "/impressum",
    "/about",
    "/ueber-uns",
    "/about-us",
]

# E-Mail-Adressen die wir ignorieren wollen (keine echten Kontaktadressen)
BLACKLIST_PATTERNS = [
    r".*@example\.",
    r".*@test\.",
    r".*@localhost",
    r"noreply@",
    r"no-reply@",
    r"mailer-daemon@",
    r".*@sentry\.",
    r".*@wixpress\.",
    r".*@google\.",
    r".*@facebook\.",
    r".*@instagram\.",
]


def _is_valid_email(email: str) -> bool:
    """
    Prueft ob eine E-Mail-Adresse gueltig und relevant ist.
    Filtert Muell-Adressen wie noreply@, test@, etc. raus.
    """
    email = email.lower().strip()

    # Zu kurz oder zu lang?
    if len(email) < 5 or len(email) > 254:
        return False

    # Gegen Blacklist pruefen
    for pattern in BLACKLIST_PATTERNS:
        if re.match(pattern, email, re.IGNORECASE):
            return False

    # Muss eine echte TLD haben (mindestens 2 Zeichen nach dem letzten Punkt)
    domain = email.split("@")[-1]
    if "." not in domain:
        return False

    return True


def _extract_emails_from_html(html: str) -> list[str]:
    """
    Extrahiert E-Mail-Adressen aus HTML auf zwei Wegen:

    1. mailto:-Links (zuverlaessigste Methode, bewusst platzierte E-Mails)
    2. Regex-Suche im gesamten Text (fängt auch E-Mails ohne Link auf)
    """
    emails = set()
    soup = BeautifulSoup(html, "html.parser")

    # Methode 1: mailto:-Links durchsuchen
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.startswith("mailto:"):
            # "mailto:info@salon.de?subject=..." -> "info@salon.de"
            email = href.replace("mailto:", "").split("?")[0].strip()
            if _is_valid_email(email):
                emails.add(email.lower())

    # Methode 2: Regex ueber den gesamten Seitentext
    text = soup.get_text(separator=" ")
    for match in EMAIL_REGEX.findall(text):
        if _is_valid_email(match):
            emails.add(match.lower())

    # Auch im rohen HTML suchen (manche E-Mails stehen in Attributen)
    for match in EMAIL_REGEX.findall(html):
        if _is_valid_email(match):
            emails.add(match.lower())

    return list(emails)


def _pick_best_email(emails: list[str]) -> str | None:
    """
    Waehlt die beste E-Mail aus einer Liste.

    Priorisierung:
    1. Persoenliche E-Mails (nicht info@, kontakt@, etc.) -> am wertvollsten
    2. info@, kontakt@, office@ -> Standard-Geschaefts-E-Mails
    3. Alles andere
    """
    if not emails:
        return None

    # Generische Praefixe die auf eine allgemeine E-Mail hindeuten
    generic_prefixes = {"info", "kontakt", "contact", "office", "mail", "hello", "hallo"}

    personal = []
    generic = []

    for email in emails:
        prefix = email.split("@")[0].lower()
        if prefix in generic_prefixes:
            generic.append(email)
        else:
            personal.append(email)

    # Persoenliche E-Mails bevorzugen, dann generische
    if personal:
        return personal[0]
    if generic:
        return generic[0]
    return emails[0]


def extract_email(website_url: str, logger: logging.Logger | None = None) -> str | None:
    """
    Hauptfunktion: Versucht eine E-Mail-Adresse von einer Website zu extrahieren.

    Ablauf:
    1. Hauptseite laden und nach E-Mails suchen
    2. Falls nichts gefunden: Unterseiten wie /kontakt, /impressum pruefen
    3. Beste E-Mail auswaehlen und zurueckgeben

    Args:
        website_url: URL der Business-Website (z.B. "https://www.salon-berlin.de")
        logger: Optional - Logger fuer Statusmeldungen

    Returns:
        E-Mail-Adresse als String oder None wenn keine gefunden wurde
    """
    if not website_url:
        return None

    # URL normalisieren (https:// hinzufuegen falls fehlend)
    if not website_url.startswith(("http://", "https://")):
        website_url = "https://" + website_url

    all_emails = []

    # Schritt 1: Hauptseite pruefen
    try:
        time.sleep(REQUEST_DELAY_SECONDS)
        response = requests.get(
            website_url,
            timeout=HTTP_TIMEOUT_SECONDS,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AppointBot/1.0)"},
            allow_redirects=True,
        )
        response.raise_for_status()
        emails = _extract_emails_from_html(response.text)
        all_emails.extend(emails)

        if logger:
            logger.debug(f"Hauptseite {website_url}: {len(emails)} E-Mail(s) gefunden")

    except requests.exceptions.RequestException as e:
        if logger:
            logger.debug(f"Fehler beim Laden von {website_url}: {e}")
        return None

    # Schritt 2: Falls keine E-Mail gefunden, Unterseiten pruefen
    if not all_emails:
        base_url = f"{urlparse(website_url).scheme}://{urlparse(website_url).netloc}"

        for path in CONTACT_PATHS:
            sub_url = urljoin(base_url, path)
            try:
                time.sleep(REQUEST_DELAY_SECONDS)
                response = requests.get(
                    sub_url,
                    timeout=HTTP_TIMEOUT_SECONDS,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; AppointBot/1.0)"},
                    allow_redirects=True,
                )
                # 404 ist OK, heisst nur "Seite gibt es nicht"
                if response.status_code == 404:
                    continue
                response.raise_for_status()

                emails = _extract_emails_from_html(response.text)
                all_emails.extend(emails)

                if logger:
                    logger.debug(f"Unterseite {sub_url}: {len(emails)} E-Mail(s) gefunden")

                # Sobald wir was gefunden haben, aufhoeren
                if all_emails:
                    break

            except requests.exceptions.RequestException:
                continue

    # Schritt 3: Beste E-Mail auswaehlen
    best = _pick_best_email(list(set(all_emails)))
    if logger and best:
        logger.debug(f"Beste E-Mail fuer {website_url}: {best}")

    return best
