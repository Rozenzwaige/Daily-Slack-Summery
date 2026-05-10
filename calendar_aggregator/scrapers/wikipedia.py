"""Hebrew Wikipedia 'on this day' scraper.

For each of the next DAYS_AHEAD days, fetches the Hebrew Wikipedia page for that
date and extracts two sections:

  • אירועים היסטוריים ביום זה  — events that happened on this date (with year)
  • חגים ואירועים החלים ביום זה — annual observances, filtered to remove
                                   foreign/irrelevant holidays

Wikipedia URL format:  https://he.wikipedia.org/wiki/{day}_ב{month_heb}
Example:               https://he.wikipedia.org/wiki/10_במאי  (10 May)

The pages use a modern <section> structure where each thematic block is wrapped
in a <section> element containing its <h2> heading and item <ul>.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta

_WIKI_BASE = "https://he.wikipedia.org/wiki"

_HE_MONTHS = {
    1: "ינואר",  2: "פברואר", 3: "מרץ",    4: "אפריל",
    5: "מאי",    6: "יוני",   7: "יולי",   8: "אוגוסט",
    9: "ספטמבר", 10: "אוקטובר", 11: "נובמבר", 12: "דצמבר",
}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CalendarBot/1.0; +https://github.com)",
    "Accept-Language": "he-IL,he;q=0.9",
}

# Patterns that mark a holiday as non-Israeli / not relevant.
# Matched case-insensitively against the full item text.
_SKIP_PATTERNS = [
    r"ראש השנה האזרחית",
    r"חג מילת ישו",
    r"קוואנזה",
    r"יום נחלת הכלל",
    r"שחרור קובה",
    r"יום העצמאות של (?!ישראל)",   # foreign independence days (keep Israel's)
    r"יום הרפובליקה של",
    r"יום הלאומי של (?!ישראל)",
    r"יום המדינה של (?!ישראל)",
    r"יום ההולדת של המלך",
    r"חג לאומי של",
    r"חג לאומי ב",
    r"ראש השנה (?:הסיני|הפרסי|הוייטנאמי|ה?ה?הינדי)",
    r"חג ה(?:מולד|עלייה לשמים|תחייה|פנטקוסטה|אפיפניה|קורפוס)",   # Christian
    r"רמדאן|עיד אל|מוחרם|אשורא|מילאד",                             # Islamic
    r"דיואלי|הולי|ויסאק|נוורוז",                                     # Other
]
_SKIP_RE = [re.compile(p) for p in _SKIP_PATTERNS]


def _should_skip(text: str) -> bool:
    return any(r.search(text) for r in _SKIP_RE)


def _abs_link(tag) -> str:
    if not tag:
        return ""
    href = tag.get("href", "")
    if href.startswith("http"):
        return href
    if href.startswith("//"):          # protocol-relative
        return "https:" + href
    if href.startswith("/"):
        return "https://he.wikipedia.org" + href
    return ""


def _fetch_day(d: date) -> list[list]:
    date_str = d.strftime("%d/%m/%Y")
    url = f"{_WIKI_BASE}/{d.day}_ב{_HE_MONTHS[d.month]}"
    rows = []

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        content = soup.find("div", class_="mw-parser-output")
        if not content:
            return rows

        # Modern Wikipedia wraps each thematic block in a <section> element.
        # Fall back to flat h2/ul scan for older page structures.
        sections = content.find_all("section", recursive=False)

        if sections:
            for section in sections:
                heading_el = section.find(["h2", "h3"])
                heading = heading_el.get_text(" ", strip=True) if heading_el else ""
                _process_section(heading, section, date_str, rows)
        else:
            # Flat layout: h2/h3 siblings followed by ul siblings
            current = ""
            for el in content.children:
                name = getattr(el, "name", None)
                if name in ("h2", "h3"):
                    current = el.get_text(" ", strip=True)
                elif name == "ul" and current:
                    _process_section(current, el, date_str, rows)

    except Exception as e:
        print(f"[wikipedia] {date_str} error: {e}")

    return rows


def _process_section(heading: str, container, date_str: str, rows: list):
    """Parse items from a section and append matching rows."""
    if re.search(r"אירועים", heading):
        for li in container.find_all("li"):
            text = li.get_text(" ", strip=True)
            if not text:
                continue
            year_m = re.match(r"^(\d{1,4})\s*[–—-]\s*", text)
            year = year_m.group(1) if year_m else ""
            label = f"אירוע היסטורי ({year})" if year else "אירוע היסטורי"
            rows.append([date_str, "", label, text, _abs_link(li.find("a", href=True))])

    elif re.search(r"חגים", heading):
        for li in container.find_all("li"):
            text = li.get_text(" ", strip=True)
            if not text or _should_skip(text):
                continue
            rows.append([date_str, "", "חג / אירוע שנתי", text, _abs_link(li.find("a", href=True))])


def get_events(days_ahead: int = 7) -> list[list]:
    today = date.today()
    rows = []
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        day_rows = _fetch_day(d)
        rows.extend(day_rows)
        print(f"[wikipedia] {d.strftime('%d/%m/%Y')}: {len(day_rows)} items")
        if i < days_ahead - 1:
            time.sleep(1)   # be polite to Wikipedia's servers

    print(f"[wikipedia] total {len(rows)} items")
    return rows
