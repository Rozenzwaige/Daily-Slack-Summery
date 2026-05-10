"""Hebrew Wikipedia — daily holidays and observances.

For each of the next DAYS_AHEAD days, fetches the Hebrew Wikipedia page for
that date and extracts the section:

  חגים ואירועים החלים ביום זה

Each item becomes one row:
  A  DD/MM/YYYY   (current year — the event repeats annually)
  B  (empty)
  C  item text exactly as it appears on the page
  D  first paragraph of the linked Wikipedia article (if the item has a link)
  E  link to the Wikipedia article (if present)

Wikipedia URL format:  https://he.wikipedia.org/wiki/{day}_ב{month_heb}
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


def _abs_link(tag) -> str:
    if not tag:
        return ""
    href = tag.get("href", "")
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://he.wikipedia.org" + href
    return ""


def _fetch_article_intro(url: str) -> str:
    """Return the first non-empty paragraph from a Wikipedia article."""
    if not url or "wikipedia.org" not in url:
        return ""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        content = soup.find("div", class_="mw-parser-output")
        if not content:
            return ""
        for p in content.find_all("p", recursive=False):
            text = re.sub(r"\s+", " ", p.get_text(" ", strip=True))
            if len(text) > 30:
                return text[:400]
        return ""
    except Exception:
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

        sections = content.find_all("section", recursive=False)

        if sections:
            for section in sections:
                heading_el = section.find(["h2", "h3"])
                heading = heading_el.get_text(" ", strip=True) if heading_el else ""
                if re.search(r"חגים", heading):
                    _extract_items(section, date_str, rows)
        else:
            # Flat layout: h2/h3 siblings followed by ul siblings
            in_holidays = False
            for el in content.children:
                name = getattr(el, "name", None)
                if name in ("h2", "h3"):
                    in_holidays = bool(re.search(r"חגים", el.get_text(" ", strip=True)))
                elif name == "ul" and in_holidays:
                    _extract_items(el, date_str, rows)

    except Exception as e:
        print(f"[wikipedia] {date_str} error: {e}")

    return rows


def _extract_items(container, date_str: str, rows: list):
    for li in container.find_all("li"):
        text = li.get_text(" ", strip=True)
        if not text:
            continue
        link_tag = li.find("a", href=True)
        link = _abs_link(link_tag)
        description = ""
        if link:
            description = _fetch_article_intro(link)
            time.sleep(0.5)  # be polite between article fetches
        rows.append([date_str, "", text, description, link])


def get_events(days_ahead: int = 7) -> list[list]:
    today = date.today()
    rows = []
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        day_rows = _fetch_day(d)
        rows.extend(day_rows)
        print(f"[wikipedia] {d.strftime('%d/%m/%Y')}: {len(day_rows)} items")
        if i < days_ahead - 1:
            time.sleep(1)

    print(f"[wikipedia] total {len(rows)} items")
    return rows
