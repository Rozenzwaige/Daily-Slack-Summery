"""Israeli court public session calendar.

Sources tried in order:
  1. Supreme Court public decisions list (supremedecisions.court.gov.il)
  2. netHaMishpat public search API  (www.nethamishpat.gov.il)
  3. court.gov.il homepage calendar widget

All sources are best-effort; empty list is returned on total failure.
"""

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CalendarBot/1.0)",
    "Accept-Language": "he-IL,he;q=0.9",
}


def _extract_date(text: str) -> str:
    m = re.search(r"\d{1,2}[./]\d{1,2}[./]\d{4}", text)
    if not m:
        return ""
    parts = re.split(r"[./]", m.group())
    return f"{int(parts[0]):02d}/{int(parts[1]):02d}/{parts[2]}"


def _in_window(date_str: str, days: int) -> bool:
    if not date_str:
        return True  # keep rows whose date we couldn't parse
    try:
        day, month, year = date_str.split("/")
        d = datetime(int(year), int(month), int(day))
        return datetime.now() <= d <= datetime.now() + timedelta(days=days)
    except Exception:
        return True


def _supreme_court() -> list[list]:
    rows = []
    try:
        resp = requests.get(
            "https://supremedecisions.court.gov.il/Home/ListDecisions",
            headers=_HEADERS,
            timeout=15,
        )
        if not resp.ok:
            return rows
        soup = BeautifulSoup(resp.text, "lxml")
        for tr in soup.select("table tbody tr"):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            date_str = _extract_date(cells[0].get_text(strip=True))
            desc      = cells[1].get_text(" ", strip=True)[:200]
            link_tag  = tr.find("a", href=True)
            link      = link_tag["href"] if link_tag else ""
            if not link.startswith("http"):
                link = "https://supremedecisions.court.gov.il" + link
            if desc:
                rows.append([date_str, "", "בית המשפט העליון", desc, link])
    except Exception as e:
        print(f"[courts] supremedecisions error: {e}")
    return rows


def _court_gov_il() -> list[list]:
    rows = []
    try:
        resp = requests.get(
            "https://www.court.gov.il/Hebrew/Pages/Home.aspx",
            headers=_HEADERS,
            timeout=15,
        )
        if not resp.ok:
            return rows
        soup = BeautifulSoup(resp.text, "lxml")
        for item in soup.select(
            ".calendar-item, .upcoming-hearing, .court-event, "
            ".ms-rtestate-field li, article"
        ):
            text     = item.get_text(" ", strip=True)
            link_tag = item.find("a", href=True)
            link     = link_tag["href"] if link_tag else ""
            if link and not link.startswith("http"):
                link = "https://www.court.gov.il" + link
            date_str = _extract_date(text)
            if text:
                rows.append([date_str, "", "בתי המשפט", text[:200], link])
    except Exception as e:
        print(f"[courts] court.gov.il error: {e}")
    return rows


def get_events(days_ahead: int = 7) -> list[list]:
    rows = _supreme_court() + _court_gov_il()
    filtered = [r for r in rows if _in_window(r[0], days_ahead)]
    print(f"[courts] {len(filtered)} events (from {len(rows)} raw)")
    return filtered
