"""Israeli court events via legal-news RSS feeds.

The official court websites (court.gov.il, supremedecisions.court.gov.il)
block all automated access at the network level (Imperva WAF).

Instead we aggregate RSS feeds from major Israeli news outlets and filter
for articles that cover court hearings, verdicts, and rulings.

Sources chosen based on court-coverage density testing:
  ישראל היום  — ~13 court articles per feed cycle
  וואלה        — ~6
  מעריב        — ~4
  ynet         — ~2
  גלובס        — ~1
"""

import feedparser
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

_FEEDS = [
    ("ישראל היום", "https://www.israelhayom.co.il/rss.xml"),
    ("וואלה",       "https://rss.walla.co.il/feed/1"),
    ("מעריב",       "https://www.maariv.co.il/rss/rssfeedsopenaccess"),
    ("ynet",        "https://www.ynet.co.il/Integration/StoryRss1854.xml"),
    ("גלובס",       "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iid=585"),
]

# Keywords that indicate court-related content
_COURT_KW = [
    "בית משפט", "בית-משפט",
    "בית המשפט",
    "עליון", "מחוזי", "שלום",
    "עתירה",
    "פסק דין", "פסק-דין",
    "גזר דין", "גזר-דין",
    "דיון",
    "נאשם", "נאשמת",
    "תביעה",
    "שופט", "שופטת",
    "הרשעה", "זיכוי",
    "מעצר", "ערעור",
    "היועמ\"ש", "היועץ המשפטי",
    "פרקליטות",
]

_KW_RE = re.compile("|".join(re.escape(kw) for kw in _COURT_KW))


def _is_court(entry: dict) -> bool:
    text = entry.get("title", "") + " " + entry.get("summary", "")
    return bool(_KW_RE.search(text))


def _parse_pub_date(entry: dict) -> tuple[str, str]:
    """Return (DD/MM/YYYY, HH:MM) from RSS entry."""
    raw = entry.get("published", "") or entry.get("updated", "")
    if not raw:
        return datetime.now().strftime("%d/%m/%Y"), ""
    try:
        dt = parsedate_to_datetime(raw)
        # Convert to Israel time (UTC+3 summer / UTC+2 winter — approximate with +3)
        dt_il = dt.astimezone(timezone(timedelta(hours=3)))
        return dt_il.strftime("%d/%m/%Y"), dt_il.strftime("%H:%M")
    except Exception:
        return datetime.now().strftime("%d/%m/%Y"), ""


def _clean(text: str, max_len: int = 200) -> str:
    text = re.sub(r"<[^>]+>", " ", text)   # strip HTML tags
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def get_events(days_ahead: int = 7) -> list[list]:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days_ahead)
    seen_urls: set[str] = set()
    rows: list[list] = []

    for source, url in _FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                if not _is_court(entry):
                    continue
                # Skip old articles
                raw_date = entry.get("published_parsed") or entry.get("updated_parsed")
                if raw_date:
                    pub_dt = datetime(*raw_date[:6], tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        continue

                link = entry.get("link", "")
                if link in seen_urls:
                    continue
                seen_urls.add(link)

                date_str, time_str = _parse_pub_date(entry)
                title   = _clean(entry.get("title", ""))
                summary = _clean(entry.get("summary", ""), 200)
                rows.append([date_str, time_str, title, summary, link])
        except Exception as e:
            print(f"[courts] {source} error: {e}")

    print(f"[courts] {len(rows)} court articles from {len(_FEEDS)} feeds")
    return rows
