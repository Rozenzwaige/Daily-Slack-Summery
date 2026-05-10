"""Israeli court hearings — extracts upcoming hearing dates from news RSS feeds.

The official court calendar (court.gov.il/ngcs.web.site) requires authentication
and cannot be accessed publicly.  Instead we:

  1. Fetch RSS feeds from major Israeli news outlets.
  2. For each article that mentions a court context, ask Claude to determine
     whether it mentions a *future* hearing with a specific date/time.
  3. Return only rows where Claude extracts structured upcoming-hearing data.

Claude model: claude-haiku-4-5-20251001 (fast, cheap, Hebrew-capable).
"""

import json
import os
import re
import feedparser
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

try:
    import anthropic
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False

_FEEDS = [
    ("ישראל היום", "https://www.israelhayom.co.il/rss.xml"),
    ("וואלה",       "https://rss.walla.co.il/feed/1"),
    ("מעריב",       "https://www.maariv.co.il/rss/rssfeedsopenaccess"),
    ("ynet",        "https://www.ynet.co.il/Integration/StoryRss1854.xml"),
    ("גלובס",       "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iid=585"),
]

# Pre-filter: only send articles that look court-related to Claude
_COURT_KW = [
    "בית משפט", "בית-משפט", "בית המשפט",
    "עליון", "מחוזי", "שלום",
    "עתירה", "פסק דין", "פסק-דין", "גזר דין",
    "דיון", "נאשם", "נאשמת", "תביעה",
    "שופט", "שופטת", "הרשעה", "זיכוי",
    "מעצר", "ערעור", "היועמ\"ש", "פרקליטות",
]
_KW_RE = re.compile("|".join(re.escape(kw) for kw in _COURT_KW))

_SYSTEM_PROMPT = """אתה עוזר שמחלץ מידע על דיוני בתי משפט עתידיים מכתבות חדשות בעברית.

עבור כל כתבה, החלט האם היא מזכירה דיון משפטי עתידי (שטרם התקיים) עם תאריך ספציפי.

**השב תמיד ב-JSON בלבד**, ללא טקסט נוסף.

אם הכתבה מזכירה דיון עתידי עם תאריך:
{
  "has_upcoming_hearing": true,
  "date": "DD/MM/YYYY",
  "time": "HH:MM או ריק אם לא ידוע",
  "event": "תיאור קצר של הדיון (עד 60 תווים)",
  "description": "תיאור מורחב (עד 200 תווים)"
}

אם אין דיון עתידי עם תאריך ספציפי:
{
  "has_upcoming_hearing": false
}

כללים:
- "דיון עתידי" = דיון שיתקיים אחרי פרסום הכתבה
- חייב להיות תאריך ספציפי (לא "בשבוע הבא" בלי תאריך)
- אל תמציא תאריכים — רק מה שמופיע בטקסט
- אם יש שנה לא מלאה, הנח שנה נוכחית"""

_MAX_ARTICLE_CHARS = 800


def _is_court_related(entry: dict) -> bool:
    text = entry.get("title", "") + " " + entry.get("summary", "")
    return bool(_KW_RE.search(text))


def _clean(text: str, max_len: int = _MAX_ARTICLE_CHARS) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def _parse_pub_date(entry: dict):
    raw = entry.get("published", "") or entry.get("updated", "")
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        return None


def _ask_claude(client, title: str, summary: str, link: str) -> dict | None:
    """Send one article to Claude and return parsed JSON or None."""
    article_text = f"כותרת: {title}\n\nתקציר: {summary}"
    if link:
        article_text += f"\n\nקישור: {link}"

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": article_text}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except Exception as e:
        print(f"[courts] Claude error for '{title[:40]}': {e}")
        return None


def get_events(days_ahead: int = 7) -> list[list]:
    if not _HAS_ANTHROPIC:
        print("[courts] 'anthropic' package not installed — skipping")
        return []

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("[courts] ANTHROPIC_API_KEY not set — skipping")
        return []

    client = anthropic.Anthropic(api_key=api_key)
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days_ahead)
    seen_urls: set[str] = set()
    candidate_articles: list[dict] = []

    # --- Collect candidate articles from all feeds ---
    for source, url in _FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                if not _is_court_related(entry):
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
                candidate_articles.append({
                    "source": source,
                    "title":   _clean(entry.get("title", ""), 120),
                    "summary": _clean(entry.get("summary", ""), _MAX_ARTICLE_CHARS),
                    "link":    link,
                })
        except Exception as e:
            print(f"[courts] {source} feed error: {e}")

    print(f"[courts] {len(candidate_articles)} candidate articles → sending to Claude")

    # --- Ask Claude to extract upcoming hearings ---
    rows: list[list] = []
    for art in candidate_articles:
        result = _ask_claude(client, art["title"], art["summary"], art["link"])
        if not result or not result.get("has_upcoming_hearing"):
            continue

        date_str  = result.get("date", "")
        time_str  = result.get("time", "")
        event     = result.get("event", art["title"])[:60]
        desc      = result.get("description", art["summary"])[:200]
        link      = art["link"]

        if not date_str:
            continue

        rows.append([date_str, time_str, event, desc, link])

    print(f"[courts] {len(rows)} upcoming hearings extracted by Claude")
    return rows
