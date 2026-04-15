#!/usr/bin/env python3
"""
Standing Together — Daily News Summary
Collects Israeli & Palestinian news from the last 24 hours,
filters by relevant topics, summarizes in Hebrew with Claude,
and sends to a Slack channel every morning.

Topics: Israeli-Palestinian peace, settler violence, social inequality,
        West Bank, Gaza war, climate.
"""

import os
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import anthropic
import feedparser
import requests
from bs4 import BeautifulSoup

# ─── Credentials (injected as GitHub Secrets / env vars) ─────────────────────

SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

# ─── Topic keywords ───────────────────────────────────────────────────────────

KEYWORDS_HE = [
    "עזה", "גזה", "רצועת עזה",
    "גדה", "הגדה המערבית", "יהודה ושומרון",
    "פלסטינ", "מתנחל", "התנחלות", "כיבוש",
    "ג'נין", "ג׳נין", "שכם", "נאבלוס", "חברון", "רמאללה", "טול כרם", "קלקיליה",
    "רווחה", "עוני", "אי-שוויון", "אי שוויון", "פערים", "הדרה",
    "ביטוח לאומי", "דמי אבטלה", "שכר מינימום", "יוקר המחיה", "דיור",
    "קיצוץ", "תקציב חברתי", "מחאה חברתית",
    "אקלים", "שינוי האקלים", "סביבה", "גל חום", "בצורת", "הצפה",
    "שלום", "הסכם", "הפסקת אש", "משא ומתן", "שחרור חטופים",
    "חטופ", "ערבי", "בדואי", "מגזר ערבי", "דו-קיום",
    "חמאס", "ג'יהאד", "חיזבאלה",
]

KEYWORDS_EN = [
    "Gaza", "West Bank", "settler", "settlement", "Palestinian", "occupation",
    "Jenin", "Nablus", "Hebron", "Ramallah", "Tulkarm", "Qalqilya",
    "welfare", "inequality", "poverty", "social", "housing", "budget cut",
    "climate", "environment", "heat wave", "drought", "flood",
    "ceasefire", "peace", "hostage", "negotiation", "release",
    "Hamas", "Hezbollah", "airstrike", "bombing", "civilian", "casualt",
]

# ─── RSS sources ──────────────────────────────────────────────────────────────

RSS_FEEDS = [
    # Israeli general news
    ("ynet",            "https://www.ynet.co.il/Integration/StoryRss2.xml"),
    ("שיחה מקומית",     "https://www.mekomit.co.il/feed/"),
    ("הארץ",            "https://www.haaretz.co.il/cmlink/1.1458"),
    ("N12",             "https://www.n12.co.il/rss/"),
    ("כאן חדשות",       "https://www.kan.org.il/rss/"),
    # Economy / welfare
    ("גלובס",           "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=1"),
    ("כלכליסט",         "https://www.calcalist.co.il/Rss/RssFeedsTableBody.aspx?TableID=1026&siteID=1"),
    ("דה מרקר",         "https://www.themarker.com/cmlink/1.4458"),
    # International
    ("Guardian Middle East", "https://www.theguardian.com/world/middleeast/rss"),
    ("NYT Middle East",      "https://rss.nytimes.com/services/xml/rss/nyt/MiddleEast.xml"),
    # Palestinian & Arab
    ("Al-Jazeera English",   "https://www.aljazeera.com/xml/rss/all.xml"),
    ("Wafa",                 "https://english.wafa.ps/rss"),
    ("Ma'an News",           "https://www.maannews.com/rss/latest-news"),
]

# Google News RSS — searches specific topics in Hebrew (very up-to-date)
GOOGLE_NEWS_QUERIES = [
    "עזה מלחמה",
    "גדה המערבית מתנחלים",
    "פלסטינים ישראל",
    "אי שוויון חברתי ישראל",
    "רווחה ביטוח לאומי ישראל",
    "אקלים ישראל",
    "שלום ישראל פלסטין",
    "Gaza ceasefire",
    "West Bank settler violence",
]

# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_recent(entry, hours: int = 26) -> bool:
    """Return True if the entry was published within the last N hours."""
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                pub = datetime(*val[:6], tzinfo=timezone.utc)
                cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
                return pub >= cutoff
            except Exception:
                pass
    # If no date is available, include the entry (better to over-include)
    return True


def is_relevant(title: str, summary: str = "") -> bool:
    """Return True if the article matches at least one topic keyword."""
    text = (title + " " + summary).lower()
    for kw in KEYWORDS_HE:
        if kw.lower() in text:
            return True
    for kw in KEYWORDS_EN:
        if kw.lower() in text:
            return True
    return False


def fetch_rss(name: str, url: str) -> list[dict]:
    """Parse an RSS feed and return relevant recent articles."""
    articles = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            if not is_recent(entry):
                continue
            title   = entry.get("title", "").strip()
            summary = entry.get("summary", "").strip()
            link    = entry.get("link", "")
            if not title:
                continue
            if is_relevant(title, summary):
                articles.append({
                    "source":  name,
                    "title":   title,
                    "summary": summary[:300],
                    "link":    link,
                })
    except Exception as e:
        print(f"  ⚠️  RSS error [{name}]: {e}")
    return articles


def fetch_google_news(query: str) -> list[dict]:
    """Fetch Google News RSS for a specific Hebrew/English query."""
    encoded = urllib.parse.quote(query)
    url = (
        f"https://news.google.com/rss/search"
        f"?q={encoded}&hl=iw&gl=IL&ceid=IL:iw&num=15"
    )
    # All Google News results match our query by definition — skip keyword filter
    articles = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            if not is_recent(entry):
                continue
            title = entry.get("title", "").strip()
            link  = entry.get("link", "")
            if title:
                articles.append({
                    "source":  f"Google News",
                    "title":   title,
                    "summary": "",
                    "link":    link,
                })
    except Exception as e:
        print(f"  ⚠️  Google News error [{query}]: {e}")
    return articles


def scrape_site(name: str, url: str, selectors: list[str]) -> list[dict]:
    """Generic scraper — tries a list of CSS selectors to find headlines."""
    articles = []
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        seen = set()
        for sel in selectors:
            for el in soup.select(sel)[:30]:
                title = el.get_text(separator=" ", strip=True)
                if len(title) < 10 or title in seen:
                    continue
                seen.add(title)
                if is_relevant(title):
                    articles.append({
                        "source":  name,
                        "title":   title,
                        "summary": "",
                        "link":    url,
                    })
    except Exception as e:
        print(f"  ⚠️  Scrape error [{name}]: {e}")
    return articles


# ─── Collect all articles ─────────────────────────────────────────────────────

def collect_articles() -> list[dict]:
    all_articles: list[dict] = []

    print("📡 Fetching RSS feeds...")
    for name, url in RSS_FEEDS:
        batch = fetch_rss(name, url)
        print(f"   {name}: {len(batch)}")
        all_articles.extend(batch)
        time.sleep(0.3)  # polite delay

    print("🔍 Fetching Google News topic searches...")
    for query in GOOGLE_NEWS_QUERIES:
        batch = fetch_google_news(query)
        print(f"   [{query}]: {len(batch)}")
        all_articles.extend(batch)
        time.sleep(0.3)

    print("🖥️  Scraping כאן & גל\"צ news pages...")
    all_articles.extend(scrape_site(
        "כאן (רשת ב')",
        "https://www.kan.org.il/news/",
        ["h2", "h3", ".article-title", ".item-title", ".story-title"],
    ))
    all_articles.extend(scrape_site(
        "גל\"צ",
        "https://glz.co.il/news",
        ["h2", "h3", ".article-title", ".news-title"],
    ))

    # Deduplicate by normalised title prefix
    seen: set[str] = set()
    unique: list[dict] = []
    for a in all_articles:
        key = a["title"][:60].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(a)

    print(f"\n📊 Unique relevant articles: {len(unique)}")
    return unique


# ─── Summarise with Claude ────────────────────────────────────────────────────

def summarise(articles: list[dict]) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Build article list (cap at 100 to keep costs low)
    lines = []
    for i, a in enumerate(articles[:100], 1):
        line = f"{i}. [{a['source']}] {a['title']}"
        if a["summary"]:
            line += f"\n   {a['summary'][:250]}"
        lines.append(line)
    articles_text = "\n".join(lines)

    today = datetime.now().strftime("%d/%m/%Y")

    prompt = f"""להלן רשימת כתבות חדשות מ-24 השעות האחרונות ({today}).

אנא צור סיכום יומי מקצועי ותמציתי *בעברית* עבור ארגון "עומדים ביחד" — ארגון ישראלי-פלסטיני שעוסק בשיתוף פעולה, שלום וצדק חברתי.

*מיקוד:* התמקד אך ורק בנושאים הבאים —
1. 🕊️ *שלום ישראלי-פלסטיני* — מגעים, הסכמים, הפסקות אש, שחרור חטופים
2. 🔴 *אלימות מתנחלים ואירועי הגדה המערבית* — התקפות, פשעי שנאה, גירוש
3. 💣 *המלחמה בעזה* — התפתחויות מרכזיות, נפגעים, הסכמים
4. ⚖️ *אי-שוויון חברתי ורווחה* — תקציב, קיצוצים, מחאות, שכר, דיור
5. 🌍 *אקלים וסביבה* — חדשות אקלים מישראל ומהאזור
6. 🗞️ *קול פלסטיני* — מה מדווחים Wafa, Al-Jazeera, Ma'an ועיתונות פלסטינית

*פורמט — השתמש בדיוק בתבנית הזו:*

📰 *סיכום חדשות יומי | {today}*

*🕊️ שלום ישראלי-פלסטיני*
• ...

*🔴 גדה המערבית ומתנחלים*
• ...

*💣 המלחמה בעזה*
• ...

*⚖️ חברה ורווחה*
• ...

*🌍 אקלים וסביבה*
• ...

*🗞️ קול פלסטיני*
• ...

_מקורות: ynet, הארץ, N12, שיחה מקומית, גלובס, כלכליסט, Guardian, NYT, Al-Jazeera, Wafa ועוד_

*כללים:*
- כל נקודה: משפט אחד עד שניים
- 2–4 נקודות לקטגוריה (אם אין חדשות בקטגוריה — כתוב "לא דווח")
- אל תמציא מידע שאינו מופיע ברשימה
- שפה: עברית תקנית ופשוטה

---
כתבות לסיכום:

{articles_text}
---

כתוב את הסיכום:"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


# ─── Send to Slack ────────────────────────────────────────────────────────────

def send_to_slack(text: str):
    """Post the summary to the Slack channel via Incoming Webhook."""
    # Slack section blocks max out at 3000 chars — split if needed
    MAX_BLOCK = 2900
    chunks = [text[i : i + MAX_BLOCK] for i in range(0, len(text), MAX_BLOCK)]

    blocks = []
    for chunk in chunks:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": chunk},
        })
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "🤖 סיכום אוטומטי | Claude AI | עומדים ביחד",
            }
        ],
    })

    payload = {"blocks": blocks}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
    resp.raise_for_status()
    print("✅ Sent to Slack")


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  Standing Together — Daily News  |  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*55}\n")

    articles = collect_articles()

    if not articles:
        msg = f"📰 *סיכום חדשות {datetime.now().strftime('%d/%m/%Y')}*\nלא נמצאו כתבות רלוונטיות ב-24 השעות האחרונות."
        send_to_slack(msg)
        return

    print("\n🤖 Summarising with Claude...")
    summary = summarise(articles)

    print("\n📤 Posting to Slack...")
    send_to_slack(summary)

    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
