#!/usr/bin/env python3
"""
Standing Together — Daily News Summary
Collects Israeli & Palestinian news from the last 24 hours,
filters by relevant topics, summarizes in Hebrew with Claude,
and sends to a Slack channel every morning.

Topics: Israeli-Palestinian peace, settler violence, social inequality,
        West Bank, Gaza war, climate.
"""

import json
import os
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import anthropic
import feedparser
import requests
from bs4 import BeautifulSoup

SEEN_FILE = Path(__file__).parent / "seen_articles.json"


def _article_key(article: dict) -> str:
    url = article.get("link", "").strip()
    if url:
        return url
    return article.get("title", "")[:80].lower().strip()


def load_seen_articles() -> set[str]:
    if not SEEN_FILE.exists():
        return set()
    try:
        data = json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        return {url for url, ts in data.items() if ts >= cutoff}
    except Exception:
        return set()


def save_seen_articles(articles: list[dict]) -> None:
    existing: dict = {}
    if SEEN_FILE.exists():
        try:
            existing = json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    fresh = {url: ts for url, ts in existing.items() if ts >= cutoff}
    now = datetime.now(timezone.utc).isoformat()
    for a in articles:
        key = _article_key(a)
        if key:
            fresh[key] = now
    SEEN_FILE.write_text(json.dumps(fresh, ensure_ascii=False, indent=2), encoding="utf-8")

# ─── Credentials (injected as GitHub Secrets / env vars) ─────────────────────

SLACK_WEBHOOK_URL  = os.environ["SLACK_WEBHOOK_URL"]
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
SLACK_BOT_TOKEN    = os.environ.get("SLACK_BOT_TOKEN", "")
NEWS_INPUTS_CHANNEL = os.environ.get("NEWS_INPUTS_CHANNEL", "")
HAARETZ_COOKIES    = os.environ.get("HAARETZ_COOKIES", "")
GLOBES_COOKIES     = os.environ.get("GLOBES_COOKIES", "")


def _parse_cookies(cookie_str: str) -> dict:
    """Parse a cookie string like 'name1=val1; name2=val2' into a dict."""
    result = {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            result[k.strip()] = v.strip()
    return result


COOKIES_BY_DOMAIN: dict[str, dict] = {}
if HAARETZ_COOKIES:
    _htz = _parse_cookies(HAARETZ_COOKIES)
    COOKIES_BY_DOMAIN["haaretz.co.il"] = _htz
    COOKIES_BY_DOMAIN["themarker.com"] = _htz  # דה מרקר — אותו מנוי
if GLOBES_COOKIES:
    COOKIES_BY_DOMAIN["globes.co.il"] = _parse_cookies(GLOBES_COOKIES)

# ─── Topic keywords ───────────────────────────────────────────────────────────

KEYWORDS_HE = [
    # שלום / עזה / גדה
    "עזה", "גזה", "רצועת עזה",
    "גדה", "הגדה המערבית", "יהודה ושומרון",
    "פלסטינ", "מתנחל", "התנחלות", "כיבוש",
    "ג'נין", "ג׳נין", "שכם", "נאבלוס", "חברון", "רמאללה", "טול כרם", "קלקיליה",
    "מחמוד עבאס", "הרשות הפלסטינית", "הרשות הפלסטינאית",
    # אלימות מתנחלים
    "אלימות מתנחלים", "פוגרום", "גירוש פלסטינים", "הרס בתים",
    "הצתה", "כריתת עצים", "ונדליזם", "פשע שנאה",
    "סנקציות על מתנחלים", "עצור מתנחל", "מתנחלים חמושים",
    "גבעות תנועה", "חוות מתנחלים", "מאחז",
    "טיהור אתני", "טרור יהודי", "טרור המתנחלים",
    "שלום", "הסכם", "הפסקת אש", "משא ומתן", "שחרור חטופים",
    "חטופ", "ערבי", "בדואי", "מגזר ערבי", "דו-קיום",
    "חמאס", "ג'יהאד", "חיזבאלה",
    # חברה, רווחה וחינוך
    "רווחה", "עוני", "אי-שוויון", "אי שוויון", "פערים", "הדרה",
    "ביטוח לאומי", "דמי אבטלה", "שכר מינימום", "יוקר המחיה", "דיור",
    "קיצוץ", "תקציב חברתי", "מחאה חברתית",
    "גזענות", "שירותים חברתיים", "תחבורה ציבורית", "פריפריה",
    "חינוך", "בית ספר", "בתי ספר", "מורים", "מורה", "שביתת מורים",
    "השכלה גבוהה", "אוניברסיטה", "מכללה", "סטודנטים", "שכר לימוד",
    "בגרות", "תלמידים", "פדגוגיה", "משרד החינוך",
    # כלכלה
    "כלכלה", "בנק ישראל", "ריבית", "אינפלציה", "תעסוקה", "אבטלה",
    "תקציב", "תקציב המדינה", "מיסוי", "מס הכנסה", "מע\"מ", "גירעון", "חוב לאומי",
    "שוק ההון", "מניות", "הייטק", "יצוא", "יבוא", "סחר חוץ",
    "מחירים", "עלות המחיה", "צרכנות",
    "סוציאלי", "סוציאליסטי", "סוציאליזם", "הפרטה",
    "איגודי עובדים", "ארגוני עובדים", "מאבקי עובדים",
    "שביתה", "סכסוך עבודה", "פיטורים", "קיצוצים",
    "הסתדרות", "כח לעובדים",
    # שמאל בעולם
    "ברני סנדרס", "אלכסנדריה אוקסיו קורטז", "AOC",
    "ג'רמי קורבין", "ממדני", "מאמדני",
    "מלנשון", "פודמוס", "סומר", "סיריזה", "צבא השמאל",
    "מפלגת הלייבור", "לייבור", "סטארמר",
    "פדרו סנצ'ז", "SPD", "ירוקים גרמניה",
    "פופוליזם שמאלי", "מדיניות שמאלית", "מפלגת שמאל",
    "דמוקרטיה סוציאלית", "סוציאל דמוקרטיה",
    "שמאל קיצוני", "שמאל מתון", "התקדמות פרוגרסיבית",
    # אקלים וסביבה
    "אקלים", "שינוי האקלים", "התחממות גלובלית", "סביבה",
    "גל חום", "בצורת", "הצפה", "זיהום",
    # אלימות ופשיעה בחברה הערבית
    "פשיעה בחברה הערבית", "אלימות בחברה הערבית", "ירי בישוב ערבי",
    "רצח בישוב ערבי", "כנופיה", "מאפיה ערבית", "סחיטה",
    "ירי בעיר", "נרצח", "פשע מאורגן", "אלימות בנגב",
    "אלימות בכפר", "ישוב ערבי", "עיירה ערבית",
    "במגזר הערבי", "יוזמות אברהם",
]

KEYWORDS_EN = [
    # שלום / עזה / גדה
    "Gaza", "West Bank", "settler", "settlement", "Palestinian", "occupation",
    "Jenin", "Nablus", "Hebron", "Ramallah", "Tulkarm", "Qalqilya",
    "Abbas", "Palestinian Authority",
    "settler violence", "settler attack", "pogrom", "arson", "price tag",
    "settler sanctions", "demolition", "outpost", "settler rampage",
    "ethnic cleansing", "Jewish terrorism", "settler terrorism",
    "ceasefire", "peace", "hostage", "negotiation", "release",
    "Hamas", "Hezbollah", "airstrike", "bombing", "civilian", "casualt",
    # חברה ורווחה
    "welfare", "inequality", "poverty", "social", "housing", "budget cut",
    "racism", "discrimination", "public transport", "periphery",
    "education", "university", "school", "teachers strike", "tuition", "students",
    # כלכלה
    "economy", "inflation", "interest rate", "Bank of Israel", "tax", "employment",
    "unemployment", "GDP", "deficit", "stock market", "cost of living",
    "socialist", "socialism", "privatization", "labor union", "workers strike",
    "layoffs", "austerity", "labor dispute", "Histadrut",
    # אקלים
    "climate", "global warming", "environment", "heat wave", "drought", "flood", "pollution",
    # שמאל בעולם
    "Bernie Sanders", "AOC", "Alexandria Ocasio-Cortez", "Jeremy Corbyn", "Mamdani", "Mélenchon", "Melenchon",
    "Podemos", "Sumar", "Syriza", "Labour party", "Keir Starmer",
    "Pedro Sanchez", "left-wing party", "progressive policy",
    "social democracy", "democratic socialist", "left populism",
    "workers rights", "nationalization", "left-wing government",
]

# ─── RSS sources ──────────────────────────────────────────────────────────────

# (RSS_FEEDS list removed — sources are now defined directly in collect_articles)

# Google News RSS — searches specific topics in Hebrew (very up-to-date)
GOOGLE_NEWS_QUERIES = [
    "עזה מלחמה",
    "גדה המערבית מתנחלים",
    "אלימות מתנחלים פלסטינים",
    "פלסטינים ישראל",
    "אי שוויון חברתי ישראל",
    "רווחה ביטוח לאומי ישראל",
    "כלכלה ישראל בנק ישראל",
    "יוקר המחיה ישראל",
    "חינוך ישראל",
    "אקלים ישראל",
    "שלום ישראל פלסטין",
    "פשיעה אלימות חברה ערבית ישראל",
    "שביתה איגוד עובדים ישראל",
    "Gaza ceasefire",
    "West Bank settler violence",
    "workers strike labor union",
    "socialist policy left-wing",
    "Bernie Sanders AOC progressive",
    "Labour Podemos Syriza left-wing",
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
    # No date available — exclude to avoid stale articles from previous days
    return False


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
    """Fetch Google News RSS using browser headers to avoid blocks."""
    encoded = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=iw&gl=IL&ceid=IL:iw&num=15"
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        resp = requests.get(url, headers=headers, timeout=12)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        for entry in feed.entries:
            if not is_recent(entry):
                continue
            title = entry.get("title", "").strip()
            link  = entry.get("link", "")
            if title:
                articles.append({
                    "source":  "Google News",
                    "title":   title,
                    "summary": "",
                    "link":    link,
                })
    except Exception as e:
        print(f"  ⚠️  Google News error [{query}]: {e}")
    return articles


def fetch_rss_with_headers(name: str, url: str) -> list[dict]:
    """Fetch RSS using browser headers (bypasses some paywalls/blocks)."""
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8",
        }
        resp = requests.get(url, headers=headers, timeout=12)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
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


def _cookies_for(url: str) -> dict:
    """Return stored cookies for the domain of this URL, if any."""
    for domain, cookies in COOKIES_BY_DOMAIN.items():
        if domain in url:
            return cookies
    return {}


def scrape_homepage(name: str, url: str, article_substr: str = None,
                    min_len: int = 18, no_filter: bool = False) -> list[dict]:
    """
    Scrape a news page by scanning all <a> links.
    no_filter=True: include all titles without keyword filtering (for topic-specific sections).
    """
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8",
        }
        resp = requests.get(url, headers=headers, cookies=_cookies_for(url), timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Remove noise elements
        for tag in soup(["nav", "footer", "script", "style", "header", "aside"]):
            tag.decompose()

        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            title = a.get_text(separator=" ", strip=True)
            href  = a["href"]

            # Skip too short or too long texts
            if len(title) < min_len or len(title) > 200:
                continue
            # If a URL pattern is required, enforce it
            if article_substr and article_substr not in href:
                continue

            title_key = title[:55].lower()
            if title_key in seen:
                continue
            seen.add(title_key)

            if no_filter or is_relevant(title):
                full_url = href if href.startswith("http") else url.rstrip("/") + "/" + href.lstrip("/")
                articles.append({
                    "source":  name,
                    "title":   title,
                    "summary": "",
                    "link":    full_url,
                })
    except Exception as e:
        print(f"  ⚠️  Scrape error [{name}]: {e}")
    return articles


# ─── Read manual inputs from Slack #news-inputs ──────────────────────────────

def read_slack_inputs() -> list[dict]:
    """
    Read messages from #news-inputs posted in the last 26 hours.
    Expects messages in the format:  URL — הערה אישית
    Returns them as articles with source='📌 נוסף ידנית'.
    """
    if not SLACK_BOT_TOKEN or not NEWS_INPUTS_CHANNEL:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=26)
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    params  = {
        "channel": NEWS_INPUTS_CHANNEL,
        "oldest":  str(cutoff.timestamp()),
        "limit":   50,
    }
    try:
        resp = requests.get(
            "https://slack.com/api/conversations.history",
            headers=headers, params=params, timeout=10
        )
        data = resp.json()
        if not data.get("ok"):
            print(f"  ⚠️  Slack inputs error: {data.get('error')}")
            return []

        inputs = []
        for msg in data.get("messages", []):
            text = msg.get("text", "").strip()
            if not text or text.startswith("📰"):   # skip the bot's own summary
                continue
            # Split on  —  or  -  to separate URL from personal note
            parts = text.replace(" — ", "\n").replace(" - ", "\n").split("\n", 1)
            link  = parts[0].strip()
            note  = parts[1].strip() if len(parts) > 1 else ""
            inputs.append({
                "source":  "📌 נוסף ידנית",
                "title":   note or link,
                "summary": "",
                "link":    link if link.startswith("http") else "",
            })
        print(f"   #news-inputs: {len(inputs)} הודעות")
        return inputs

    except Exception as e:
        print(f"  ⚠️  Slack inputs error: {e}")
        return []


# ─── Collect all articles ─────────────────────────────────────────────────────

def collect_articles() -> list[dict]:
    all_articles: list[dict] = []

    print("📡 Fetching RSS feeds (reliable sources)...")
    reliable_rss = [
        ("ynet",                 "https://www.ynet.co.il/Integration/StoryRss2.xml"),
        ("שיחה מקומית",          "https://www.mekomit.co.il/feed/"),
        ("N12",                  "https://www.n12.co.il/rss/"),
        ("Guardian Middle East", "https://www.theguardian.com/world/middleeast/rss"),
        ("NYT Middle East",      "https://rss.nytimes.com/services/xml/rss/nyt/MiddleEast.xml"),
        ("Al-Jazeera English",   "https://www.aljazeera.com/xml/rss/all.xml"),
        ("Wafa",                 "https://english.wafa.ps/rss"),
        ("Ma'an News",           "https://www.maannews.com/rss/latest-news"),
        # הארץ ודה מרקר — RSS עדיף על scraping כי האתרים JS-heavy
        ("הארץ",                 "https://www.haaretz.co.il/cmlink/1.1885897"),
        ("הארץ — חינוך",         "https://www.haaretz.co.il/cmlink/1.4318068"),
        ("הארץ — כלכלה",         "https://www.haaretz.co.il/cmlink/1.1953796"),
        ("דה מרקר",              "https://www.themarker.com/cmlink/1.4318069"),
        # וואלה RSS
        ("וואלה חדשות",          "https://rss.walla.co.il/feed/22"),
        ("וואלה כלכלה",          "https://rss.walla.co.il/feed/2"),
    ]
    for name, url in reliable_rss:
        batch = fetch_rss(name, url)
        print(f"   {name}: {len(batch)}")
        all_articles.extend(batch)
        time.sleep(0.3)

    print("📰 Scraping Israeli news pages...")
    homepage_sources = [
        # ynet — filter תוקן ל-/article/ (אמיתי)
        ("ynet — רווחה",          "https://www.ynet.co.il/topics/%D7%A8%D7%95%D7%95%D7%97%D7%94", "/article/", True),
        ("ynet — חינוך",          "https://www.ynet.co.il/topics/%D7%97%D7%99%D7%A0%D7%95%D7%9A", "/article/", True),
        ("ynet — כלכלה",          "https://www.ynet.co.il/economy",           "/article/",  True),
        # ישראל היום
        ("ישראל היום — רווחה",    "https://www.israelhayom.co.il/news/welfare",    None, True),
        ("ישראל היום — חינוך",    "https://www.israelhayom.co.il/news/education",  None, True),
        ("ישראל היום — מוניציפלי","https://www.israelhayom.co.il/news/municipal",  None, False),
        ("ישראל היום — חדשות",    "https://www.israelhayom.co.il/israelnow",       None, False),
        # כאן וגל"צ
        ("כאן חדשות",             "https://www.kan.org.il/",                  "/item/",    False),
        ("גל\"צ",                  "https://www.glz.co.il/",                   None,        False),
    ]
    for name, url, substr, nf in homepage_sources:
        batch = scrape_homepage(name, url, article_substr=substr, no_filter=nf)
        print(f"   {name}: {len(batch)}")
        all_articles.extend(batch)
        time.sleep(0.5)

    print("💰 Scraping economy pages...")
    economy_sources = [
        # גלובס — filter תוקן ל-/news/article.aspx (אמיתי)
        ("גלובס",               "https://www.globes.co.il/",   "/news/article.aspx", True),
    ]
    for name, url, substr, nf in economy_sources:
        batch = scrape_homepage(name, url, article_substr=substr, no_filter=nf)
        print(f"   {name}: {len(batch)}")
        all_articles.extend(batch)
        time.sleep(0.5)

    print("🔍 Fetching Google News topic searches...")
    for query in GOOGLE_NEWS_QUERIES:
        batch = fetch_google_news(query)
        print(f"   [{query}]: {len(batch)}")
        all_articles.extend(batch)
        time.sleep(0.3)

    print("📌 Reading manual inputs from #news-inputs...")
    manual = read_slack_inputs()
    all_articles.extend(manual)

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

    # Build article list with URLs (cap at 100 to keep costs low)
    lines = []
    for i, a in enumerate(articles[:100], 1):
        line = f"{i}. [{a['source']}] {a['title']}"
        if a["link"]:
            line += f"\n   קישור: {a['link']}"
        if a["summary"]:
            line += f"\n   {a['summary'][:200]}"
        lines.append(line)
    articles_text = "\n".join(lines)

    today = datetime.now().strftime("%d/%m/%Y")

    prompt = f"""להלן רשימת כתבות חדשות מ-24 השעות האחרונות ({today}).

אנא צור סיכום יומי מקצועי ותמציתי *בעברית* עבור ארגון "עומדים ביחד" — ארגון ישראלי-פלסטיני שעוסק בשיתוף פעולה, שלום וצדק חברתי.

*נושאי הסיכום — לפי סדר עדיפות:*

⭐⭐⭐ *עדיפות גבוהה* — כלול תמיד אם יש תוכן, 2–4 נקודות:
1. 🔴 *גדה המערבית ומתנחלים* — אלימות מתנחלים, טרור יהודי, פשעי שנאה, גירוש, הרס בתים
2. 🕊️ *שלום ישראלי-פלסטיני* — **רק** מגעים ישירים ישראל-פלסטינים: משא ומתן, הסכמים, שחרור חטופים, הפסקות אש
3. 💣 *המלחמה בעזה* — התפתחויות מרכזיות, נפגעים אזרחיים, מצב הומניטרי
4. ⚖️ *חברה, רווחה וחינוך* — תקציב, קיצוצים, שכר, דיור, ביטוח לאומי, חינוך, פריפריה, גזענות
5. 💰 *כלכלה* — בנק ישראל, אינפלציה, תעסוקה, מאבקי עובדים, הסתדרות, הפרטה, יוקר המחיה
6. 🔫 *אלימות ופשיעה בחברה הערבית* — ירי, רצח, כנופיות, פשע מאורגן, מבצעי משטרה

⭐⭐ *עדיפות בינונית* — כלול רק אם יש כתבה משמעותית (לא סתם אזכור):
7. 🌍 *אקלים וסביבה* — התחממות גלובלית, זיהום, אסונות טבע, מדיניות אקלים
8. 🌐 *אזורי ובינלאומי* — ישראל-איראן, ישראל-לבנון, לחצים דיפלומטיים; כולל מדיניות סוציאלית/שמאלית דרמטית בעולם
9. 🗞️ *קול פלסטיני* — מה מדווחים Wafa, Al-Jazeera, Ma'an

⭐ *עדיפות נמוכה* — כלול רק אם יש חדשה בולטת וחשובה במיוחד:
10. 🌹 *שמאל בעולם* — ברני סנדרס, AOC, ממדני, קורבין, מלנשון, מפלגות שמאל בספרד/בריטניה/צרפת/גרמניה/יוון; מדיניות פרוגרסיבית דרמטית

*כללים מחייבים — קרא לפני הכל:*
1. **סקציה ללא כתבות רלוונטיות ברשימה — אל תכתוב אותה בכלל.** אסור לכתוב "לא דווח", "אין חדשות", או כל ניסוח דומה. פשוט דלג לסקציה הבאה.
2. כל נקודה: משפט אחד עד שניים, 2–4 נקודות לסקציה.
3. אל תמציא מידע שאינו מופיע ברשימה.
4. שפה: עברית תקנית ופשוטה.
5. *חובה:* בסוף כל נקודה הוסף קישור למקור בפורמט Slack: `<URL|שם_מקור>`
   לדוגמה: `• ישראל הודיעה על הפסקת אש. <https://www.ynet.co.il/article/123|ynet>`
6. כתבות עם מקור "📌 נוסף ידנית" — כלול אותן תחת הקטגוריה המתאימה וסמן כ-📌

*פורמט הפלט (כלול רק סקציות שיש להן תוכן):*

📰 *סיכום חדשות יומי | {today}*

*🕊️ שלום ישראלי-פלסטיני*
• [רק אם יש מגעים ישירים ישראל-פלסטינים]

*🔴 גדה המערבית ומתנחלים*
• [רק אם יש אירועים]

*💣 המלחמה בעזה*
• [רק אם יש התפתחויות]

*⚖️ חברה, רווחה וחינוך*
• [רק אם יש כתבות]

*💰 כלכלה*
• [רק אם יש כתבות]

*🔫 אלימות ופשיעה בחברה הערבית*
• [רק אם יש כתבות]

*🌍 אקלים וסביבה*
• [רק אם יש כתבות]

*🗞️ קול פלסטיני*
• [רק אם יש כתבות]

*🌹 שמאל בעולם*
• [רק אם יש כתבות]

*🌐 אזורי ובינלאומי*
• [רק אם יש כתבות]

_מקורות: ynet, הארץ, וואלה, N12, שיחה מקומית, גלובס, כלכליסט, דה מרקר, Guardian, NYT, Al-Jazeera, Wafa ועוד_

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

def split_by_lines(text: str, max_len: int = 2900) -> list[str]:
    """Split text at line boundaries — never cuts mid-sentence."""
    if len(text) <= max_len:
        return [text]
    chunks, current = [], ""
    for line in text.split("\n"):
        candidate = (current + "\n" + line) if current else line
        if len(candidate) > max_len:
            if current:
                chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def send_to_slack(text: str):
    """Post the summary to the Slack channel via Incoming Webhook."""
    chunks = split_by_lines(text)

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

    # Filter out articles already sent in the last 48 hours
    seen = load_seen_articles()
    before = len(articles)
    articles = [a for a in articles if _article_key(a) not in seen]
    print(f"🔁 Dedup: {before} → {len(articles)} articles (filtered {before - len(articles)} already seen)")

    if not articles:
        print("[INFO] No new articles — skipping Slack message.")
        return

    print("\n🤖 Summarising with Claude...")
    summary = summarise(articles)

    print("\n📤 Posting to Slack...")
    send_to_slack(summary)

    save_seen_articles(articles)
    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
