#!/usr/bin/env python3
"""
test_scraping.py — בדיקת איסוף כתבות בלבד, ללא Slack וללא Claude.
הרצה:
  set HAARETZ_COOKIES=...
  set GLOBES_COOKIES=...
  python news_summary/test_scraping.py
"""

import os
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import feedparser
import requests
from bs4 import BeautifulSoup

# ─── Cookies ─────────────────────────────────────────────────────────────────

def _parse_cookies(s: str) -> dict:
    result = {}
    for part in s.split(";"):
        part = part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            result[k.strip()] = v.strip()
    return result

COOKIES_BY_DOMAIN: dict[str, dict] = {}
if os.environ.get("HAARETZ_COOKIES"):
    _htz = _parse_cookies(os.environ["HAARETZ_COOKIES"])
    COOKIES_BY_DOMAIN["haaretz.co.il"] = _htz
    COOKIES_BY_DOMAIN["themarker.com"] = _htz
    print("✅ HAARETZ_COOKIES loaded")
else:
    print("⚠️  HAARETZ_COOKIES not set")

if os.environ.get("GLOBES_COOKIES"):
    COOKIES_BY_DOMAIN["globes.co.il"] = _parse_cookies(os.environ["GLOBES_COOKIES"])
    print("✅ GLOBES_COOKIES loaded")
else:
    print("⚠️  GLOBES_COOKIES not set")

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _cookies_for(url: str) -> dict:
    for domain, cookies in COOKIES_BY_DOMAIN.items():
        if domain in url:
            return cookies
    return {}

def is_recent(entry, hours: int = 26) -> bool:
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                pub = datetime(*val[:6], tzinfo=timezone.utc)
                cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
                return pub >= cutoff
            except Exception:
                pass
    return False

def fetch_rss(name: str, url: str) -> list[dict]:
    try:
        feed = feedparser.parse(url)
        results = [e.get("title","") for e in feed.entries if is_recent(e) and e.get("title")]
        return results
    except Exception as e:
        return [f"ERROR: {e}"]

def scrape(name: str, url: str, article_substr: str = None,
           min_len: int = 18, debug: bool = False) -> list[str]:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8",
        }
        resp = requests.get(url, headers=headers, cookies=_cookies_for(url), timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["nav", "footer", "script", "style", "header", "aside"]):
            tag.decompose()

        seen, titles = set(), []
        all_hrefs = set()

        for a in soup.find_all("a", href=True):
            title = a.get_text(separator=" ", strip=True)
            href  = a["href"]
            if len(title) < min_len or len(title) > 200:
                continue
            all_hrefs.add(href[:80])
            if article_substr and article_substr not in href:
                continue
            key = title[:55].lower()
            if key in seen:
                continue
            seen.add(key)
            titles.append(title)

        if debug and not titles:
            # הצג דגימה של URLs שנמצאו — כדי להבין מה ה-pattern
            sample = sorted(all_hrefs)[:15]
            print(f"              🔍 DEBUG — URLs לדוגמה (סה\"כ {len(all_hrefs)}):")
            for h in sample:
                print(f"                 {h}")

        return titles
    except Exception as e:
        return [f"ERROR: {e}"]

# ─── Run tests ────────────────────────────────────────────────────────────────

print(f"\n{'='*60}")
print(f"  Scraping test | {datetime.now().strftime('%d/%m/%Y %H:%M')}")
print(f"{'='*60}\n")

tests = [
    # (label, type, url, substr)
    # RSS
    ("ynet RSS",              "rss",    "https://www.ynet.co.il/Integration/StoryRss2.xml",        None),
    ("שיחה מקומית RSS",       "rss",    "https://www.mekomit.co.il/feed/",                          None),
    ("N12 RSS",               "rss",    "https://www.n12.co.il/rss/",                               None),
    ("Al-Jazeera RSS",        "rss",    "https://www.aljazeera.com/xml/rss/all.xml",                None),
    ("הארץ — Google News",      "rss",    "https://news.google.com/rss/search?q=site:haaretz.co.il+when:1d&hl=he&gl=IL&ceid=IL:he", None, False),
    ("דה מרקר — Google News",   "rss",    "https://news.google.com/rss/search?q=site:themarker.com+when:1d&hl=he&gl=IL&ceid=IL:he", None, False),
    ("גלובס RSS — כלכלה",       "rss",    "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=2",   None, False),
    ("גלובס RSS — שוק ההון",    "rss",    "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=585", None, False),
    ("גלובס RSS — נדל\"ן",       "rss",    "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=607", None, False),
    ("גלובס RSS — עסקים",       "rss",    "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=594", None, False),
    ("וואלה חדשות RSS",         "rss",    "https://rss.walla.co.il/feed/22",                        None, False),
    ("וואלה כלכלה RSS",         "rss",    "https://rss.walla.co.il/feed/2",                         None, False),
    # ynet topics — filter תוקן
    ("ynet — רווחה",           "scrape", "https://www.ynet.co.il/topics/%D7%A8%D7%95%D7%95%D7%97%D7%94", "/article/", False),
    ("ynet — כלכלה",           "scrape", "https://www.ynet.co.il/economy",                          "/article/", False),
    # ישראל היום
    ("ישראל היום — רווחה",     "scrape", "https://www.israelhayom.co.il/news/welfare",              None, False),
    ("ישראל היום — חינוך",     "scrape", "https://www.israelhayom.co.il/news/education",            None, False),
    # גלובס — filter תוקן
    ("גלובס",                   "scrape", "https://www.globes.co.il/",                              "/news/article.aspx", False),
]

total = 0
problems = []

for label, kind, url, *rest in tests:
    substr = rest[0] if rest else None
    dbg    = rest[1] if len(rest) > 1 else False
    if kind == "rss":
        titles = fetch_rss(label, url)
    else:
        titles = scrape(label, url, substr, debug=dbg)

    n = len(titles)
    total += max(n, 0)
    is_error = any(str(t).startswith("ERROR") for t in titles)

    if is_error:
        status = "❌"
        problems.append(label)
    elif n == 0:
        status = "⚠️  0"
        problems.append(label)
    else:
        status = f"✅ {n}"

    print(f"  {status:12} {label}")
    if titles and not is_error:
        # הדפס 3 כותרות לדוגמה
        for t in titles[:3]:
            print(f"              • {t[:80]}")
    elif is_error:
        print(f"              {titles[0]}")
    time.sleep(0.3)

print(f"\n{'='*60}")
print(f"  סה\"כ כותרות שנמצאו: {total}")
if problems:
    print(f"  ⚠️  בעיות: {', '.join(problems)}")
else:
    print("  ✅ הכל עובד!")
print(f"{'='*60}\n")
