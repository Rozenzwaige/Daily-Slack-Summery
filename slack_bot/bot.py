#!/usr/bin/env python3
"""
איתמר — Standing Together News Bot
====================================
Slack bot that answers news queries in Hebrew via DM.
Users can ask for topic-specific news summaries for any time range.

Example queries:
  "תביא לי מה קרה עם אלימות מתנחלים ביומיים האחרונים"
  "מה הכותרות על ארגוני עובדים בשבוע האחרון?"
  "עדכון על המלחמה בעזה היום"
"""

import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import anthropic
import feedparser
import requests
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

# ─── Credentials ──────────────────────────────────────────────────────────────

SLACK_BOT_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN   = os.environ["SLACK_APP_TOKEN"]   # xapp-... (Socket Mode)
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

# ─── Initialize ───────────────────────────────────────────────────────────────

app    = AsyncApp(token=SLACK_BOT_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Thread pool for running blocking I/O without blocking the event loop
executor = ThreadPoolExecutor(max_workers=10)

# ─── RSS sources (fast — no heavy scraping) ───────────────────────────────────

RSS_SOURCES = [
    # עברית
    ("ynet",               "https://www.ynet.co.il/Integration/StoryRss2.xml"),
    ("שיחה מקומית",         "https://www.mekomit.co.il/feed/"),
    ("הארץ",               "https://news.google.com/rss/search?q=site:haaretz.co.il+when:2d&hl=he&gl=IL&ceid=IL:he"),
    ("דה מרקר",            "https://news.google.com/rss/search?q=site:themarker.com+when:2d&hl=he&gl=IL&ceid=IL:he"),
    ("גלובס — כלכלה",      "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=2"),
    ("גלובס — שוק ההון",   "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=585"),
    ("גלובס — עסקים",      "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=594"),
    ("וואלה חדשות",         "https://rss.walla.co.il/feed/22"),
    ("וואלה כלכלה",         "https://rss.walla.co.il/feed/2"),
    # בינלאומי
    ("Al-Jazeera",         "https://www.aljazeera.com/xml/rss/all.xml"),
    ("NYT Middle East",    "https://rss.nytimes.com/services/xml/rss/nyt/MiddleEast.xml"),
    ("Guardian",           "https://www.theguardian.com/world/middleeast/rss"),
    ("Le Monde",           "https://www.lemonde.fr/en/rss/une.xml"),
    ("Reuters",            "https://news.google.com/rss/search?q=site:reuters.com+(Israel+OR+Gaza+OR+Palestinian)+when:2d&hl=en-US&gl=US&ceid=US:en"),
    ("AP",                 "https://news.google.com/rss/search?q=site:apnews.com+(Israel+OR+Gaza+OR+Palestinian)+when:2d&hl=en-US&gl=US&ceid=US:en"),
    ("AFP",                "https://news.google.com/rss/search?q=AFP+(Israel+OR+Gaza+OR+Palestinian)+when:2d&hl=en-US&gl=US&ceid=US:en"),
    ("Wafa",               "https://news.google.com/rss/search?q=site:wafa.ps+when:2d&hl=en-US&gl=US&ceid=US:en"),
]

# ─── Article fetching ─────────────────────────────────────────────────────────

def _is_within(entry, hours: int) -> bool:
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                pub = datetime(*val[:6], tzinfo=timezone.utc)
                return pub >= datetime.now(timezone.utc) - timedelta(hours=hours)
            except Exception:
                pass
    return False


def _fetch_one(name_url: tuple) -> tuple:
    name, url = name_url
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/rss+xml, text/xml, */*",
        }
        resp = requests.get(url, headers=headers, timeout=12)
        feed = feedparser.parse(resp.content)
        return name, feed.entries
    except Exception as e:
        print(f"  RSS error [{name}]: {e}")
        return name, []


def collect_articles(hours: int) -> list[dict]:
    """Fetch all RSS sources in parallel, return articles within the time window."""
    all_articles = []

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_fetch_one, RSS_SOURCES))

    for name, entries in results:
        for entry in entries:
            if not _is_within(entry, hours):
                continue
            title = entry.get("title", "").strip()
            if not title or len(title) < 10:
                continue
            # Skip "- Source" placeholder titles from Google News
            if title.startswith("- ") and len(title) < 25:
                continue
            all_articles.append({
                "source":  name,
                "title":   title,
                "summary": entry.get("summary", "")[:200],
                "link":    entry.get("link", ""),
            })

    # Deduplicate by title prefix
    seen, unique = set(), []
    for a in all_articles:
        key = a["title"][:60].lower()
        if key not in seen:
            seen.add(key)
            unique.append(a)

    return unique


# ─── Intent parsing ───────────────────────────────────────────────────────────

INTENT_PROMPT = """\
המשתמש שלח הודעה לבוט חדשות של ארגון "עומדים ביחד": "{query}"

ענה ב-JSON בלבד (ללא טקסט נוסף, ללא markdown):
{{
  "is_news_query": <true אם מדובר בבקשה לחדשות / false אם זו שאלה אחרת>,
  "hours": <כמה שעות אחורה לחפש — "היום"=24, "יומיים"=48, "שבוע"=168, "חודש"=720, ברירת מחדל=48>,
  "topic_label": "<שם הנושא בעברית קצר, לכותרת — לדוגמה: אלימות מתנחלים>",
  "search_keywords": ["<מילת מפתח עברית 1>", "<מילת מפתח עברית 2>", "<מילת מפתח באנגלית>", ...]
}}

כללים:
- search_keywords: 5-10 מילות מפתח שיעזרו לסנן כתבות רלוונטיות (עברית ואנגלית)
- אם המשתמש ביקש "חדשות כלליות" או "סיכום" ללא נושא ספציפי — search_keywords יהיה רשימה ריקה []
- hours: עגל לאחד מהערכים: 6, 12, 24, 48, 72, 168, 720\
"""


def parse_intent(user_text: str) -> dict:
    resp = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=400,
        messages=[{"role": "user", "content": INTENT_PROMPT.format(query=user_text)}],
    )
    raw = resp.content[0].text.strip()
    # Strip markdown code fences if present
    if "```" in raw:
        raw = raw.split("```")[1].replace("json", "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return {
            "is_news_query": True,
            "hours": 48,
            "topic_label": "חדשות",
            "search_keywords": [],
        }


# ─── Topic filtering ──────────────────────────────────────────────────────────

def filter_by_topic(articles: list[dict], keywords: list[str]) -> list[dict]:
    if not keywords:
        return articles
    filtered = []
    for a in articles:
        text = (a["title"] + " " + a.get("summary", "")).lower()
        if any(kw.lower() in text for kw in keywords):
            filtered.append(a)
    return filtered


# ─── Summarization ────────────────────────────────────────────────────────────

def _period_label(hours: int) -> str:
    if hours <= 6:   return "6 השעות האחרונות"
    if hours <= 12:  return "12 השעות האחרונות"
    if hours <= 24:  return "24 השעות האחרונות"
    if hours <= 48:  return "יומיים האחרונים"
    if hours <= 72:  return "3 הימים האחרונים"
    if hours <= 168: return "השבוע האחרון"
    return "החודש האחרון"


SUMMARY_PROMPT = """\
המשתמש שאל: "{query}"

להלן {n} כתבות על הנושא "{topic}" מ{period}:

{articles}

צור סיכום תמציתי בעברית:
- 4-7 נקודות עיקריות, לפי סדר חשיבות
- משפט אחד עד שניים לכל נקודה
- בסוף כל נקודה, קישור למקור בפורמט Slack: <URL|שם_מקור>
  לדוגמה: • ישראל הכריזה על הפסקת אש. <https://ynet.co.il/...|ynet>
- אל תמציא מידע שאינו ברשימה
- אם יש זוויות שונות לאותו אירוע — ציין אותן

כתוב את הסיכום:\
"""

NO_RESULTS_MSG = (
    "😕 לא מצאתי כתבות על *{topic}* ב{period}.\n\n"
    "נסה:\n"
    "• טווח זמן רחב יותר (לדוגמה: *בשבוע האחרון*)\n"
    "• ניסוח אחר לנושא"
)


def summarize(articles: list[dict], topic_label: str, hours: int, user_query: str) -> str:
    period = _period_label(hours)

    if not articles:
        return NO_RESULTS_MSG.format(topic=topic_label, period=period)

    lines = []
    for i, a in enumerate(articles[:70], 1):
        line = f"{i}. [{a['source']}] {a['title']}"
        if a["link"]:
            line += f"\n   קישור: {a['link']}"
        lines.append(line)

    resp = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1200,
        messages=[{
            "role": "user",
            "content": SUMMARY_PROMPT.format(
                query=user_query,
                topic=topic_label,
                period=period,
                n=len(articles),
                articles="\n".join(lines),
            ),
        }],
    )
    return resp.content[0].text.strip()


# ─── Slack event handlers ─────────────────────────────────────────────────────

WELCOME_MSG = (
    "שלום! אני *איתמר* 👋\n\n"
    "אני בוט החדשות של *עומדים ביחד*. שלח לי שאלה ואסכם לך את הכתבות הרלוונטיות.\n\n"
    "*לדוגמה:*\n"
    "• _תביא לי עדכון על אלימות מתנחלים ביומיים האחרונים_\n"
    "• _מה קרה עם ארגוני עובדים בשבוע האחרון?_\n"
    "• _סיכום המלחמה בעזה היום_\n"
    "• _כותרות כלכלה מהשבוע_\n\n"
    "מה תרצה לדעת? 📰"
)


@app.event("app_home_opened")
async def handle_home(event, client):
    """Show welcome message when user opens the App Home tab."""
    await client.views_publish(
        user_id=event["user"],
        view={
            "type": "home",
            "blocks": [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": WELCOME_MSG},
            }],
        },
    )


@app.event("message")
async def handle_dm(message, say, client):
    """Handle DMs sent directly to the bot."""
    # Ignore bot messages and non-DM channels
    if message.get("bot_id") or message.get("subtype"):
        return
    if message.get("channel_type") != "im":
        return

    user_text = message.get("text", "").strip()
    if not user_text:
        return

    channel = message["channel"]
    loop    = asyncio.get_event_loop()

    # Post initial status message (will be updated in-place)
    status = await client.chat_postMessage(
        channel=channel,
        text="🔍 מחפש כתבות... (לוקח כ-15 שניות)",
    )
    ts = status["ts"]

    async def update(text: str):
        await client.chat_update(channel=channel, ts=ts, text=text)

    try:
        # 1. Parse intent
        intent = await loop.run_in_executor(executor, parse_intent, user_text)

        if not intent.get("is_news_query", True):
            await update(WELCOME_MSG)
            return

        hours       = int(intent.get("hours", 48))
        topic_label = intent.get("topic_label", "חדשות")
        keywords    = intent.get("search_keywords", [])
        period      = _period_label(hours)

        await update(f"📡 מאסף כתבות על *{topic_label}* מ{period}...")

        # 2. Fetch articles (parallel RSS fetching in thread pool)
        articles = await loop.run_in_executor(executor, collect_articles, hours)

        # 3. Filter by topic
        filtered = filter_by_topic(articles, keywords)

        await update(
            f"✍️ מסכם {len(filtered)} כתבות על *{topic_label}* "
            f"({len(articles)} כתבות נמצאו בסה\"כ)..."
        )

        # 4. Summarize
        summary = await loop.run_in_executor(
            executor, summarize, filtered, topic_label, hours, user_text
        )

        # 5. Final response
        header = f"📰 *{topic_label}* | {period}\n\n"
        await update(header + summary)

    except Exception as e:
        print(f"Error handling message: {e}", flush=True)
        await update("😕 אירעה שגיאה. נסה שוב בעוד רגע.")


# ─── App mention (in channels) ───────────────────────────────────────────────

@app.event("app_mention")
async def handle_mention(event, say):
    await say("שלום! שלח לי *הודעה פרטית* ואשמח לסכם חדשות בשבילך 📰")


# ─── Entry point ─────────────────────────────────────────────────────────────

async def main():
    print("🤖 איתמר bot starting...", flush=True)
    handler = AsyncSocketModeHandler(app, SLACK_APP_TOKEN)
    await handler.start_async()


if __name__ == "__main__":
    asyncio.run(main())
