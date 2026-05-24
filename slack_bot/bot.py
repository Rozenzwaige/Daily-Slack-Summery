#!/usr/bin/env python3
"""
איתמר — Standing Together News Bot
====================================
Slack bot that answers news queries in Hebrew.
Works in two modes:
  • DM — any message triggers a search
  • Channel — responds only when mentioned (@איתמר), replies in a thread

Example queries:
  "תביא לי מה קרה עם אלימות מתנחלים ביומיים האחרונים"
  "מה הכותרות על ארגוני עובדים בשבוע האחרון?"
  "עדכון על המלחמה בעזה היום"
"""

import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

# Show slack-bolt connection logs
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

import anthropic
import feedparser
import requests
from aiohttp import web
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

# ─── Credentials ──────────────────────────────────────────────────────────────

SLACK_BOT_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN   = os.environ["SLACK_APP_TOKEN"]   # xapp-... (Socket Mode)
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

# ─── Initialize ───────────────────────────────────────────────────────────────

app    = AsyncApp(token=SLACK_BOT_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─── Debug middleware — log every incoming payload ────────────────────────────
@app.middleware
async def log_all(body, next):
    evt = body.get("event", {})
    print(f"🔵 PAYLOAD type={body.get('type')} event_type={evt.get('type')} channel_type={evt.get('channel_type')} user={evt.get('user')}", flush=True)
    await next()


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
    # בינלאומי — כלל עולמי
    ("Al-Jazeera",         "https://www.aljazeera.com/xml/rss/all.xml"),
    ("NYT World",          "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
    ("NYT Middle East",    "https://rss.nytimes.com/services/xml/rss/nyt/MiddleEast.xml"),
    ("Guardian World",     "https://www.theguardian.com/world/rss"),
    ("Guardian Mid-East",  "https://www.theguardian.com/world/middleeast/rss"),
    ("BBC World",          "http://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Reuters World",      "https://news.google.com/rss/search?q=site:reuters.com+when:2d&hl=en-US&gl=US&ceid=US:en"),
    ("AP World",           "https://news.google.com/rss/search?q=site:apnews.com+when:2d&hl=en-US&gl=US&ceid=US:en"),
    ("Le Monde",           "https://www.lemonde.fr/en/rss/une.xml"),
    # בינלאומי — ישראל/עזה ספציפי
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
{context_line}
ענה ב-JSON בלבד (ללא טקסט נוסף, ללא markdown):
{{
  "is_news_query": <true אם מדובר בבקשה לחדשות / false אם זו שאלה אחרת>,
  "hours": <כמה שעות אחורה לחפש — "היום"=24, "יומיים"=48, "שבוע"=168, "חודש"=720, ברירת מחדל=48>,
  "topic_label": "<שם הנושא בעברית קצר, לכותרת — לדוגמה: אלימות מתנחלים>",
  "search_keywords": ["<מילת מפתח עברית 1>", "<מילת מפתח עברית 2>", "<מילת מפתח באנגלית>", ...]
}}

כללים:
- search_keywords: 5-10 מילות מפתח שיעזרו לסנן כתבות רלוונטיות (עברית ואנגלית)
- אם ההודעה היא המשך שיחה (לדוגמה "אז בשבוע האחרון" / "נסה יותר") — שמור את אותו נושא וmילות מפתח מהשיחה הקודמת, רק עדכן את hours
- אם המשתמש ביקש "חדשות כלליות" או "סיכום" ללא נושא ספציפי — search_keywords יהיה רשימה ריקה []
- hours: עגל לאחד מהערכים: 6, 12, 24, 48, 72, 168, 720\
"""


def parse_intent(user_text: str, prev_topic: str = "", prev_keywords: list | None = None) -> dict:
    context_line = ""
    if prev_topic:
        kw_str = ", ".join(prev_keywords or [])
        context_line = f'\nהקשר שיחה קודמת: הנושא היה "{prev_topic}" עם מילות מפתח: {kw_str}.\n'

    resp = claude.messages.create(
        model="claude-haiku-4-5",
        max_tokens=400,
        messages=[{"role": "user", "content": INTENT_PROMPT.format(
            query=user_text,
            context_line=context_line,
        )}],
    )
    raw = resp.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1].replace("json", "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return {
            "is_news_query": True,
            "hours": 48,
            "topic_label": prev_topic or "חדשות",
            "search_keywords": prev_keywords or [],
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

להלן {n} כתבות שסוננו על הנושא "{topic}" מ{period}:

{articles}

הוראות:
- סכם רק כתבות שקשורות ישירות לנושא "{topic}"
- אם רוב הכתבות אינן קשורות לנושא, השב: "😕 לא מצאתי כתבות רלוונטיות על *{topic}* ב{period}. נסה טווח זמן רחב יותר או ניסוח אחר."
- 4-7 נקודות עיקריות, לפי סדר חשיבות
- משפט אחד עד שניים לכל נקודה
- בסוף כל נקודה קישור למקור: <URL|שם_מקור>
- אל תמציא מידע שאינו ברשימה

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
        model="claude-haiku-4-5",
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

# Tracks active threads: (channel, thread_ts) → {"topic": str, "keywords": list}
# Populated when the bot answers an @mention; cleared on restart.
_active_threads: dict[tuple[str, str], dict] = {}


async def _process_query(user_text: str, channel: str, thread_ts: str | None, client) -> None:
    """
    Core pipeline: intent → fetch → filter → summarize → reply.
    Passes previous thread context to Claude so follow-up messages
    ("אז בשבוע האחרון") retain the original topic and keywords.
    """
    loop = asyncio.get_event_loop()
    post_kwargs = {"thread_ts": thread_ts} if thread_ts else {}

    status = await client.chat_postMessage(
        channel=channel,
        text="🔍 מחפש כתבות... (לוקח כ-15 שניות)",
        **post_kwargs,
    )
    msg_ts = status["ts"]

    async def update(text: str):
        await client.chat_update(channel=channel, ts=msg_ts, text=text)

    try:
        # Pull previous topic/keywords from thread context (if exists)
        ctx          = _active_threads.get((channel, thread_ts), {}) if thread_ts else {}
        prev_topic   = ctx.get("topic", "")
        prev_kw      = ctx.get("keywords", [])

        intent = await loop.run_in_executor(
            executor, parse_intent, user_text, prev_topic, prev_kw
        )

        if not intent.get("is_news_query", True):
            await update(WELCOME_MSG)
            return

        hours       = int(intent.get("hours", 48))
        topic_label = intent.get("topic_label", "חדשות")
        keywords    = intent.get("search_keywords", [])
        period      = _period_label(hours)

        # Save updated context for next follow-up
        if thread_ts:
            _active_threads[(channel, thread_ts)] = {
                "topic":    topic_label,
                "keywords": keywords,
            }

        await update(f"📡 מאסף כתבות על *{topic_label}* מ{period}...")

        articles = await loop.run_in_executor(executor, collect_articles, hours)
        filtered = filter_by_topic(articles, keywords)

        await update(
            f"✍️ מסכם {len(filtered)} כתבות על *{topic_label}* "
            f"({len(articles)} כתבות נמצאו בסה\"כ)..."
        )

        summary = await loop.run_in_executor(
            executor, summarize, filtered, topic_label, hours, user_text
        )

        await update(f"📰 *{topic_label}* | {period}\n\n{summary}")

    except Exception as e:
        print(f"Error processing query: {e}", flush=True)
        await update("😕 אירעה שגיאה. נסה שוב בעוד רגע.")


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
async def handle_message(event, say, client):
    """
    Handle incoming messages:
    • DM  → always respond
    • Channel thread reply → respond only if bot is active in that thread
    """
    print(f"📨 message: channel_type={event.get('channel_type')} "
          f"bot_id={event.get('bot_id')} subtype={event.get('subtype')} "
          f"thread_ts={event.get('thread_ts')} text={event.get('text','')[:50]}",
          flush=True)

    if event.get("bot_id") or event.get("subtype"):
        return

    channel      = event["channel"]
    channel_type = event.get("channel_type")
    thread_ts    = event.get("thread_ts")   # set only when inside a thread
    msg_ts       = event.get("ts")
    user_text    = event.get("text", "").strip()

    if not user_text:
        return

    # ── DM ────────────────────────────────────────────────────────────────────
    if channel_type == "im":
        await _process_query(user_text, channel, None, client)
        return

    # ── Channel thread reply (no @mention needed) ─────────────────────────────
    # Respond if this message is a reply in a thread the bot is already in.
    if thread_ts and thread_ts != msg_ts and (channel, thread_ts) in _active_threads:  # noqa: E501
        await _process_query(user_text, channel, thread_ts, client)


# ─── App mention (in channels) ───────────────────────────────────────────────

@app.event("app_mention")
async def handle_mention(event, say, client):
    """
    Handle @איתמר mentions in channels.
    Registers the thread as active so follow-up messages (without @mention)
    are also handled by handle_message.
    """
    if event.get("bot_id") or event.get("subtype"):
        return

    import re as _re
    raw_text  = event.get("text", "")
    user_text = _re.sub(r"<@[A-Z0-9]+>", "", raw_text).strip()
    channel   = event["channel"]
    # Use thread_ts if the mention is inside an existing thread, else event["ts"]
    thread_ts = event.get("thread_ts") or event["ts"]

    # Register thread so follow-up replies are handled without @mention
    # Initialize with empty context (will be filled after first query)
    if (channel, thread_ts) not in _active_threads:
        _active_threads[(channel, thread_ts)] = {}
    print(f"🧵 Registered active thread ({channel}, {thread_ts})", flush=True)

    if not user_text:
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text="שלום! תייג אותי עם שאלה, לדוגמה: _@איתמר מה קרה עם ארגוני עובדים השבוע?_ 📰",
        )
        return

    await _process_query(user_text, channel, thread_ts, client)


# ─── Health check server (keeps Fly.io machine alive) ────────────────────────

async def start_health_server():
    """Minimal HTTP server so Fly.io doesn't stop the machine."""
    health_app = web.Application()
    health_app.router.add_get("/health", lambda r: web.Response(text="ok"))
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    print("🩺 Health server listening on :8080", flush=True)


# ─── Entry point ─────────────────────────────────────────────────────────────

async def main():
    print("🤖 איתמר bot starting...", flush=True)
    await start_health_server()
    print(f"🔑 SLACK_APP_TOKEN prefix: {SLACK_APP_TOKEN[:12]}...", flush=True)
    print(f"🔑 SLACK_BOT_TOKEN prefix: {SLACK_BOT_TOKEN[:12]}...", flush=True)
    handler = AsyncSocketModeHandler(app, SLACK_APP_TOKEN)
    print("🔌 Connecting to Slack via Socket Mode...", flush=True)
    await handler.start_async()
    print("✅ Socket Mode connected!", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
