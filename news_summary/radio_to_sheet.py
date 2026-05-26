#!/usr/bin/env python3
"""
Standing Together — Hourly Radio Transcription → Google Sheet
=============================================================
Runs every hour (triggered by GitHub Actions at :20 past each target hour).
Transcribes the latest bulletin from Galatz + Kan Reshet Bet and appends
every individual news item as a row in the "תמלולי רדיו" Google Sheet tab.

Required env vars:
  GROQ_API_KEY            — Groq API key (Whisper transcription)
  ANTHROPIC_API_KEY       — Claude Haiku (post-processing; optional but recommended)
  GOOGLE_CREDENTIALS_JSON — Service-account JSON (for Sheets)
  RADIO_SHEET_ID          — Spreadsheet ID

Target hours (IDT = UTC+3): 6, 7, 8, 9, 10, 12, 13, 17, 18
Both stations get all 9 hours.
~18 bulletins/day × ~4 min ≈ 72 min/day = well within Groq free tier.
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import feedparser
import requests

# ─── Configuration ────────────────────────────────────────────────────────────

# Whisper prompt — seeds domain vocabulary so the model spells Hebrew terms correctly.
# Keep under 224 tokens (~500 chars). Uses actual broadcast phrases so Whisper
# "hears" them first and prefers these spellings.
# Groq Whisper limit: 896 characters. Prioritise terms that cause the worst
# phonetic errors; Claude post-processing handles the rest.
_WHISPER_PROMPT = (
    'גלי צה"ל מירושלים שלום רב, כאן חדשות, כאן רשת ב\', קול ישראל מירושלים שלום רב. '
    'הרמטכ"ל, מצה"ל נמסר, כוחותינו, דובר צה"ל, אלוף במילואים, כטב"ם. '
    'כלי טייס עויין, כלי טייס בלתי מאויש, מרחפן נפץ, אזעקות, טיל נ"מ, התפוצץ. '
    'נתניהו, גלנט, בן גביר, סמוטריץ\', זמיר, הרצוג, עמידרור, אזולאי. '
    'טהראן, מטולה, ג\'נין, רפיח, דרום לבנון, חיזבאללה, בבאר שבע. '
    'משא ומתן, תוכנית הגרעין, לפני כשעה, שלשום, אמש, בתוך כך, כלשונו, המוצע. '
    'נפתלי מנשה, דורון קדוש, רן יבנאי, כתבתנו, ולסיום, כאן תחזית. '
    'מד"א, שב"כ, מזג האוויר, מחדר החדשות, בית המשפט העליון, ביישומון. '
)

GALATZ_RSS = (
    "https://www.omnycontent.com/d/playlist/"
    "6dcbc33f-1fb6-49de-9ae2-ad8a00c01523/"
    "642b5ea6-ce25-4da0-94b8-ade800c22a62/"
    "e9ba2fce-8956-400c-b84e-ade800c27a87/podcast.rss"
)
KAN_RSS = "https://www.spreaker.com/show/6095076/episodes/feed"

# Per-station target hours (IDT = UTC+3)
# כאן רשת ב — no evening bulletin at 19/20/21 worth transcribing
_ALL_HOURS: frozenset[int] = frozenset({6, 7, 8, 9, 10, 12, 13, 17, 18})

GALATZ_HOURS: frozenset[int] = _ALL_HOURS
KAN_HOURS:    frozenset[int] = _ALL_HOURS

# Union — used to decide whether to run at all this hour
TARGET_HOURS_IDT: frozenset[int] = _ALL_HOURS

_SHEET_TAB     = "תמלולי רדיו"
_SHEET_HEADERS = ["תאריך", "שעה", "תחנה", "כותרת", "תמלול", "קובץ אודיו"]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _split_bulletin(segments, full_text: str, gap_seconds: float = 2.0) -> list[str]:
    """Split Whisper verbose_json segments into individual news items via gaps."""
    if not segments:
        return [full_text]
    items: list[str] = []
    current: list[str] = []
    for i, seg in enumerate(segments):
        text = (seg.text if hasattr(seg, "text") else seg.get("text", "")).strip()
        end  =  seg.end  if hasattr(seg, "end")  else seg.get("end",  0)
        if text:
            current.append(text)
        if i + 1 < len(segments):
            nxt        = segments[i + 1]
            next_start = nxt.start if hasattr(nxt, "start") else nxt.get("start", 0)
            if next_start - end >= gap_seconds:
                candidate = " ".join(current).strip()
                if len(candidate) >= 20:
                    items.append(candidate)
                current = []
    if current:
        candidate = " ".join(current).strip()
        if len(candidate) >= 20:
            items.append(candidate)
    return items or [full_text]


def _process_story(text: str) -> tuple[str, str]:
    """
    Single Claude Haiku call per story item:
      1. Fix phonetic transcription errors (words Whisper mishears).
      2. Generate a concise 5-8 word Hebrew headline.

    Returns (headline, cleaned_text).
    Falls back to (first_sentence, original_text) if API unavailable or fails.
    """
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        return text.split(".")[0].strip()[:120], text

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)

        system = (
            "אתה עורך תמלולים של שידורי רדיו בעברית. לכל ידיעה שתקבל, בצע שתי משימות:\n"
            "1. תקן שגיאות פונטיות בלבד — מילים שנשמעות דומה אך שגויות בהקשר. "
            "אל תוסיף מילים, אל תחסיר, ואל תשנה תוכן, סגנון או מבנה.\n"
            "2. צור כותרת עיתונאית קצרה של 5-8 מילים בעברית.\n"
            "   טיפ לכותרת: אם הטקסט מתחיל ב'ולסיום' — זו הידיעה האחרונה בתוכנית. "
            "   אם הטקסט מתחיל ב'כאן תחזית' — מדובר בתחזית מזג האוויר.\n\n"
            "פורמט תגובה מחייב (בדיוק כך):\n"
            "כותרת: [כותרת]\n"
            "---\n"
            "[טקסט מתוקן]"
        )

        error_examples = (
            "שגיאות פונטיות נפוצות לתיקון:\n"
            "כל ישראל מירושלים → קול ישראל מירושלים | שלום רעב → שלום רב | רמש → אמש\n"
            "חטבתנו / לחטבתנו → כתבתנו / לכתבתנו | דובך → דווח\n"
            "רב אלופי אל זמיר → רב אלוף אייל זמיר | פייט פרטי → בית פרטי\n"
            "במילוי → במילואים | הזולי → אזולאי | צמלת שין → סמלת ש'\n"
            "הרגשה חמוסה → הרגשה חמוצה | מין הרגשה → מן הרגשה\n"
            "בנטולה → במטולה | התפרוצץ → התפוצץ | ממידרור → עמידרור\n"
            "מורשר → מועשר | ברשת בית → ברשת ב' | בתוכך → בתוך כך\n"
            "ביחי שעתיים → בכשעתיים | תארן → טהראן | מזגה אוויר → מזג האוויר\n"
            "אורך החדשות → עורך החדשות | הרמטכאל → הרמטכ\"ל | מצהל → מצה\"ל\n"
            "לפני קשה → לפני כשעה | עזקות → אזעקות | גיש → הגיש\n"
            "קליטה הסבילתי מאויה → כלי טייס בלתי מאויש\n"
            "פייננצ'ל טיימס → פייננשל טיימס\n"
            "נפשלנו / נפשלו → נכשלנו / נכשלו\n"
            "מקטב התביעה → מכתב התביעה\n"
            "יבר שבע → בבאר שבע\n"
            "עקף התמיכה → היקף התמיכה\n"
            "בוויכרות → בבדיקות\n"
            "וחספיהם → וכספיהם\n"
            "מעברכת נכסים → מהברחת נכסים\n"
            "עריכות הימים → אריכות הימים\n"
            "הרפואישי בה / הרפואי שיבה → הרפואי שיבא\n"
            "עשרים לאפשר → עשויים לאפשר\n"
            "בקן 11 → בכאן 11\n"
            "הצלמה → המצלמה\n"
            "מחדה החדשות → מחדר החדשות\n"
            "ביישומות → ביישומון\n"
            "נפתלים מנשא → נפתלי מנשה\n"
            "נטמן / נתמן → נטמן (בית עלמין) | בעילת → באילת\n"
            "כתובנו → כתבנו | בקואליציה נרקמת → בקואליציה נרקמת (שמור)\n"
            "משבק → משב\"כ | שבק → שב\"כ | מצהל → מצה\"ל\n"
            "קובלים → כובלים | ובחמה זורים → ובכמה אזורים\n"
            "כטב\"ם → כטב\"ם (שמור — כלי טייס בלתי מאויש)\n"
            "אחבות → הרחבות | בינימיני וגואטה → בינימיני וגואטה (שמור)\n"
            "המסע ומתן / מסע ומתן → משא ומתן\n"
            "תוכנית הגרים → תוכנית הגרעין\n"
            "קורמי המאורים → גורמים המעורים\n"
            "טרוס → truth (רשת חברתית)\n"
            "מתרחת → מתארחת\n"
            "מקביר צליה / מכבי צליה → מכבי הרצליה\n"
            "ליגה תעל / ליגת תעל → ליגת העל\n"
            "בחדורגל → בכדורגל\n"
            "הקבי → הכבאי\n"
            "לחבות / לחבות → לכבות\n"
            "שאר דולר / שאר הדולר → שער דולר / שער הדולר\n"
            "מדה → מד\"א\n"
            "תחדות → התאחדות\n"
            "מזג העביר → מזג האויר\n"
            "נחיל ירי → נכיל ירי\n"
        )

        user_msg = f"{error_examples}\n\nידיעה לעיבוד:\n{text}"

        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=min(len(text) * 2 + 400, 4096),
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )

        raw = resp.content[0].text.strip()

        # Parse structured response: "כותרת: ...\n---\n[text]"
        if "כותרת:" in raw and "---" in raw:
            header_part, body_part = raw.split("---", 1)
            headline = header_part.replace("כותרת:", "").strip()
            cleaned  = body_part.strip()
            if headline and cleaned:
                return headline, cleaned

        # Fallback parsing if format is off
        lines = [l for l in raw.split("\n") if l.strip()]
        headline = lines[0].replace("כותרת:", "").strip() if lines else ""
        cleaned  = "\n".join(lines[1:]).strip() if len(lines) > 1 else text
        return headline or text.split(".")[0].strip()[:120], cleaned or text

    except Exception as e:
        print(f"   ⚠️  Claude process failed: {e}")
        return text.split(".")[0].strip()[:120], text


def _split_kan_stories(full_text: str) -> list[str]:
    """
    Use Claude Haiku to split a Kan Reshet Bet bulletin into individual story items.

    Expected bulletin structure:
      1. "כאן חדשות ברשת ב' הכותרות: ..." → headlines block  (item 0 / ידיעה 1)
      2. "קול ישראל מירושלים ..."           → expanded stories begin
      3. Individual stories, one per topic
      4. "כאן תחזית ..."                    → weather/promo segment
      5. "עד כאן מחדר החדשות ..."           → closing — appended to last item, NOT separate

    Returns a list of raw (uncleaned) text items.
    Falls back to [full_text] on API failure.
    """
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        return [full_text]

    try:
        import anthropic, json as _json, re as _re
        client = anthropic.Anthropic(api_key=anthropic_key)

        system = (
            "קיבלת תמלול גולמי של שידור חדשות של כאן רשת ב'. "
            "עליך לחלק את הטקסט לפריטים נפרדים.\n\n"
            "מבנה הבולטין:\n"
            "• פריט 1 — בלוק הכותרות: מתחיל ב'כאן חדשות' ומסתיים לפני 'קול ישראל מירושלים' "
            "(בתמלול גולמי מופיע לרוב כ'כל ישראל מירושלים' — זו שגיאת תמלול נפוצה).\n"
            "• פריטים 2+ — ידיעות מורחבות: הטקסט שאחרי 'קול ישראל' / 'כל ישראל מירושלים', "
            "כל ידיעה עוסקת בנושא אחד.\n"
            "• פריט תחזית — מתחיל ב'כאן תחזית', תמיד הפריט האחרון.\n\n"
            "כיצד לזהות גבולות בין ידיעות:\n"
            "• ידיעה חדשה מתחילה כאשר הנושא משתנה לחלוטין — "
            "אדם אחר, אירוע אחר, מקום אחר.\n"
            "• סימנים אופייניים לפתיחת ידיעה חדשה: שם של אדם חדש כנושא, "
            "ביטויים כמו 'השר X', 'יו\"ר Y', 'השיגורים מ...', 'בית המשפט', וכד'.\n"
            "• ציטוט, ספד, ריאיון שמשכו עוד ידיעות — כולם שייכים לאותה ידיעה.\n"
            "• אמירות כמו 'כך אמר לתוכניתנו', 'כך ציין' — הן המשך הידיעה הנוכחית.\n\n"
            "כללים חשובים:\n"
            "• 'עד כאן מחדר החדשות...' שייך לפריט האחרון — אל תפריד אותו.\n"
            "• שמור את הטקסט המקורי בדיוק — אל תשנה, אל תוסיף, אל תגרע.\n"
            "• כל פריט חייב להיות לפחות 30 תווים.\n"
            "• החזר JSON בלבד, ללא כל טקסט נוסף:\n"
            "{\"items\": [\"פריט 1\", \"פריט 2\", ...]}"
        )

        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=min(len(full_text) * 2 + 1000, 6000),
            system=system,
            messages=[{"role": "user", "content": full_text}],
        )

        raw_resp = resp.content[0].text.strip()
        # Extract JSON even if wrapped in markdown code block
        json_match = _re.search(r'\{.*?"items".*?\}', raw_resp, _re.DOTALL)
        data = _json.loads(json_match.group() if json_match else raw_resp)
        items = [i.strip() for i in data.get("items", []) if i.strip() and len(i.strip()) >= 30]
        if items:
            print(f"   📰  Kan: split into {len(items)} items")
            return items

    except Exception as e:
        print(f"   ⚠️  Kan story splitting failed: {e} — using full text as single item")

    return [full_text]


def _extract_bulletin_title(full_text: str, source_name: str) -> str:
    """
    Extract a station-specific title from the full Whisper transcript.

    כאן רשת ב': everything between 'הכותרות' and 'קול ישראל מירושלים'
                 (the full headlines block — goes verbatim into column D)

    גל"צ:        the first news story — after 'עם מה שקורה עכשיו'
                 and before 'ידיעה שמסר / ידיעה שנמסרה [reporter name]'

    Falls back to the first sentence if no pattern matches.
    """
    import re

    text = full_text.strip()

    if "כאן" in source_name or "רשת ב" in source_name:
        # Capture from "כאן חדשות ברשת ב' הכותרות:" through the end of the headlines block.
        # The full opening line is preserved so ידיעה 1 title = text = the complete block.
        # "קול ישראל" is often transcribed as "כל ישראל" — match both
        m = re.search(
            r'(כאן חדשות.+?הכותרות[:\s]*.+?)(?=(?:קול|כל) ישראל|$)',
            text, re.DOTALL,
        )
        if m:
            return m.group(1).strip()
        # Fallback: just everything after "הכותרות"
        m = re.search(r'הכותרות[:\s]*(.+?)(?=(?:קול|כל) ישראל|$)', text, re.DOTALL)
        if m:
            return m.group(1).strip()

    else:  # גל"צ and any other station
        m = re.search(
            r'עם מה שקורה עכשיו[,.]?\s*(.+?)(?=\s*ידיעה ש(?:מסר|נמסרה)|$)',
            text, re.DOTALL,
        )
        if m:
            return m.group(1).strip()

    # Fallback: first sentence
    return text.split(".")[0].strip()


def transcribe_bulletin(rss_url: str, source_name: str, target_hour_idt: int) -> list[dict]:
    """
    Find the bulletin published closest to target_hour_idt (IDT) in the RSS feed,
    download it, transcribe with Groq Whisper, and split into individual items.
    Accepts publication times within ±90 min of the target hour.
    """
    from groq import Groq
    groq_client = Groq(api_key=os.environ["GROQ_API_KEY"])

    # Build a UTC window around the target hour
    now_utc = datetime.now(timezone.utc)
    target_utc = now_utc.replace(
        hour=(target_hour_idt - 3) % 24, minute=0, second=0, microsecond=0
    )
    if target_utc > now_utc:           # target is "today" but we haven't reached it
        target_utc -= timedelta(days=1)
    window_start = target_utc - timedelta(minutes=30)
    window_end   = target_utc + timedelta(minutes=90)

    feed = feedparser.parse(rss_url)
    for entry in feed.entries:
        if not getattr(entry, "published_parsed", None):
            continue
        pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        if not (window_start <= pub <= window_end):
            continue

        # Found the episode — get audio URL
        audio_url = None
        for enc in getattr(entry, "enclosures", []):
            if enc.get("type", "").startswith("audio") or enc.get("url", "").endswith(".mp3"):
                audio_url = enc["url"]
                break
        if not audio_url:
            continue

        print(f"   ⬇️  Downloading {source_name} {target_hour_idt:02d}:00 bulletin...")
        r = requests.get(audio_url, timeout=60, allow_redirects=True)
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
            f.write(r.content)
            tmp = f.name

        print(f"   🎙  Transcribing {len(r.content) // 1024} KB with Whisper...")
        with open(tmp, "rb") as audio_file:
            result = groq_client.audio.transcriptions.create(
                model="whisper-large-v3",
                file=audio_file,
                language="he",
                response_format="verbose_json",
                timestamp_granularities=["segment"],
                prompt=_WHISPER_PROMPT,
            )
        os.unlink(tmp)

        full_raw_text = getattr(result, "text", "")
        hour_str = f"{target_hour_idt:02d}:00"

        if "כאן" in source_name or "רשת ב" in source_name:
            # ── כאן רשת ב' ────────────────────────────────────────────────────
            # 1. Strip pre-news ads: everything before "כאן חדשות"
            kan_idx = full_raw_text.find("כאן חדשות")
            if kan_idx > 0:
                print(f"   ✂️  Stripped {kan_idx} chars of pre-news content")
                full_raw_text = full_raw_text[kan_idx:]

            # 2. Claude splits the whole bulletin into story items
            all_raw_items = _split_kan_stories(full_raw_text)
            raw_title_block  = all_raw_items[0] if all_raw_items else full_raw_text
            story_raw_items  = all_raw_items[1:] if len(all_raw_items) > 1 else []
        else:
            # ── גל"צ (and any future station) ────────────────────────────────
            # Silence-gap splitting; title block extracted via regex
            raw_title_block = _extract_bulletin_title(full_raw_text, source_name)
            story_raw_items = _split_bulletin(
                getattr(result, "segments", []),
                full_raw_text,
                gap_seconds=1.0,   # Galatz plays ~1 s of music between stories
            )

        # Claude Haiku: fix phonetic errors + generate headline for each story item
        print(f"   ✏️  Processing {len(story_raw_items)} story item(s) with Claude Haiku...")
        processed = [_process_story(item) for item in story_raw_items]

        # Clean the title block for ידיעה 1 (title = text = full block, no short headline)
        _, cleaned_title = _process_story(raw_title_block) if raw_title_block else ("", "")

        total = 1 + len(processed)
        print(f"   ✅ {source_name} {hour_str}: {total} news items")

        articles = []

        # ידיעה 1: כותרת ותמלול זהים (headlines block for Kan / first story for Galatz)
        block = cleaned_title or (processed[0][1] if processed else "")
        articles.append({
            "source":         source_name,
            "title":          block,
            "url":            audio_url,
            "text":           block,
            "published":      pub.isoformat(),
            "_bulletin_hour": hour_str,
            "_item_index":    1,
        })

        # ידיעות 2+: short generated headline in D, full cleaned text in E
        for idx, (headline, cleaned_text) in enumerate(processed, 2):
            articles.append({
                "source":         source_name,
                "title":          headline,
                "url":            audio_url,
                "text":           cleaned_text,
                "published":      pub.isoformat(),
                "_bulletin_hour": hour_str,
                "_item_index":    idx,
            })

        return articles  # one episode per hour per station

    print(f"   ⚠️  No bulletin found for {source_name} at {target_hour_idt:02d}:00 IDT")
    return []


# ─── Google Sheet writer ──────────────────────────────────────────────────────

def write_to_sheet(articles: list[dict]) -> None:
    if not articles:
        return
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    sheet_id   = os.environ.get("RADIO_SHEET_ID", "").strip()
    if not creds_json or not sheet_id:
        print("⚠️  GOOGLE_CREDENTIALS_JSON or RADIO_SHEET_ID not set — skipping")
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        client = gspread.authorize(creds)
        sh = client.open_by_key(sheet_id)

        try:
            ws = sh.worksheet(_SHEET_TAB)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=_SHEET_TAB, rows=5000, cols=6)
            ws.append_row(_SHEET_HEADERS, value_input_option="RAW")
            sh.batch_update({"requests": [{
                "updateSheetProperties": {
                    "properties": {"sheetId": ws.id, "rightToLeft": True},
                    "fields": "rightToLeft",
                }
            }]})
            print(f"📋 Created tab '{_SHEET_TAB}'")

        rows = []
        for a in articles:
            try:
                dt     = datetime.fromisoformat(a["published"].replace("Z", "+00:00"))
                date_s = (dt + timedelta(hours=3)).strftime("%d/%m/%Y")
            except Exception:
                date_s = datetime.now().strftime("%d/%m/%Y")
            rows.append([
                date_s,
                a.get("_bulletin_hour", ""),
                a.get("source", ""),
                a.get("title", ""),
                a.get("text", ""),
                a.get("url", ""),
            ])

        if rows:
            ws.insert_rows(rows, row=2, value_input_option="USER_ENTERED")
            print(f"📋 Wrote {len(rows)} row(s) to Google Sheet")

    except Exception as e:
        print(f"⚠️  Sheet write failed: {e}")
        raise


# ─── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    now_idt      = datetime.now(timezone.utc) + timedelta(hours=3)
    current_hour = now_idt.hour

    if current_hour not in TARGET_HOURS_IDT:
        print(f"ℹ️  IDT hour {current_hour:02d} is not a target hour — nothing to do.")
        return

    print(f"📻 Transcribing {current_hour:02d}:00 IDT bulletins "
          f"({now_idt.strftime('%d/%m/%Y %H:%M')} IDT)...")

    all_articles: list[dict] = []
    for rss_url, source_name, station_hours in [
        (GALATZ_RSS, 'גל"צ',    GALATZ_HOURS),
        (KAN_RSS,    "רשת ב",  KAN_HOURS),
    ]:
        if current_hour not in station_hours:
            print(f"   ⏭️  {source_name} — skipping hour {current_hour:02d}")
            continue
        try:
            articles = transcribe_bulletin(rss_url, source_name, current_hour)
            all_articles.extend(articles)
        except Exception as e:
            print(f"❌ Error transcribing {source_name}: {e}")

    write_to_sheet(all_articles)
    print(f"✅ Done — {len(all_articles)} item(s) written to sheet.")


if __name__ == "__main__":
    main()
