#!/usr/bin/env python3
"""
Standing Together — Evening TV News Transcription → Google Sheet
================================================================
Runs daily (triggered by cron-job.org at 01:00 UTC = 04:00 IDT, Sun–Thu).
Fetches the previous evening's 20:00 main edition from Channel 13 (רשת 13),
downloads the first 10 minutes of audio, transcribes with Groq Whisper,
splits into news items with Claude Haiku, and writes each item as a row in
the "תמלולי טלויזיה" tab of the same Google Sheet used for radio.

Required env vars:
  GROQ_API_KEY            — Groq API key (Whisper transcription)
  ANTHROPIC_API_KEY       — Claude Haiku (optional but recommended)
  GOOGLE_CREDENTIALS_JSON — Service-account JSON (for Sheets)
  RADIO_SHEET_ID          — Spreadsheet ID (same workbook as radio, different tab)
"""

import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import requests

# ─── Configuration ────────────────────────────────────────────────────────────

CHANNEL13_URL  = "https://13tv.co.il/vod/news/the-main-edition/"
CHANNEL13_BASE = "https://13tv.co.il"
_KALTURA_PID   = "2346261"     # Kaltura partner ID for reshet.tv
_AUDIO_SECONDS = 600           # first 10 minutes
_STATION_NAME  = "רשת 13"
_SHEET_TAB     = "תמלולי טלויזיה"
_SHEET_HEADERS = ["תאריך", "שעה", "תחנה", "כותרת", "תמלול", "קובץ אודיו", "זמן התחלה"]

# Whisper prompt — same vocab as radio, 862 UTF-8 bytes (Groq limit: 896)
_WHISPER_PROMPT = (
    'גלי צה"ל, כאן חדשות, כאן רשת ב\', קול ישראל מירושלים שלום רב. '
    'הרמטכ"ל, מצה"ל נמסר, כוחותינו, דובר צה"ל, אלוף במילואים, כטב"ם. '
    'כלי טייס עויין, כלי טייס בלתי מאויש, מרחפן נפץ, אזעקות, טיל נ"מ. '
    'נתניהו, גלנט, בן גביר, סמוטריץ\', זמיר, הרצוג, עמידרור, אזולאי. '
    'טהראן, מטולה, ג\'נין, רפיח, דרום לבנון, חיזבאללה, באר שבע. '
    'משא ומתן, תוכנית הגרעין, לפני כשעה, שלשום, אמש, בתוך כך, כלשונו. '
    'נפתלי מנשה, דורון קדוש, רן יבנאי, כתבתנו, ולסיום, כאן תחזית. '
    'מד"א, שב"כ, מזג האוויר, מחדר החדשות, בית המשפט העליון, ביישומון. '
)

# ─── Episode fetching ─────────────────────────────────────────────────────────

def fetch_latest_ch13_episode() -> dict | None:
    """
    Fetch the latest Channel 13 main-edition episode from the VOD page.

    Returns dict:
      kalturaId   — Kaltura entry ID
      episode_url — direct link to the episode page on 13tv.co.il
      title       — episode title
      publish_dt  — datetime (UTC) when the episode was published
    Returns None if no suitable episode is found.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    r = requests.get(CHANNEL13_URL, headers=headers, timeout=20)
    r.raise_for_status()

    nd_match = re.search(
        r'id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', r.text, re.DOTALL
    )
    if not nd_match:
        raise RuntimeError("Could not find __NEXT_DATA__ in Channel 13 page")

    data = json.loads(nd_match.group(1))
    try:
        posts = data["props"]["pageProps"]["page"]["Content"]["PageGrid"][1]["posts"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected page structure: {exc}") from exc

    if not posts:
        return None

    latest = posts[0]
    kaltura_id = (latest.get("video") or {}).get("kalturaId", "")
    title      = latest.get("title", "")
    pub_str    = latest.get("publishDate", "")

    if not kaltura_id:
        raise RuntimeError(f"No kalturaId in latest post: {title!r}")

    # Build episode page URL from the 'link' field
    link = latest.get("link") or latest.get("portal_link") or ""
    if link:
        episode_url = link if link.startswith("http") else f"{CHANNEL13_BASE}{link}"
    else:
        episode_url = CHANNEL13_URL   # fallback: series page

    # Parse publish datetime; the API stores times in UTC
    try:
        publish_dt = datetime.strptime(pub_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except Exception:
        publish_dt = datetime.now(timezone.utc)

    return {
        "kalturaId":   kaltura_id,
        "episode_url": episode_url,
        "title":       title,
        "publish_dt":  publish_dt,
    }


# ─── Audio download ───────────────────────────────────────────────────────────

def _build_kaltura_url(kaltura_id: str) -> str:
    return (
        f"https://cdnapisec.kaltura.com/p/{_KALTURA_PID}/sp/{_KALTURA_PID}00"
        f"/playManifest/entryId/{kaltura_id}/format/applehttp/protocol/https/a.m3u8"
    )


def _get_stream_url(kaltura_url: str) -> str:
    """
    Use yt-dlp to resolve the Kaltura manifest to a signed HLS stream URL.
    We need this because the segment URLs are time-limited and signed by Kaltura.
    """
    result = subprocess.run(
        ["yt-dlp", "--no-warnings", "-f", "worst", "--get-url", kaltura_url],
        capture_output=True, text=True, timeout=60,
    )
    url = result.stdout.strip().split("\n")[0]
    if not url or not url.startswith("http"):
        # Fallback: try without format filter
        result = subprocess.run(
            ["yt-dlp", "--no-warnings", "--get-url", kaltura_url],
            capture_output=True, text=True, timeout=60,
        )
        url = result.stdout.strip().split("\n")[0]
    if not url or not url.startswith("http"):
        raise RuntimeError(
            f"yt-dlp could not resolve stream URL.\nstderr: {result.stderr[:300]}"
        )
    return url


def _download_audio_segment(stream_url: str, duration_sec: int = _AUDIO_SECONDS) -> str:
    """
    Use ffmpeg to download the first `duration_sec` seconds as a mono 16 kHz MP3.
    Returns the path to a temporary file (caller must delete it).
    """
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
        out_path = f.name

    cmd = [
        "ffmpeg", "-y",
        "-i", stream_url,
        "-t", str(duration_sec),
        "-vn",                    # audio only — skip video stream
        "-acodec", "libmp3lame",
        "-ar", "16000",           # 16 kHz — optimal for Whisper
        "-ac", "1",               # mono
        "-q:a", "5",
        out_path,
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=300)
    if result.returncode != 0:
        err = result.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"ffmpeg failed (exit {result.returncode}): {err[:300]}")
    return out_path


# ─── Timestamp helpers (mirrors radio_to_sheet) ───────────────────────────────

def _build_time_index(segments) -> list[tuple[int, float]]:
    index: list[tuple[int, float]] = []
    pos = 0
    for seg in segments:
        text  = (seg.text  if hasattr(seg, "text")  else seg.get("text",  "")).strip()
        start =  seg.start if hasattr(seg, "start") else seg.get("start", 0)
        index.append((pos, float(start)))
        pos += len(text) + 1
    return index


def _find_start_sec(item_text: str, full_text: str,
                    time_index: list[tuple[int, float]]) -> float:
    if not time_index:
        return 0.0
    for n in (50, 25, 15):
        pos = full_text.find(item_text[:n].strip())
        if pos >= 0:
            break
    else:
        return 0.0
    start_sec = 0.0
    for char_pos, seg_start in time_index:
        if char_pos <= pos:
            start_sec = seg_start
        else:
            break
    return start_sec


def _fmt_ts(secs: float) -> str:
    m, s = divmod(int(secs), 60)
    return f"{m:02d}:{s:02d}"


# ─── Story processing (shared logic with radio_to_sheet) ─────────────────────

def _process_story(text: str) -> tuple[str, str]:
    """
    Single Claude Haiku call:
      1. Fix phonetic transcription errors.
      2. Generate a 5-8 word Hebrew headline.
    Returns (headline, cleaned_text).
    Falls back to (first_sentence, original_text) if Claude unavailable.
    """
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        return text.split(".")[0].strip()[:120], text

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)

        system = (
            "אתה עורך תמלולים של שידורי טלוויזיה בעברית. לכל ידיעה שתקבל, בצע שתי משימות:\n"
            "1. תקן שגיאות פונטיות בלבד — מילים שנשמעות דומה אך שגויות בהקשר. "
            "אל תוסיף מילים, אל תחסיר, ואל תשנה תוכן, סגנון או מבנה.\n"
            "2. צור כותרת עיתונאית קצרה של 5-8 מילים בעברית.\n\n"
            "פורמט תגובה מחייב (בדיוק כך):\n"
            "כותרת: [כותרת]\n"
            "---\n"
            "[טקסט מתוקן]"
        )
        error_examples = (
            "שגיאות פונטיות נפוצות לתיקון:\n"
            "כל ישראל → קול ישראל | הרמטכאל → הרמטכ\"ל | מצהל → מצה\"ל\n"
            "נתניהי / ניתניהו → נתניהו | עזקות → אזעקות | כטבם → כטב\"ם\n"
            "דובך → דווח | חטבתנו → כתבתנו | בתוכך → בתוך כך\n"
            "מזג העביר → מזג האוויר | המסע ומתן → משא ומתן\n"
            "תוכנית הגרים → תוכנית הגרעין | ביישומות → ביישומון\n"
            "מד\"א → מד\"א (שמור) | שב\"כ → שב\"כ (שמור)\n"
        )

        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=min(len(text) * 2 + 400, 4096),
            system=system,
            messages=[{"role": "user", "content": f"{error_examples}\n\nידיעה לעיבוד:\n{text}"}],
        )

        raw = resp.content[0].text.strip()
        if "כותרת:" in raw and "---" in raw:
            header_part, body_part = raw.split("---", 1)
            headline = header_part.replace("כותרת:", "").strip()
            cleaned  = body_part.strip()
            if headline and cleaned:
                return headline, cleaned

        lines = [ln for ln in raw.split("\n") if ln.strip()]
        headline = lines[0].replace("כותרת:", "").strip() if lines else ""
        cleaned  = "\n".join(lines[1:]).strip() if len(lines) > 1 else text
        return headline or text.split(".")[0].strip()[:120], cleaned or text

    except Exception as exc:
        print(f"   ⚠️  Claude _process_story failed: {exc}")
        return text.split(".")[0].strip()[:120], text


def _split_tv_stories(full_text: str) -> list[str]:
    """
    Use Claude Haiku to split the first 10 minutes of a TV news bulletin
    into individual story items.

    Expected structure:
      • Item 1 — Opening + headlines block (anchor intro, ~1-2 min)
      • Items 2+ — Expanded stories (studio or field reporter, one topic each)

    Returns list of raw text items, or [full_text] on failure.
    """
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        return [full_text]

    try:
        import anthropic as _anthro, json as _json, re as _re
        client = _anthro.Anthropic(api_key=anthropic_key)

        system = (
            "קיבלת תמלול של 10 הדקות הראשונות של מהדורת החדשות של רשת 13 בשעה 20:00. "
            "עליך לחלק את הטקסט לפריטים נפרדים.\n\n"
            "מבנה המהדורה (10 דקות ראשונות):\n"
            "• פריט 1 — פתיחה + כותרות: 'ערב טוב' / ברכת פתיחה + הצגת הכותרות "
            "(בדרך כלל דקה-שתיים ראשונות).\n"
            "• פריטים 2+ — ידיעות מורחבות: כל ידיעה עוסקת בנושא אחד. "
            "יכולה לכלול קריין בסטודיו, כתב בשטח, ראיון או כולם יחד — "
            "כולם שייכים לאותה ידיעה כל עוד הנושא זהה.\n\n"
            "כיצד לזהות גבולות בין ידיעות:\n"
            "• שינוי נושא מוחלט — אדם אחר, אירוע אחר, מקום אחר.\n"
            "• ביטויים כמו 'בנושא אחר', 'כעת נעבור', שם כתב חדש בפתיחה.\n"
            "• ציטוטים, ראיונות, תגובות — חלק מאותה ידיעה.\n\n"
            "כללים:\n"
            "• שמור על הטקסט המקורי בדיוק — אל תשנה, אל תוסיף, אל תגרע.\n"
            "• כל פריט לפחות 30 תווים.\n"
            "• החזר JSON בלבד, ללא טקסט נוסף:\n"
            "{\"items\": [\"פריט 1\", \"פריט 2\", ...]}"
        )

        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=min(len(full_text) * 2 + 1000, 6000),
            system=system,
            messages=[{"role": "user", "content": full_text}],
        )

        raw = resp.content[0].text.strip()
        json_match = _re.search(r'\{.*?"items".*?\}', raw, _re.DOTALL)
        data  = _json.loads(json_match.group() if json_match else raw)
        items = [i.strip() for i in data.get("items", []) if i.strip() and len(i.strip()) >= 30]
        if items:
            print(f"   📺 Split into {len(items)} items")
            return items

    except Exception as exc:
        print(f"   ⚠️  _split_tv_stories failed: {exc} — using full text")

    return [full_text]


# ─── Main transcription pipeline ─────────────────────────────────────────────

def transcribe_episode(episode: dict) -> list[dict]:
    """
    Full pipeline for one episode:
      1. Resolve Kaltura → signed HLS URL (yt-dlp)
      2. Download 10 min audio-only (ffmpeg)
      3. Transcribe (Groq Whisper)
      4. Split into stories (Claude)
      5. Fix errors + generate headlines (Claude)

    Returns list of article dicts ready for write_to_sheet().
    """
    from groq import Groq
    groq_client = Groq(api_key=os.environ["GROQ_API_KEY"])

    kaltura_url = _build_kaltura_url(episode["kalturaId"])

    print(f"   📡 Resolving stream URL via yt-dlp...")
    stream_url = _get_stream_url(kaltura_url)

    print(f"   ⬇️  Downloading {_AUDIO_SECONDS}s of audio via ffmpeg...")
    tmp_mp3 = _download_audio_segment(stream_url)

    try:
        size_kb = os.path.getsize(tmp_mp3) // 1024
        print(f"   🎙  Transcribing {size_kb} KB with Whisper...")
        with open(tmp_mp3, "rb") as audio_file:
            result = groq_client.audio.transcriptions.create(
                model="whisper-large-v3",
                file=audio_file,
                language="he",
                response_format="verbose_json",
                timestamp_granularities=["segment"],
                prompt=_WHISPER_PROMPT,
            )
    finally:
        os.unlink(tmp_mp3)

    full_text = getattr(result, "text", "").strip()
    if not full_text:
        print("   ⚠️  Empty transcript — skipping")
        return []

    segments   = getattr(result, "segments", [])
    time_index = _build_time_index(segments)

    # Split into raw story items; filter jingle hallucinations
    raw_items = [i for i in _split_tv_stories(full_text) if len(i.strip()) >= 30]
    if not raw_items:
        raw_items = [full_text]

    # Derive broadcast date: the 20:00 that preceded this upload
    pub_utc  = episode["publish_dt"]
    pub_idt  = pub_utc.astimezone(timezone(timedelta(hours=3)))
    # The broadcast was the 20:00 on the most recent weekday before upload
    broadcast_idt = pub_idt.replace(hour=20, minute=0, second=0, microsecond=0)
    if broadcast_idt > pub_idt:
        broadcast_idt -= timedelta(days=1)
    date_str = broadcast_idt.strftime("%d/%m/%Y")
    hour_str = "20:00"

    print(f"   ✏️  Processing {len(raw_items)} item(s) with Claude Haiku...")

    articles: list[dict] = []

    # ידיעה 1: כותרת ותמלול זהים — בלוק הפתיחה + הכותרות
    raw_title     = raw_items[0]
    story_raws    = raw_items[1:]
    _, cleaned_t  = _process_story(raw_title)
    if len(cleaned_t.strip()) >= 30:
        ts1 = _fmt_ts(_find_start_sec(raw_title, full_text, time_index))
        articles.append({
            "source":    _STATION_NAME,
            "title":     cleaned_t,
            "text":      cleaned_t,
            "url":       episode["episode_url"],
            "_date":     date_str,
            "_hour":     hour_str,
            "_start_ts": ts1,
        })

    # ידיעות 2+: כותרת קצרה + תמלול מלא
    for raw_s, (headline, cleaned_text) in zip(story_raws, [_process_story(s) for s in story_raws]):
        if len(cleaned_text.strip()) < 30:
            continue
        ts = _fmt_ts(_find_start_sec(raw_s, full_text, time_index))
        articles.append({
            "source":    _STATION_NAME,
            "title":     headline,
            "text":      cleaned_text,
            "url":       episode["episode_url"],
            "_date":     date_str,
            "_hour":     hour_str,
            "_start_ts": ts,
        })

    print(f"   ✅ {_STATION_NAME} {hour_str} ({date_str}): {len(articles)} item(s)")
    return articles


# ─── Google Sheet writer ──────────────────────────────────────────────────────

def write_to_sheet(articles: list[dict]) -> None:
    """Append rows to the 'תמלולי טלויזיה' tab (newest-first at row 2)."""
    if not articles:
        return

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    sheet_id   = os.environ.get("RADIO_SHEET_ID", "").strip()
    if not creds_json or not sheet_id:
        print("⚠️  GOOGLE_CREDENTIALS_JSON or RADIO_SHEET_ID not set — skipping sheet write")
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
        sh     = client.open_by_key(sheet_id)

        try:
            ws = sh.worksheet(_SHEET_TAB)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=_SHEET_TAB, rows=5000, cols=7)
            ws.append_row(_SHEET_HEADERS, value_input_option="RAW")
            sh.batch_update({"requests": [{
                "updateSheetProperties": {
                    "properties": {"sheetId": ws.id, "rightToLeft": True},
                    "fields": "rightToLeft",
                }
            }]})
            print(f"📋 Created tab '{_SHEET_TAB}'")

        # Ensure column G header exists
        try:
            if ws.cell(1, 7).value != "זמן התחלה":
                ws.update_cell(1, 7, "זמן התחלה")
        except Exception:
            pass

        rows = [
            [
                a.get("_date",     ""),
                a.get("_hour",     ""),
                a.get("source",    ""),
                a.get("title",     ""),
                a.get("text",      ""),
                a.get("url",       ""),
                a.get("_start_ts", ""),   # G — MM:SS offset in the audio file
            ]
            for a in articles
        ]
        if rows:
            ws.insert_rows(rows, row=2, value_input_option="USER_ENTERED")
            print(f"📋 Wrote {len(rows)} row(s) to '{_SHEET_TAB}'")

    except Exception as exc:
        print(f"⚠️  Sheet write failed: {exc}")
        raise


# ─── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    now_idt = datetime.now(timezone.utc) + timedelta(hours=3)
    print(f"\n📺 TV News Transcription  |  {now_idt.strftime('%d/%m/%Y %H:%M')} IDT\n")

    print("🔍 Fetching latest Channel 13 episode...")
    try:
        episode = fetch_latest_ch13_episode()
    except Exception as exc:
        print(f"❌ Failed to fetch episode: {exc}")
        sys.exit(1)

    if episode is None:
        print("ℹ️  No episode found — nothing to do.")
        return

    # Only process if the episode was published in the last 18 hours
    # (gives a safe window regardless of whether publishDate is UTC or IDT)
    age_hours = (datetime.now(timezone.utc) - episode["publish_dt"]).total_seconds() / 3600
    if age_hours > 18:
        pub_s = episode["publish_dt"].strftime("%Y-%m-%d %H:%M UTC")
        print(f"ℹ️  Latest episode ({episode['title']}) was published {age_hours:.1f}h ago "
              f"({pub_s}) — already processed or too old. Skipping.")
        return

    print(f"📺 Episode : {episode['title']}")
    print(f"   Kaltura : {episode['kalturaId']}")
    print(f"   URL     : {episode['episode_url']}")

    try:
        articles = transcribe_episode(episode)
    except Exception as exc:
        print(f"❌ Transcription failed: {exc}")
        sys.exit(1)

    write_to_sheet(articles)
    print(f"\n✅ Done — {len(articles)} item(s) written to sheet.\n")


if __name__ == "__main__":
    main()
