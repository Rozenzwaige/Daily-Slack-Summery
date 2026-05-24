#!/usr/bin/env python3
"""
Standing Together — Hourly Radio Transcription → Google Sheet
=============================================================
Runs every hour (triggered by GitHub Actions at :20 past each target hour).
Transcribes the latest bulletin from Galatz + Kan Reshet Bet and appends
every individual news item as a row in the "תמלולי רדיו" Google Sheet tab.

Required env vars:
  GROQ_API_KEY            — Groq API key (Whisper transcription)
  GOOGLE_CREDENTIALS_JSON — Service-account JSON (for Sheets)
  RADIO_SHEET_ID          — Spreadsheet ID

Target hours (IDT = UTC+3): 6, 7, 8, 12, 13, 18, 19, 20, 21
9 bulletins × 2 stations × ~4 min ≈ 72 min/day = well within Groq free tier.
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import feedparser
import requests

# ─── Configuration ────────────────────────────────────────────────────────────

GALATZ_RSS = (
    "https://www.omnycontent.com/d/playlist/"
    "6dcbc33f-1fb6-49de-9ae2-ad8a00c01523/"
    "642b5ea6-ce25-4da0-94b8-ade800c22a62/"
    "e9ba2fce-8956-400c-b84e-ade800c27a87/podcast.rss"
)
KAN_RSS = "https://www.spreaker.com/show/6095076/episodes/feed"

TARGET_HOURS_IDT: frozenset[int] = frozenset({6, 7, 8, 12, 13, 18, 19, 20, 21})

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
            )
        os.unlink(tmp)

        hour_str = f"{target_hour_idt:02d}:00"
        items = _split_bulletin(
            getattr(result, "segments", []),
            getattr(result, "text", ""),
        )
        print(f"   ✅ {source_name} {hour_str}: {len(items)} news items")

        articles = []
        for idx, item_text in enumerate(items, 1):
            first_line = item_text.split(".")[0].strip()[:80]
            articles.append({
                "source":         source_name,
                "title":          first_line or f"ידיעה {idx} — {hour_str} {source_name}",
                "url":            audio_url,
                "text":           item_text,
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
    for rss_url, source_name in [
        (GALATZ_RSS, 'גלי צה"ל — רדיו'),
        (KAN_RSS,    "כאן — רשת ב"),
    ]:
        try:
            articles = transcribe_bulletin(rss_url, source_name, current_hour)
            all_articles.extend(articles)
        except Exception as e:
            print(f"❌ Error transcribing {source_name}: {e}")

    write_to_sheet(all_articles)
    print(f"✅ Done — {len(all_articles)} item(s) written to sheet.")


if __name__ == "__main__":
    main()
