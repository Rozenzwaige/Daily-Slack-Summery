#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ifat_processor.py
-----------------
שליפת כתבות מ-API יפעת → Google Sheets

הפעלה:
  python ifat_processor.py --fetch-api           # שלוף אתמול + היום
  python ifat_processor.py --fetch-api --date DD/MM/YYYY
  python ifat_processor.py --archive             # ארכיון היסטורי

עריכת רשימת דמויות: characters.json
הגדרות: ifat_config.json
"""

from __future__ import annotations

import sys
import io
# Force UTF-8 output on Windows console
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import json
import os
import re
import time
import traceback
import unicodedata
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from typing import Optional
from urllib.parse import quote

import gspread
import requests as _requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "ifat_config.json"
CHARACTERS_FILE = BASE_DIR / "characters.json"

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_HEADERS = [
    "תאריך",           # A
    "שעה",              # B
    "גוף תקשורת",      # C
    "מדור",             # D  מדור/תת-מקור מיפעת
    "כותרת",            # E
    "תוכן",             # F
    "כתב",              # G
    "דמויות",           # H
    "קישור",            # I
    "מספר סידורי",     # J  (לשימוש פנימי - מניעת כפילויות)
    "שפת פרסום",       # K  עברית / ערבית / אנגלית / רוסית
    "מדיה",             # L  אינטרנט / טלוויזיה / רדיו / עיתונות
    "סנטימנט",          # M  חיובי / ניטרלי / שלילי
    "סוג פרסום",        # N  dropdown: איזכור / אינסרט / ידיעה / …
    "נושא",             # O  dropdown (מרובה): דמויות ציבוריות / כן שלום / …
    "מגזר",             # P  ← ממולא מגיליון האינדקס (לפרסומים חדשים בלבד)
    "חשיפה",            # Q  audienceRating מיפעת
    "ערך",              # R  itemValue מיפעת
]

# ── Dropdown values ───────────────────────────────────────────────────────────
PUB_TYPE_OPTIONS = [
    "איזכור", "אינסרט", "ידיעה", "טור דעה", "כותרת",
    "לינק", "מודעה", "סינק", "סיקור", "פולו",
    "קרדיט", "ראיון", "תגובתי",
]

TOPIC_OPTIONS = [
    "דמויות ציבוריות",
    "המשמר ההומניטרי",
    "התנגדות למלחמה",
    "חברתי כלכלי",
    "חוסן",
    "כללי",
    "כן שלום",
    "מיגון",
    "מעגלים",
    "סביבתי",
    "סטודנטים",
    "עיר סגולה",
    "רוב העיר",
    "רוזה מדיה",
    "שיבושים",
    "שלום ישראלי פלסטיני",
]


# ============================================================
# Config + state
# ============================================================

def load_config() -> dict:
    with open(CONFIG_FILE, encoding="utf-8-sig") as f:
        return json.load(f)


def load_characters() -> list:
    with open(CHARACTERS_FILE, encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# Character matching
# ============================================================

def _norm(text: str) -> str:
    """Normalize for fuzzy matching: NFC, lowercase, collapsed spaces."""
    text = unicodedata.normalize("NFC", text or "")
    return " ".join(text.lower().split())


def find_character(search_text: str, characters: list) -> str:
    """Return canonical name if any variant appears in search_text, else ''."""
    norm = _norm(search_text)
    for char in characters:
        for variant in char.get("variants", [char["canonical"]]):
            if _norm(variant) in norm:
                return char["canonical"]
    return ""


def enrich(data: dict, characters: list) -> dict:
    """
    Assign reporter_col (G) and character_col (H):
    - G = explicit reporter (if not an ST figure); or non-ST interviewees when
          there is no explicit reporter.
    - H = only ST figures (from reporter, interviewees, or full-text scan).
    """
    reporter         = data.get("reporter", "")
    interviewees_str = data.get("interviewees", "")

    # Split interviewees into individual names
    interviewee_names = (
        [n.strip() for n in re.split(r"[,،]", interviewees_str) if n.strip()]
        if interviewees_str else []
    )

    # Classify each interviewee: ST figure → H, otherwise → G
    st_interviewees:     list[str] = []
    non_st_interviewees: list[str] = []
    for name in interviewee_names:
        char = find_character(name, characters)
        if char:
            if char not in st_interviewees:
                st_interviewees.append(char)
        else:
            non_st_interviewees.append(name)

    # Check reporter
    reporter_char = find_character(reporter, characters) if reporter else ""

    # ── Column G (כתב) ──────────────────────────────────────────────────────
    if reporter_char:
        # Reporter is an ST figure → move to H, G gets non-ST interviewees
        g_col = ", ".join(non_st_interviewees)
    elif reporter:
        # Regular reporter in G
        g_col = reporter
    else:
        # No explicit reporter → non-ST interviewees go to G
        g_col = ", ".join(non_st_interviewees)

    # ── Column H (דמויות) ────────────────────────────────────────────────────
    h_figures: list[str] = []
    if reporter_char and reporter_char not in h_figures:
        h_figures.append(reporter_char)
    for name in st_interviewees:
        if name not in h_figures:
            h_figures.append(name)

    # Also scan full text for any additional ST mentions not yet captured
    full_text = " ".join([
        data.get("title",   ""),
        data.get("content", ""),
        interviewees_str,
        reporter,
    ])
    for char in characters:
        canonical = char["canonical"]
        if canonical not in h_figures and find_character(full_text, [char]):
            h_figures.append(canonical)

    data["reporter_col"]  = g_col
    data["character_col"] = ", ".join(h_figures)
    return data


# ============================================================
# Google Sheets
# ============================================================

def _encode_url(url: str) -> str:
    """Percent-encode non-ASCII characters (e.g. Hebrew) in a URL so that
    Google Sheets recognises the value as a clickable hyperlink."""
    if not url or not url.startswith("http"):
        return url
    return quote(url, safe=";/?:@&=+$,#%-._~!'()*[]")


# Shared gspread client + spreadsheet — created once per run to avoid
# duplicate-auth race conditions that can cause partial get_all_values() reads.
_gspread_client_cache: dict = {}

def _get_spreadsheet(config: dict):
    """Return a cached (client, spreadsheet) pair for the configured spreadsheet."""
    key = config["spreadsheet_id"]
    if key not in _gspread_client_cache:
        creds_file  = BASE_DIR / config["credentials_file"]
        creds       = Credentials.from_service_account_file(str(creds_file), scopes=GOOGLE_SCOPES)
        client      = gspread.authorize(creds)
        spreadsheet = client.open_by_key(key)
        _gspread_client_cache[key] = (client, spreadsheet)
    return _gspread_client_cache[key]


def _get_worksheet(config: dict):
    _, spreadsheet = _get_spreadsheet(config)
    sheet_name = config.get("sheet_name", "פרסומים")

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows="2000", cols="20")

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(SHEET_HEADERS)

    return ws


def _normalize_date(date_str: str) -> str:
    """Ensure date is DD/MM/YYYY (4-digit year). Fixes DD/MM/YY."""
    if not date_str:
        return date_str
    parts = date_str.split("/")
    if len(parts) == 3 and len(parts[2]) == 2:
        parts[2] = "20" + parts[2]
    return "/".join(parts)


def _row_sort_key(row: list) -> datetime:
    """Sort key for a data row: parse date (col A) + time (col B)."""
    date_str = _normalize_date(row[0] if len(row) > 0 else "")
    time_str = row[1] if len(row) > 1 else ""
    try:
        if time_str:
            return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
        return datetime.strptime(date_str, "%d/%m/%Y")
    except ValueError:
        return datetime.min


def _set_dropdown_validation(ws, num_data_rows: int):
    """
    Set data-validation dropdowns on columns N (סוג פרסום) and O (נושא).
    Column N  → index 13 (0-based) — strict single-select from PUB_TYPE_OPTIONS
    Column O  → index 14           — non-strict (allows free text / multi-value)
                                     from TOPIC_OPTIONS
    """
    if num_data_rows <= 0:
        return

    def _validation_request(col_idx: int, options: list[str], strict: bool) -> dict:
        return {
            "setDataValidation": {
                "range": {
                    "sheetId":          ws.id,
                    "startRowIndex":    1,
                    "endRowIndex":      1 + num_data_rows,
                    "startColumnIndex": col_idx,
                    "endColumnIndex":   col_idx + 1,
                },
                "rule": {
                    "condition": {
                        "type":   "ONE_OF_LIST",
                        "values": [{"userEnteredValue": v} for v in options],
                    },
                    "strict":      strict,
                    "showCustomUi": True,
                },
            }
        }

    try:
        ws.spreadsheet.batch_update({"requests": [
            _validation_request(13, PUB_TYPE_OPTIONS, strict=False),
            _validation_request(14, TOPIC_OPTIONS,    strict=False),
        ]})
    except Exception as e:
        print(f"    אזהרה: לא ניתן להגדיר dropdown: {e}")


def _set_row_heights(ws, num_data_rows: int, height_px: int = 21):
    """Set all data rows (header excluded) to a fixed pixel height and CLIP
    wrap strategy so that multi-line content never auto-expands a row."""
    if num_data_rows <= 0:
        return
    try:
        body = {
            "requests": [
                # 1. Fixed pixel height
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId":    ws.id,
                            "dimension":  "ROWS",
                            "startIndex": 1,           # 0-indexed; skip header row
                            "endIndex":   1 + num_data_rows,
                        },
                        "properties": {"pixelSize": height_px},
                        "fields":     "pixelSize",
                    }
                },
                # 2. CLIP wrap so long / multi-line text never expands the row
                {
                    "repeatCell": {
                        "range": {
                            "sheetId":       ws.id,
                            "startRowIndex": 1,
                            "endRowIndex":   1 + num_data_rows,
                        },
                        "cell": {
                            "userEnteredFormat": {"wrapStrategy": "CLIP"}
                        },
                        "fields": "userEnteredFormat.wrapStrategy",
                    }
                },
            ]
        }
        ws.spreadsheet.batch_update(body)
    except Exception as e:
        print(f"    אזהרה: לא ניתן לקבוע גובה שורות: {e}")


def _dedupe_and_sort_sheet(ws):
    """
    Post-process the sheet:
    1. Remove duplicate rows that share the same serial (col J) — keep first occurrence.
    2. Sort all data rows by date (col A) + time (col B), oldest first (top → bottom).
    Called automatically after new rows are appended.
    """
    try:
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return

        data_rows   = all_values[1:]
        orig_count  = len(data_rows)

        # ── Deduplicate ───────────────────────────────────────────────────────
        seen_serials: set[str] = set()
        deduped: list[list]    = []
        for row in data_rows:
            serial = row[9].strip() if len(row) > 9 else ""
            if serial:
                if serial not in seen_serials:
                    seen_serials.add(serial)
                    deduped.append(row)
                # else: skip duplicate
            else:
                deduped.append(row)   # שורה ידנית ללא מספר סידורי — שמור

        # ── Sort ──────────────────────────────────────────────────────────────
        deduped.sort(key=_row_sort_key)

        duplicates_removed = orig_count - len(deduped)
        if deduped == data_rows:
            return   # nothing changed

        # ── Rewrite ───────────────────────────────────────────────────────────
        ws.batch_clear([f"A2:ZZ{1 + orig_count}"])
        if deduped:
            ws.update("A2", deduped, value_input_option="USER_ENTERED")

        if duplicates_removed:
            print(f"    הוסרו {duplicates_removed} כפילויות")
        print(f"    הגיליון מוין מחדש ({len(deduped)} שורות)")

    except Exception as e:
        print(f"    אזהרה: לא ניתן למיין/לנקות כפילויות: {e}")


def append_to_sheet(articles: list[dict], config: dict, update_empty: bool = False,
                    sheet_name: Optional[str] = None):
    """Write articles to the Google Sheet — APPEND ONLY, never clears the sheet.

    New rows are added at the bottom.
    Existing rows (matched by serial in column J) are updated in-place
    only for cells that are currently empty.
    Manually-entered rows are never touched.
    """
    if sheet_name:
        config = {**config, "sheet_name": sheet_name}
    if not articles:
        return

    ws = _get_worksheet(config)

    # --- Read existing serials + row positions ---
    # We only need column J (serial, index 9) to detect duplicates,
    # and the full row only for rows we might need to update.
    all_values = ws.get_all_values()
    raw_data   = all_values[1:] if len(all_values) > 1 else []

    # Build serial → (sheet_row_number_1based, row_data)
    # sheet_row_number: header=1, first data row=2, …
    serial_to_sheetrow: dict[str, tuple] = {}
    for i, row in enumerate(raw_data, start=2):   # 1-based; row 1 = header
        s = row[9].strip() if len(row) > 9 else ""
        if s and s not in serial_to_sheetrow:
            serial_to_sheetrow[s] = (i, row)

    known_serials: set[str] = set(serial_to_sheetrow.keys())

    # --- Classify each article ---
    new_rows:    list[list]            = []   # articles to append
    row_updates: list[tuple[int,list]] = []   # (sheet_row_num, full_row_data)
    skipped = updated = 0

    for a in articles:
        serial    = a.get("serial", "").strip()
        new_title = a.get("title",  "").strip()

        if serial and serial in known_serials:
            sheet_row, old_row = serial_to_sheetrow[serial]

            existing_title = old_row[4].strip()  if len(old_row) > 4  else ""
            existing_chars = old_row[7].strip()  if len(old_row) > 7  else ""
            existing_lang  = old_row[10].strip() if len(old_row) > 10 else ""
            existing_media = old_row[11].strip() if len(old_row) > 11 else ""
            existing_sent  = old_row[12].strip() if len(old_row) > 12 else ""
            existing_ptype = old_row[13].strip() if len(old_row) > 13 else ""
            existing_topic = old_row[14].strip() if len(old_row) > 14 else ""

            new_chars    = a.get("character_col", "").strip()
            new_reporter = a.get("reporter_col",  "").strip()
            new_lang     = a.get("language",      "").strip()
            new_media    = a.get("media",         "").strip()
            new_sent     = a.get("sentiment",     "").strip()
            new_ptype    = a.get("pub_type",      "").strip()
            new_topic    = a.get("topic",         "").strip()

            needs_update = False
            if update_empty and new_title and not existing_title:
                needs_update = True
            if new_chars and not existing_chars:
                needs_update = True
            if (new_lang or new_media or new_sent or new_ptype) and not (
                    existing_lang and existing_media and existing_sent and existing_ptype):
                needs_update = True
            if new_topic and not existing_topic:
                needs_update = True

            if needs_update:
                existing_link = old_row[8] if len(old_row) > 8 else ""
                updated_row = [
                    a.get("date",    "") or (old_row[0] if len(old_row) > 0 else ""),
                    a.get("time",    "") or (old_row[1] if len(old_row) > 1 else ""),
                    a.get("source",  "") or (old_row[2] if len(old_row) > 2 else ""),
                    a.get("section", "") or (old_row[3] if len(old_row) > 3 else ""),
                    new_title     or existing_title,
                    a.get("content", "") or (old_row[5] if len(old_row) > 5 else ""),
                    new_reporter  or (old_row[6] if len(old_row) > 6 else ""),
                    new_chars     or existing_chars,
                    existing_link,
                    serial,
                    new_lang  or existing_lang,
                    new_media or existing_media,
                    new_sent  or existing_sent,
                    new_ptype or existing_ptype,
                    new_topic or existing_topic,
                ]
                row_updates.append((sheet_row, updated_row))
                updated += 1
            else:
                skipped += 1
            continue

        # New article — build row and queue for appending
        link = _encode_url(a.get("link", ""))
        new_rows.append([
            a.get("date",         ""),   # A
            a.get("time",         ""),   # B
            a.get("source",       ""),   # C
            a.get("section",      ""),   # D - מדור (יפעת subsource)
            a.get("title",        ""),   # E
            a.get("content",      ""),   # F
            a.get("reporter_col", ""),   # G
            a.get("character_col",""),   # H
            link,                        # I
            serial,                      # J
            a.get("language",     ""),   # K
            a.get("media",        ""),   # L
            a.get("sentiment",    ""),   # M
            a.get("pub_type",     ""),   # N
            a.get("topic",        ""),   # O
            a.get("sector",       ""),   # P - מגזר (מהאינדקס)
            a.get("audience",     ""),   # Q - חשיפה
            a.get("item_value",   ""),   # R - ערך
        ])
        if serial:
            known_serials.add(serial)

    # --- Apply in-place updates (one ws.update call per changed row) ---
    for sheet_row, row_data in row_updates:
        try:
            end_col = chr(ord('A') + len(row_data) - 1)   # 'O' for 15 columns
            ws.update(f"A{sheet_row}:{end_col}{sheet_row}",
                      [row_data], value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"    אזהרה: לא ניתן לעדכן שורה {sheet_row}: {e}")

    # --- Normalize dates and sort new rows by date+time before appending ---
    if new_rows:
        for row in new_rows:
            if row:
                row[0] = _normalize_date(row[0])
        new_rows.sort(key=_row_sort_key)
        ws.append_rows(new_rows, value_input_option="USER_ENTERED",
                       insert_data_option="INSERT_ROWS")

    # --- Formatting (dropdowns + row heights) for new rows only ---
    if new_rows or row_updates:
        total_rows = len(raw_data) + len(new_rows)
        _set_row_heights(ws, total_rows)
        _set_dropdown_validation(ws, total_rows)

    # --- Deduplicate + sort the full sheet after new rows were added ---
    if new_rows:
        _dedupe_and_sort_sheet(ws)

    # --- Print summary ---
    if skipped:
        print(f"    דולגו {skipped} כתבות כפולות")
    if updated:
        print(f"    עודכנו {updated} כתבות קיימות")
    if new_rows:
        total = len(raw_data) + len(new_rows)
        print(f"    נוספו {len(new_rows)} כתבות חדשות (סה\"כ ~{total} בגיליון)")
    if not new_rows and not updated:
        print("    אין כתבות חדשות להוסיף")


# ============================================================
# יפעת API – fetch articles directly (replaces PDF workflow)
# ============================================================

_IFAT_API_BASE = "https://media.ifat.com/data/api/customer"

# JS snippet used by Playwright to call GetArticles from within the browser
_FETCH_ARTICLES_JS = """
async ([token, pageNum, pageSize]) => {
    const resp = await fetch(
        `https://media.ifat.com/data/api/customer/GetArticles?PageNumber=${pageNum}&PageSize=${pageSize}`,
        {
            method: 'POST',
            headers: {
                'Authorization': 'bearer ' + token,
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/plain, */*',
            },
            body: JSON.stringify({
                ItemType: '', Sort: 'desc', SortField: 'publishdate',
                Source: '', SubjectID: ''
            })
        }
    );
    return await resp.json();
}
"""


def _ifat_browser_login(config: dict):
    """
    Launch headless Chromium, log in to יפעת, and return (playwright, browser, page, token).
    The browser must be closed by the caller.
    """
    from playwright.sync_api import sync_playwright

    pw      = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context()
    bpage   = context.new_page()

    # Intercept the Login response to capture the JWT token
    token_holder: dict = {}

    def _on_resp(response):
        if "Login" in response.url and response.status == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and "token" in data:
                    token_holder["token"] = data["token"]
            except Exception:
                pass

    bpage.on("response", _on_resp)

    bpage.goto("https://media.ifat.com/login", wait_until="load", timeout=60000)

    # Dismiss cookie consent popup if present
    try:
        consent = bpage.locator("button:has-text('אישור')")
        if consent.first.is_visible(timeout=2000):
            consent.first.click()
            bpage.wait_for_timeout(300)
    except Exception:
        pass

    bpage.wait_for_selector("input", timeout=10000)

    # Switch to password tab if present
    try:
        tab = bpage.get_by_text("התחברות עם סיסמה")
        if tab.is_visible(timeout=2000):
            tab.click()
            bpage.wait_for_timeout(300)
    except Exception:
        pass

    # Fill credentials and submit
    bpage.locator("input").nth(0).fill(config["ifat_username"])
    bpage.locator("input").nth(1).fill(config["ifat_password"])
    bpage.locator("input").nth(1).press("Enter")
    bpage.wait_for_url("**/dashboard**", timeout=40000)

    # Extract token (from intercepted response or localStorage)
    token = token_holder.get("token", "")
    if not token:
        token = bpage.evaluate("""() => {
            for (let k of Object.keys(localStorage)) {
                const v = localStorage.getItem(k);
                if (v && v.startsWith('ey')) return v;
            }
            return null;
        }""")

    if not token:
        browser.close()
        pw.stop()
        raise RuntimeError("Login נכשל: לא נמצא token")

    return pw, browser, bpage, token


def _ifat_fetch_page(bpage, token: str, page: int, page_size: int = 100) -> list:
    """Fetch one page of articles by running fetch() inside the browser."""
    result = bpage.evaluate(_FETCH_ARTICLES_JS, [token, page, page_size])
    if isinstance(result, list):
        return result
    return result.get("items", result.get("Items", []))


# ── HTTP-based login (no browser required) ────────────────────────────────────

def _ifat_http_login(config: dict):
    """
    Login to יפעת via direct HTTP POST — no browser needed.
    Tries several common endpoint/payload combinations.
    Returns (requests.Session, token_str).
    Raises RuntimeError if all attempts fail.
    """
    import requests as _requests

    session = _requests.Session()
    session.headers.update({
        "Content-Type":  "application/json",
        "Accept":        "application/json, text/plain, */*",
        "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin":        "https://media.ifat.com",
        "Referer":       "https://media.ifat.com/login",
    })

    username = config["ifat_username"]
    password = config["ifat_password"]

    login_urls = [
        "https://media.ifat.com/data/api/customer/Login",
        "https://media.ifat.com/api/Login",
        "https://media.ifat.com/api/customer/Login",
    ]
    payloads = [
        {"UserName": username, "Password": password},
        {"username": username, "password": password},
        {"Username": username, "Password": password},
    ]

    for url in login_urls:
        for payload in payloads:
            try:
                resp = session.post(url, json=payload, timeout=30)
                print(f"  [DEBUG] {url} → status={resp.status_code} body={resp.text[:200]!r}")
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        token = data.get("token") or data.get("Token") or ""
                        if token and token.startswith("ey"):
                            print(f"  HTTP login הצליח דרך {url}")
                            return session, token
            except Exception as e:
                print(f"  [DEBUG] {url} → exception: {e}")
                continue

    raise RuntimeError("HTTP login נכשל — לא הצלחנו לקבל token מאף endpoint")


def _ifat_fetch_page_http(session, token: str, page: int, page_size: int = 100) -> list:
    """Fetch one page of articles via direct HTTP (no browser)."""
    url  = (
        f"https://media.ifat.com/data/api/customer/GetArticles"
        f"?PageNumber={page}&PageSize={page_size}"
    )
    body = {"ItemType": "", "Sort": "desc", "SortField": "publishdate",
            "Source": "", "SubjectID": ""}
    resp = session.post(
        url, json=body,
        headers={"Authorization": f"bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    if isinstance(result, list):
        return result
    return result.get("items", result.get("Items", []))


def _api_item_to_dict(item: dict, source_index: dict | None = None,
                      language_index: dict | None = None) -> dict:
    """Convert a single יפעת API article object to our internal format."""
    pub = (item.get("publishdate", "") or "").strip()
    try:
        dt       = datetime.fromisoformat(pub[:19])
        date_str = dt.strftime("%d/%m/%Y")
        time_str = dt.strftime("%H:%M")
    except Exception:
        date_str = time_str = ""

    summery   = (item.get("summery",      "") or "").strip()
    subtitle  = (item.get("itemsubtitle", "") or "").strip()
    share_url = (item.get("shareUrl",     "") or "").strip()

    # For online articles summery IS the article URL; subtitle has the lead text
    if summery.startswith("http"):
        link    = share_url or summery
        content = subtitle
    else:
        link    = share_url
        content = summery or subtitle

    # keywords field: e.g. "אלון לי גרין / Alon-Lee Green - מנכ"ל תנועת עומדים ביחד"
    # May contain multiple entries separated by semicolons or commas
    raw_keywords = (item.get("keywords", "") or "").strip()
    # Normalise: keep only the Hebrew name before " / " or " - "
    kw_parts = []
    for kw in re.split(r"[;،]", raw_keywords):
        kw = kw.strip()
        if kw:
            # "אלון לי גרין / Alon-Lee Green - מנכ"ל..." → "אלון לי גרין"
            kw = re.split(r"\s*/\s*|\s+-\s+", kw)[0].strip()
            if kw:
                kw_parts.append(kw)
    interviewees = ", ".join(kw_parts)

    # Detect "שלום ישראלי פלסטיני" topic — used later for routing to separate sheet
    subsubject_names = item.get("subsubjectNames") or []
    if isinstance(subsubject_names, str):
        subsubject_names = [subsubject_names]
    all_topic_text = raw_keywords + " " + " ".join(subsubject_names)
    peace_topic = "שלום ישראלי פלסטיני" in all_topic_text

    raw_itemtype = item.get("itemtype", 1)
    title_str    = (item.get("itemtitle", "") or "").strip()

    source_str = (item.get("source", "") or "").strip()
    # Column D = יפעת subsource (מדור); Column P = index-based sector (מגזר)
    # subsource מגיע כ-"שם מקור - מדור" או "source-section" — מסירים את קידומת המקור
    _raw_sub  = (item.get("subsource", "") or "").strip()
    if " - " in _raw_sub:
        # "הארץ - כותרת"  →  "כותרת"
        subsource = _raw_sub.split(" - ", 1)[1].strip()
    elif "-" in _raw_sub and source_str:
        # "haaretz-front" (כאשר source="haaretz")  →  "front"
        _src_lo  = source_str.lower()
        _sub_lo  = _raw_sub.lower()
        if _sub_lo.startswith(_src_lo + "-"):
            subsource = _raw_sub[len(source_str):].lstrip("-").strip()
        else:
            subsource = _raw_sub
    else:
        subsource = _raw_sub
    sector     = lookup_sector(source_str, source_index or {}) if source_index else ""

    return {
        "date":         date_str,
        "time":         time_str,
        "source":       source_str,
        "section":      subsource,   # D - מדור (יפעת subsource)
        "sector":       sector,      # P - מגזר (from index)
        "title":        title_str,
        "content":      content,
        "reporter":     (item.get("reporter",  "") or "").strip(),
        "interviewees": interviewees,
        "link":         link,
        "serial":       str(item.get("itemid", "")),
        "is_print":     False,    # no Drive upload needed
        "peace_topic":  peace_topic,
        # metadata for new sheet columns
        "_itemtype":    raw_itemtype,
        "language":     (lookup_language(source_str, language_index or {})
                         or _api_language(item, title_str + " " + content)),
        "media":        _detect_media(raw_itemtype),
        "sentiment":    _translate_sentiment(item.get("sentiment", "")),
        # pub_type and topic depend on character_col (set after enrich())
        "pub_type":     "",
        "topic":        "",
        "audience":     item.get("audienceRating") or "",  # Q - חשיפה
        "item_value":   item.get("itemValue")      or "",  # R - ערך
    }


_OMETZ_KEYWORD = "עומדים ביחד"

# ── Source index (loaded once per run) ───────────────────────────────────────
# גיליון "אינדקס": A=שם מקור, B=מגזר, C=שפה
_source_index_cache:   dict[str, str] | None = None
_language_index_cache: dict[str, str] | None = None


def load_source_index(config: dict) -> dict:
    """
    Read the 'אינדקס' sheet and return a lowercase-normalised dict:
        { normalised_source_name → sector_string }
    Also populates _language_index_cache from column C (שפה).
    Reuses the shared gspread connection. Results are cached per run.
    """
    global _source_index_cache, _language_index_cache
    if _source_index_cache is not None:
        return _source_index_cache

    try:
        _, spreadsheet = _get_spreadsheet(config)
        ws             = spreadsheet.worksheet("אינדקס")
        rows           = ws.get_all_values()
    except Exception as e:
        print(f"[אזהרה] לא ניתן לטעון גיליון אינדקס: {e}")
        _source_index_cache   = {}
        _language_index_cache = {}
        return _source_index_cache

    sector_idx:   dict[str, str] = {}
    language_idx: dict[str, str] = {}
    for row in rows[1:]:   # skip header
        if not (len(row) >= 2 and row[0].strip()):
            continue
        key = row[0].strip().lower()
        sector_idx[key] = row[1].strip()
        if len(row) >= 3 and row[2].strip():          # עמודה C = שפה
            language_idx[key] = row[2].strip()

    _source_index_cache   = sector_idx
    _language_index_cache = language_idx
    return _source_index_cache


def lookup_sector(source: str, index: dict[str, str]) -> str:
    """
    Case-insensitive lookup of source in the index.
    Falls back to partial (substring) match if exact match not found.
    """
    if not source or not index:
        return ""
    s = source.strip().lower()
    if s in index:
        return index[s]
    for key, sector in index.items():
        if key in s or s in key:
            return sector
    return ""


def lookup_language(source: str, index: dict[str, str]) -> str:
    """
    Case-insensitive lookup of source language from the index (column C).
    Falls back to partial (substring) match if exact match not found.
    """
    if not source or not index:
        return ""
    s = source.strip().lower()
    if s in index:
        return index[s]
    for key, lang in index.items():
        if key in s or s in key:
            return lang
    return ""


# ── Topic auto-detection ──────────────────────────────────────────────────────
def _detect_topic(art: dict) -> str:
    """
    Auto-detect topic(s) for column O.
    Returns comma-separated string of applicable topics.
    """
    topics: list[str] = []

    if art.get("character_col", "").strip():
        topics.append("דמויות ציבוריות")

    if art.get("peace_topic"):
        topics.append("שלום ישראלי פלסטיני")

    return ", ".join(topics)

# ── Media type ────────────────────────────────────────────────────────────────
_ITEMTYPE_TO_MEDIA: dict[int, str] = {
    0:  "עיתונות",
    1:  "אינטרנט",
    2:  "רדיו",
    10: "טלוויזיה",
}

def _detect_media(itemtype) -> str:
    return _ITEMTYPE_TO_MEDIA.get(int(itemtype) if itemtype is not None else 1, "אינטרנט")


# ── Language ──────────────────────────────────────────────────────────────────

# Numeric language IDs that Ifat may return (languageid field)
_LANGUAGE_ID_MAP: dict = {
    1: 'עברית',    '1': 'עברית',
    2: 'ערבית',    '2': 'ערבית',
    3: 'אנגלית',   '3': 'אנגלית',
    4: 'רוסית',    '4': 'רוסית',
    5: 'צרפתית',   '5': 'צרפתית',
    6: 'ספרדית',   '6': 'ספרדית',
    7: 'גרמנית',   '7': 'גרמנית',
    8: 'אמהרית',   '8': 'אמהרית',
    9: 'תיגרינית', '9': 'תיגרינית',
}

# String language names (lowercase) that Ifat may return (language field)
_LANGUAGE_NAME_MAP: dict = {
    'hebrew':    'עברית',   'heb': 'עברית',   'עברית': 'עברית',
    'arabic':    'ערבית',   'ara': 'ערבית',   'ערבית': 'ערבית',
    'english':   'אנגלית',  'eng': 'אנגלית',  'אנגלית': 'אנגלית',
    'russian':   'רוסית',   'rus': 'רוסית',   'רוסית': 'רוסית',
    'french':    'צרפתית',  'fre': 'צרפתית',  'צרפתית': 'צרפתית',
    'spanish':   'ספרדית',  'spa': 'ספרדית',  'ספרדית': 'ספרדית',
    'german':    'גרמנית',  'ger': 'גרמנית',  'גרמנית': 'גרמנית',
    'amharic':   'אמהרית',                    'אמהרית': 'אמהרית',
    'tigrinya':  'תיגרינית','tigrigna': 'תיגרינית', 'תיגרינית': 'תיגרינית',
}


def _api_language(item: dict, fallback_text: str = "") -> str:
    """
    Read language from a יפעת API item dict.
    Priority:
      1. languageid (numeric)  →  _LANGUAGE_ID_MAP
      2. language / lang (string)  →  _LANGUAGE_NAME_MAP
      3. Text-based detection on fallback_text (last resort)
    """
    # 1. Numeric ID
    lang_id = (item.get("languageid") or item.get("LanguageId")
               or item.get("languageId") or item.get("language_id"))
    if lang_id is not None:
        mapped = _LANGUAGE_ID_MAP.get(lang_id) or _LANGUAGE_ID_MAP.get(str(lang_id))
        if mapped:
            return mapped

    # 2. String name (various casings)
    lang_str = (
        item.get("language") or item.get("Language")
        or item.get("lang")  or item.get("Lang") or ""
    ).strip()
    if lang_str:
        mapped = (_LANGUAGE_NAME_MAP.get(lang_str.lower())
                  or _LANGUAGE_NAME_MAP.get(lang_str))
        if mapped:
            return mapped

    # 3. Fallback: detect from text
    return _detect_language(fallback_text) if fallback_text else 'עברית'


def _detect_language(text: str) -> str:
    """Last-resort language detection by counting Unicode character ranges."""
    hebrew   = len(re.findall(r'[\u0590-\u05FF\uFB1D-\uFB4F]', text))
    arabic   = len(re.findall(r'[\u0600-\u06FF\uFB50-\uFDFF\uFE70-\uFEFF]', text))
    cyrillic = len(re.findall(r'[\u0400-\u04FF]', text))
    latin    = len(re.findall(r'[A-Za-z]', text))
    scores   = [('עברית', hebrew), ('ערבית', arabic), ('רוסית', cyrillic), ('אנגלית', latin)]
    best     = max(scores, key=lambda x: x[1])
    return best[0] if best[1] > 0 else 'עברית'


# ── Sentiment ─────────────────────────────────────────────────────────────────
_SENTIMENT_MAP = {
    'חיובי': 'חיובי', 'positive': 'חיובי',
    'שלילי': 'שלילי', 'negative': 'שלילי',
    'נייטרלי': 'נייטרלי', 'neutral': 'נייטרלי', 'ניטרלי': 'נייטרלי',
}

def _translate_sentiment(raw: str) -> str:
    return _SENTIMENT_MAP.get((raw or '').strip(), 'נייטרלי')


# ── Publication type ──────────────────────────────────────────────────────────
def _detect_pub_type(art: dict) -> str:
    """
    Best-effort classification. Conservative: uses 'איזכור' when an ST figure
    is mentioned (can't verify from API if actual audio/video clip exists).
    Types requiring human judgment (אינסרט, סינק, ראיון, טור דעה, etc.) are
    left for manual editing in the sheet.
    """
    content  = (art.get("content",       "") or "").strip()
    link     = (art.get("link",          "") or "").strip()
    char_col = (art.get("character_col", "") or "").strip()
    title    = (art.get("title",         "") or "").strip()

    # Pure link (no readable content)
    if not content and link:
        return "לינק"

    # Very short with no content → headline
    if not content and title and len(title) < 80:
        return "כותרת"

    # ST figure mentioned → "איזכור" (conservative; user can upgrade to
    # אינסרט / סינק / ראיון manually if there was actual audio/video)
    if char_col:
        return "איזכור"

    # Default for all media types
    return "ידיעה"


def _is_peace_only(art: dict) -> bool:
    """
    Return True when the article should go to the peace sheet instead of main:
      - tagged as peace_topic by יפעת
      - AND no ST character was detected
      - AND "עומדים ביחד" does not appear anywhere in the article text
    """
    if not art.get("peace_topic"):
        return False
    if art.get("character_col", "").strip():
        return False   # has an ST figure → main sheet
    full = " ".join([
        art.get("title",        ""),
        art.get("content",      ""),
        art.get("interviewees", ""),
        art.get("reporter",     ""),
    ])
    if _OMETZ_KEYWORD in full:
        return False   # mentions עומדים ביחד → main sheet
    return True


# ============================================================
# Google Drive — print article image upload
# ============================================================

def _get_drive_service(config: dict):
    creds_file = BASE_DIR / config["credentials_file"]
    creds = Credentials.from_service_account_file(str(creds_file), scopes=GOOGLE_SCOPES)
    return build("drive", "v3", credentials=creds)


def _get_or_create_drive_folder(drive, parent_id: str, folder_name: str) -> str:
    q = (
        f"name='{folder_name}' "
        "and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents "
        "and trashed=false"
    )
    res = drive.files().list(
        q=q, fields="files(id)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]
    folder = drive.files().create(
        body={"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]},
        fields="id", supportsAllDrives=True,
    ).execute()
    return folder["id"]


def _extract_media_url_from_share_page(share_url: str) -> str:
    """Fetch the ifat share page and extract the direct MP3/MP4 URL."""
    try:
        r = _requests.get(share_url, timeout=15)
        r.raise_for_status()
        import re as _re
        matches = _re.findall(r'(https?://[^\s"<>]+\.(?:mp3|mp4|wav))', r.text)
        return matches[0] if matches else ""
    except Exception:
        return ""


def _resolve_media_url_and_ext(link: str) -> tuple[str, str]:
    """
    Given any ifat-related link, return (direct_media_url, ext).
    Handles:
      - share.ifat.com/item?... → parse HTML for MP3/MP4 URL
      - ifatmediasite.com/Mediaserver/... → fetch directly (stream), detect ext from Content-Type
      - URL already ending in .mp3/.mp4 → return as-is
    Returns ("", "") if nothing found or download fails.
    """
    if not link:
        return "", ""

    # Already has a known extension
    low = link.lower()
    for ext in ("mp3", "mp4", "wav"):
        if low.endswith("." + ext):
            return link, ext

    # share.ifat.com → extract from HTML
    if "share.ifat.com" in link:
        direct = _extract_media_url_from_share_page(link)
        if direct:
            ext = direct.rsplit(".", 1)[-1].lower()
            return direct, ext
        return "", ""

    # Direct media server URL (e.g. ifatmediasite.com/Mediaserver/Radio/...)
    # — do a HEAD request to find Content-Type
    try:
        head = _requests.head(link, timeout=15, allow_redirects=True)
        ct = head.headers.get("Content-Type", "").lower()
        if "mpeg" in ct or "mp3" in ct:
            return link, "mp3"
        if "mp4" in ct or "video" in ct:
            return link, "mp4"
        # Fallback: try GET with stream=True and check first bytes
        r = _requests.get(link, timeout=15, stream=True)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "").lower()
        r.close()
        if "mpeg" in ct or "mp3" in ct:
            return link, "mp3"
        if "mp4" in ct or "video" in ct:
            return link, "mp4"
    except Exception:
        pass
    return "", ""


def _upload_media_to_drive(media_url: str, article: dict, config: dict, drive,
                           ext: str = "") -> str:
    """Download MP3/MP4 from ifat and upload to Google Drive. Returns webViewLink."""
    import tempfile, os
    if not ext:
        ext = media_url.rsplit(".", 1)[-1].lower()
    mime = {"mp3": "audio/mpeg", "mp4": "video/mp4", "wav": "audio/wav"}.get(ext, "application/octet-stream")

    r = _requests.get(media_url, timeout=120, stream=True)
    r.raise_for_status()

    date_str = article.get("date", "")
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        year_str  = str(dt.year)
        month_str = f"{dt.month:02d}"
        day_str   = f"{dt.day:02d}"
    except Exception:
        year_str = month_str = day_str = "unknown"

    root_folder  = config["drive_folder_id"]
    year_folder  = _get_or_create_drive_folder(drive, root_folder,  year_str)
    month_folder = _get_or_create_drive_folder(drive, year_folder,  month_str)
    day_folder   = _get_or_create_drive_folder(drive, month_folder, day_str)

    serial = article.get("serial", "unknown")
    source = re.sub(r"[^\w\-]", "_", article.get("source", "unknown"))[:30]
    fname  = f"{date_str.replace('/', '-')}_{source}_{serial}.{ext}"

    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            tmp.write(chunk)
        tmp_path = tmp.name

    try:
        uploaded = drive.files().create(
            body={"name": fname, "parents": [day_folder]},
            media_body=MediaFileUpload(tmp_path, mimetype=mime, resumable=True),
            fields="id, webViewLink",
            supportsAllDrives=True,
        ).execute()
        drive.permissions().create(
            fileId=uploaded["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
        return uploaded.get("webViewLink", "")
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _get_item_clips(token: str, itemid) -> list[dict]:
    """Call GetItemClips API to get JPG/PDF URLs for a print article."""
    url = f"https://media.ifat.com/data/api/customer/GetItemClips?ItemID={itemid}"
    resp = _requests.get(url, headers={"Authorization": f"bearer {token}"}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _upload_clip_jpg_to_drive(jpg_url: str, article: dict, config: dict, drive) -> str:
    """Download JPG from ifatmediasite and upload to Google Drive. Returns webViewLink."""
    import tempfile, os
    r = _requests.get(jpg_url, timeout=30)
    r.raise_for_status()

    date_str = article.get("date", "")
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        year_str  = str(dt.year)
        month_str = f"{dt.month:02d}"
        day_str   = f"{dt.day:02d}"
    except Exception:
        year_str = month_str = day_str = "unknown"

    root_folder  = config["drive_folder_id"]
    year_folder  = _get_or_create_drive_folder(drive, root_folder,  year_str)
    month_folder = _get_or_create_drive_folder(drive, year_folder,  month_str)
    day_folder   = _get_or_create_drive_folder(drive, month_folder, day_str)

    serial = article.get("serial", "unknown")
    source = re.sub(r"[^\w\-]", "_", article.get("source", "unknown"))[:30]
    fname  = f"{date_str.replace('/', '-')}_{source}_{serial}.jpg"

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
        tmp.write(r.content)
        tmp_path = tmp.name

    try:
        uploaded = drive.files().create(
            body={"name": fname, "parents": [day_folder]},
            media_body=MediaFileUpload(tmp_path, mimetype="image/jpeg", resumable=False),
            fields="id, webViewLink",
            supportsAllDrives=True,
        ).execute()
        drive.permissions().create(
            fileId=uploaded["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
        return uploaded.get("webViewLink", "")
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def fetch_api_articles(
    config: dict,
    characters: list,
    target_date: Optional[str] = None,
) -> tuple[list[dict], list[dict]]:
    """
    Login → fetch all articles for `target_date` (DD/MM/YYYY, default: yesterday)
    → enrich with character matching
    → return (main_articles, peace_articles).
      main_articles  – go to the regular sheet
      peace_articles – tagged שלום ישראלי פלסטיני with no ST connection
    """
    from datetime import timedelta

    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

    try:
        target_dt = datetime.strptime(target_date, "%d/%m/%Y").date()
    except ValueError:
        raise ValueError(f"פורמט תאריך שגוי: {target_date}  (צפוי DD/MM/YYYY)")

    source_index   = load_source_index(config)
    language_index = _language_index_cache or {}
    if language_index:
        print(f"  נטענו {len(language_index)} מקורות עם שפה מהאינדקס")
    else:
        print(f"  [שים לב] אין עמודת שפה באינדקס — נשתמש בזיהוי אוטומטי")

    drive_service = None
    if config.get("drive_folder_id"):
        try:
            drive_service = _get_drive_service(config)
            print(f"  Drive מאותחל — כתבות מודפסות יועלו אוטומטית")
        except Exception as e:
            print(f"  [אזהרה] לא ניתן לאתחל Drive: {e}")

    print(f"מתחבר ל-API יפעת...")
    pw, browser, bpage, token = _ifat_browser_login(config)
    print(f"מחובר. מושך כתבות עבור {target_date}...")

    main_articles:  list[dict] = []
    peace_articles: list[dict] = []
    PAGE_SIZE = 100

    try:
        for page in range(1, 9999):
            items = _ifat_fetch_page(bpage, token, page=page, page_size=PAGE_SIZE)
            if not items:
                break

            past_target = False
            for item in items:
                pub = (item.get("publishdate", "") or "")[:19]
                try:
                    item_dt = datetime.fromisoformat(pub).date()
                except Exception:
                    continue

                if item_dt == target_dt:
                    art = _api_item_to_dict(item, source_index=source_index,
                                            language_index=language_index)
                    enrich(art, characters)
                    art["pub_type"] = _detect_pub_type(art)
                    art["topic"]    = _detect_topic(art)

                    # כתבה מודפסת — הורד תמונה והעלה לDrive
                    if drive_service and art.get("media") == "עיתונות" and not art.get("link"):
                        try:
                            clips = _get_item_clips(token, item.get("itemid"))
                            if clips:
                                jpg_url = clips[0].get("url", "")
                                if jpg_url:
                                    drive_link = _upload_clip_jpg_to_drive(jpg_url, art, config, drive_service)
                                    art["link"] = drive_link
                                    print(f"    הועלה לDrive: {art.get('source')} — {art.get('title', '')[:40]}")
                        except Exception as e:
                            print(f"    [אזהרה] שגיאה בהעלאה לDrive (מודפסת): {e}")

                    # רדיו / טלוויזיה — הורד MP3/MP4 והעלה לDrive
                    elif drive_service and art.get("media") in ("רדיו", "טלוויזיה"):
                        share_url = item.get("shareUrl", "")
                        if share_url:
                            try:
                                media_url = _extract_media_url_from_share_page(share_url)
                                if media_url:
                                    drive_link = _upload_media_to_drive(media_url, art, config, drive_service)
                                    art["link"] = drive_link
                                    ext = media_url.rsplit(".", 1)[-1].upper()
                                    print(f"    הועלה {ext} לDrive: {art.get('source')} — {art.get('title', '')[:40]}")
                            except Exception as e:
                                print(f"    [אזהרה] שגיאה בהעלאה לDrive ({art.get('media')}): {e}")

                    if _is_peace_only(art):
                        peace_articles.append(art)
                    else:
                        main_articles.append(art)
                elif item_dt < target_dt:
                    past_target = True
                    break

            if past_target or len(items) < PAGE_SIZE:
                break
    finally:
        browser.close()
        pw.stop()

    print(f"נמצאו {len(main_articles)} כתבות ראשיות + {len(peace_articles)} כתבות שלום ישראלי-פלסטיני עבור {target_date}")
    return main_articles, peace_articles


# ============================================================
# Archive: fetch a date range → "ארכיון" sheet
# ============================================================

def fetch_archive_range(
    config: dict,
    characters: list,
    from_date_str: str = "01/01/2020",
    to_date_str: Optional[str] = None,
    write_batch_size: int = 300,
) -> int:
    """
    Fetch ALL articles from Yifat API in [from_date, to_date] and write
    them to the archive sheet (config["archive_sheet_name"], default "ארכיון").

    This is a ONE-TIME backfill operation.  It does NOT touch the main
    "עומדים ביחד פרסומים" or "שלום ישראלי פלסטיני" tabs and therefore
    cannot interfere with the daily --fetch-api runs.

    Returns the total number of articles written.
    """
    from datetime import timedelta

    if to_date_str is None:
        to_date_str = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

    try:
        from_dt = datetime.strptime(from_date_str, "%d/%m/%Y").date()
        to_dt   = datetime.strptime(to_date_str,   "%d/%m/%Y").date()
    except ValueError as exc:
        raise ValueError(f"פורמט תאריך שגוי ({exc}).  השתמש ב-DD/MM/YYYY") from exc

    archive_sheet = config.get("archive_sheet_name", "ארכיון")
    archive_cfg   = {**config, "sheet_name": archive_sheet}

    print(f"\n{'='*60}")
    print(f"  ארכיון יפעת: {from_date_str} → {to_date_str}")
    print(f"  טאב יעד: '{archive_sheet}'")
    print(f"  גודל אצווה לכתיבה: {write_batch_size} כתבות")
    print(f"{'='*60}\n")

    source_index = load_source_index(config)
    print("מתחבר ל-API יפעת...")
    pw, browser, bpage, token = _ifat_browser_login(config)
    print("מחובר. מתחיל שליפה...\n")

    PAGE_SIZE   = 100
    batch:      list[dict] = []
    total_written           = 0
    pages_fetched           = 0
    newest_seen: Optional[str] = None
    oldest_seen: Optional[str] = None

    try:
        for page in range(1, 99_999):
            items = _ifat_fetch_page(bpage, token, page=page, page_size=PAGE_SIZE)
            if not items:
                print("  ← אין עוד כתבות ב-API.")
                break

            pages_fetched += 1
            in_range_this_page = 0
            past_range         = False

            for item in items:
                pub = (item.get("publishdate", "") or "")[:19]
                try:
                    item_dt = datetime.fromisoformat(pub).date()
                except Exception:
                    continue

                if item_dt > to_dt:
                    continue

                if item_dt < from_dt:
                    past_range = True
                    break

                art = _api_item_to_dict(item, source_index=source_index,
                                        language_index=_language_index_cache or {})
                enrich(art, characters)
                art["pub_type"] = _detect_pub_type(art)
                art["topic"]    = _detect_topic(art)
                batch.append(art)
                in_range_this_page += 1

                date_str = item_dt.strftime("%d/%m/%Y")
                if newest_seen is None:
                    newest_seen = date_str
                oldest_seen = date_str

            print(
                f"  דף {page:4d} | בטווח: {in_range_this_page:3d} | "
                f"אצווה: {len(batch):4d} | "
                f"סה\"כ נכתב: {total_written:5d} | "
                f"תאריך אחרון: {oldest_seen or '—'}"
            )

            if len(batch) >= write_batch_size:
                print(f"\n  → כותב {len(batch)} כתבות לטאב '{archive_sheet}'...")
                append_to_sheet(batch, archive_cfg)
                total_written += len(batch)
                batch = []
                print(f"  סה\"כ נכתב עד כה: {total_written}\n")

            if past_range:
                print(f"\n  ← הגענו לפני {from_date_str} — עוצרים.")
                break

            if len(items) < PAGE_SIZE:
                print("  ← דף חלקי — סוף הנתונים ב-API.")
                break
    finally:
        browser.close()
        pw.stop()

    # Write whatever remains in the last batch
    if batch:
        print(f"\n  → כותב {len(batch)} כתבות אחרונות לטאב '{archive_sheet}'...")
        append_to_sheet(batch, archive_cfg)
        total_written += len(batch)

    print(f"\n{'='*60}")
    print(f"  הסתיים!  סה\"כ {total_written} כתבות נכתבו לטאב '{archive_sheet}'")
    print(f"  דפים שנסרקו: {pages_fetched}")
    print(f"  טווח תאריכים שנכתב: {oldest_seen or '—'} → {newest_seen or '—'}")
    print(f"{'='*60}\n")
    return total_written


# ============================================================
# Backfill Drive links for existing sheet rows
# ============================================================

def backfill_drive_links(config: dict, sheet_names: list[str] | None = None):
    """
    Scan existing sheet rows and upload media to Google Drive for any row
    whose link (col I) does not already point to drive.google.com.

    - עיתונות: calls GetItemClips(serial) → uploads JPG
    - רדיו/טלוויזיה: the existing link IS the shareUrl → extracts MP3/MP4 → uploads

    Updates the sheet cell in-place as each upload completes.
    """
    import time as _time

    _, sh = _get_spreadsheet(config)

    if sheet_names is None:
        # default: main + peace + archive
        sheet_names = [
            config.get("sheet_name",         "עומדים ביחד פרסומים"),
            config.get("peace_sheet_name",    "שלום ישראלי פלסטיני"),
            config.get("archive_sheet_name",  "ארכיון"),
        ]

    drive_service = _get_drive_service(config)

    def _relogin():
        nonlocal pw, browser, bpage, token
        try:
            browser.close()
            pw.stop()
        except Exception:
            pass
        print("  [token] מתחבר מחדש ל-IFAT...")
        pw, browser, bpage, token = _ifat_browser_login(config)
        print("  [token] token חודש בהצלחה")

    # Login to IFAT once for the bearer token (needed by GetItemClips)
    print("מתחבר ל-IFAT לקבלת token...")
    pw, browser, bpage, token = _ifat_browser_login(config)

    total_done = total_skipped = total_failed = 0

    try:
        for sname in sheet_names:
            try:
                ws = sh.worksheet(sname)
            except Exception:
                print(f"  [דילוג] הטאב '{sname}' לא נמצא")
                continue

            rows = ws.get_all_values()
            if not rows:
                continue
            header = rows[0]
            data   = rows[1:]   # 0-indexed; sheet row = i+2

            # Find rows that need backfill
            candidates = []
            for i, row in enumerate(data):
                media = row[11].strip() if len(row) > 11 else ""
                link  = row[8].strip()  if len(row) > 8  else ""
                serial = row[9].strip() if len(row) > 9  else ""
                date  = row[0].strip()  if len(row) > 0  else ""
                source = row[2].strip() if len(row) > 2  else ""

                if media not in ("עיתונות", "רדיו", "טלוויזיה"):
                    continue
                if "drive.google.com" in link:
                    continue        # already uploaded
                if not link and not serial:
                    continue        # nothing to work with

                candidates.append({
                    "sheet_row": i + 2,   # 1-based, +1 for header
                    "media":  media,
                    "link":   link,
                    "serial": serial,
                    "date":   _normalize_date(date),
                    "source": source,
                })

            print(f"\nטאב '{sname}': {len(candidates)} שורות לעדכון")

            for c in candidates:
                art = {"date": c["date"], "media": c["media"],
                       "serial": c["serial"], "source": c["source"]}
                drive_link = ""

                try:
                    if c["media"] == "עיתונות":
                        if not c["serial"]:
                            total_skipped += 1
                            continue
                        try:
                            clips = _get_item_clips(token, c["serial"])
                        except Exception as clip_err:
                            if "401" in str(clip_err):
                                _relogin()
                                clips = _get_item_clips(token, c["serial"])
                            else:
                                raise
                        if not clips:
                            print(f"  [ריק] אין קליפים לסריאל {c['serial']} — {c['source']}")
                            total_skipped += 1
                            continue
                        jpg_url = clips[0].get("url", "")
                        if not jpg_url:
                            total_skipped += 1
                            continue
                        drive_link = _upload_clip_jpg_to_drive(jpg_url, art, config, drive_service)

                    else:  # רדיו / טלוויזיה
                        raw_link = c["link"]
                        if not raw_link:
                            total_skipped += 1
                            continue
                        media_url, media_ext = _resolve_media_url_and_ext(raw_link)
                        if not media_url:
                            print(f"  [ריק] לא נמצא MP3/MP4 ב: {raw_link[:70]}")
                            total_skipped += 1
                            continue
                        drive_link = _upload_media_to_drive(media_url, art, config, drive_service, ext=media_ext)

                    if drive_link:
                        ws.update_cell(c["sheet_row"], 9, drive_link)  # col I = 9
                        ext = "JPG" if c["media"] == "עיתונות" else "MP3/MP4"
                        print(f"  ✓ {c['date']} | {c['source'][:30]} | {ext} → Drive")
                        total_done += 1
                        _time.sleep(0.3)   # gentle rate-limit on Sheets API
                    else:
                        total_skipped += 1

                except Exception as e:
                    print(f"  [שגיאה] שורה {c['sheet_row']} ({c['source']}): {e}")
                    total_failed += 1

    finally:
        browser.close()
        pw.stop()

    print(f"\n{'='*60}")
    print(f"  הושלם Backfill: {total_done} הועלו, {total_skipped} דולגו, {total_failed} שגיאות")
    print(f"{'='*60}\n")


# ============================================================
# Main
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="עיבוד PDFs מיפעת → Google Sheets")
    parser.add_argument("--fetch-api",     action="store_true",
                        help="משוך כתבות מ-API יפעת ישירות והכנס לגיליון (ברירת מחדל: אתמול)")
    parser.add_argument("--date",          metavar="DD/MM/YYYY",
                        help="תאריך לשליפה עבור --fetch-api (ברירת מחדל: אתמול)")
    parser.add_argument("--archive",       action="store_true",
                        help="שלוף ארכיון היסטורי מ-API יפעת וכתוב לטאב 'ארכיון' (פעולה חד-פעמית)")
    parser.add_argument("--from-date",     metavar="DD/MM/YYYY", default="01/01/2020",
                        help="תאריך התחלה לארכיון (ברירת מחדל: 01/01/2020)")
    parser.add_argument("--to-date",       metavar="DD/MM/YYYY",
                        help="תאריך סיום לארכיון (ברירת מחדל: אתמול)")
    parser.add_argument("--backfill",      action="store_true",
                        help="עבור על כל שורות הגיליון ועלה לDrive קבצים שעדיין מצביעים לשרתי יפעת")
    parser.add_argument("--sheets",        metavar="SHEET", nargs="+",
                        help="טאבים לעדכון בעת --backfill (ברירת מחדל: שלושת הטאבים)")
    args = parser.parse_args()

    config     = load_config()
    characters = load_characters()

    if args.backfill:
        if "ifat_username" not in config or "ifat_password" not in config:
            print("שגיאה: חסרים ifat_username / ifat_password ב-ifat_config.json")
            return
        backfill_drive_links(config, sheet_names=args.sheets)

    elif args.archive:
        if "ifat_username" not in config or "ifat_password" not in config:
            print("שגיאה: חסרים ifat_username / ifat_password ב-ifat_config.json")
            return
        fetch_archive_range(
            config,
            characters,
            from_date_str=args.from_date,
            to_date_str=args.to_date,   # None → ברירת מחדל: אתמול
        )

    elif args.fetch_api:
        if "ifat_username" not in config or "ifat_password" not in config:
            print("שגיאה: חסרים ifat_username / ifat_password ב-ifat_config.json")
            return
        if args.date:
            # תאריך ספציפי שצוין — שולף רק אותו
            dates_to_fetch = [args.date]
        else:
            # ברירת מחדל: אתמול + היום (בוקר רדיו / עיתונות מודפסת של הבוקר)
            from datetime import timedelta
            today_str     = datetime.now().strftime("%d/%m/%Y")
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
            dates_to_fetch = [yesterday_str, today_str]

        main_articles: list = []
        peace_articles: list = []
        for fetch_date in dates_to_fetch:
            m, p = fetch_api_articles(config, characters, target_date=fetch_date)
            main_articles.extend(m)
            peace_articles.extend(p)

        peace_sheet = config.get("peace_sheet_name", "שלום ישראלי פלסטיני")

        append_to_sheet(main_articles, config)
        if peace_articles:
            print(f"\nמוסיף {len(peace_articles)} כתבות לטאב '{peace_sheet}'...")
            append_to_sheet(peace_articles, config, sheet_name=peace_sheet)
        print("הושלם.")

if __name__ == "__main__":
    main()
