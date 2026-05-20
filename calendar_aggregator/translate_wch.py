"""One-time script: translate WCH column B (English titles) → column E (Hebrew).

Run once from the project root:
    python calendar_aggregator/translate_wch.py

Uses the same credentials + sheet ID as the daily aggregator.
No API key required — uses Google Translate unofficial endpoint via requests.

After this runs, the GAS calendar webapp reads translated titles from column E.
"""

import os
import sys
import time
import requests

sys.path.insert(0, os.path.dirname(__file__))

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_ID   = os.environ.get("CALENDAR_SHEET_ID")
CREDS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "calendar_aggregator/credentials.json")
TAB_WCH    = "WCH"
CHUNK      = 100      # titles per translation request
DELAY      = 0.6      # seconds between requests


def translate_batch(texts: list[str]) -> list[str]:
    """Translate a list of English strings to Hebrew in one HTTP call."""
    params = [("client", "dict-chrome-ex"), ("sl", "en"), ("tl", "iw")]
    for t in texts:
        params.append(("q", t))
    resp = requests.get(
        "https://translate.googleapis.com/translate_a/t",
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    # Response: [["he1"], ["he2"], ...] or ["he1", "he2", ...]
    results = []
    for item in data:
        if isinstance(item, list):
            results.append(item[0] if item else "")
        else:
            results.append(str(item))
    return results


def main():
    if not SHEET_ID:
        print("ERROR: CALENDAR_SHEET_ID env var not set")
        sys.exit(1)

    creds  = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    ss     = client.open_by_key(SHEET_ID)
    ws     = ss.worksheet(TAB_WCH)

    print("Reading WCH sheet…")
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        print("Sheet is empty.")
        return

    # Ensure column E header
    if len(all_rows[0]) < 5 or not all_rows[0][4]:
        ws.update_cell(1, 5, "כותרת עברית")
        print("Added column E header")

    # Find rows that need translation (col B not empty, col E empty)
    todo: list[tuple[int, str]] = []   # (1-indexed row number, english title)
    for i, row in enumerate(all_rows[1:], start=2):
        en = row[1].strip() if len(row) > 1 else ""
        he = row[4].strip() if len(row) > 4 else ""
        if en and not he:
            todo.append((i, en))

    print(f"Rows to translate: {len(todo)}")
    if not todo:
        print("All rows already translated.")
        return

    # Translate in batches and collect all results
    translations: dict[int, str] = {}   # row_number → translated title
    for batch_start in range(0, len(todo), CHUNK):
        batch      = todo[batch_start : batch_start + CHUNK]
        row_nums   = [r[0] for r in batch]
        texts      = [r[1] for r in batch]
        done_so_far = batch_start
        try:
            results = translate_batch(texts)
            for row_num, title_he in zip(row_nums, results):
                translations[row_num] = title_he
            done_so_far = batch_start + len(batch)
            print(f"  Translated {done_so_far}/{len(todo)}")
        except Exception as e:
            print(f"  Batch error at {batch_start}: {e}")
        time.sleep(DELAY)

    if not translations:
        print("No translations produced — check network access.")
        return

    # Build updated column E (all rows, preserving existing values)
    col_e: list[list[str]] = [["כותרת עברית"]]
    for i, row in enumerate(all_rows[1:], start=2):
        existing = row[4].strip() if len(row) > 4 else ""
        col_e.append([translations.get(i, existing)])

    # Write entire column E in one call
    print("Writing translations to sheet…")
    ws.update(f"E1:E{len(col_e)}", col_e)
    print(f"Done — {len(translations)} rows translated and saved.")


if __name__ == "__main__":
    main()
