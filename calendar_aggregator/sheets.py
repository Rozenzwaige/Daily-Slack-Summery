"""Google Sheets integration — append-only event rows with deduplication."""

import gspread
from google.oauth2.service_account import Credentials
from config import SPREADSHEET_ID, CREDENTIALS_FILE, ALL_TABS, HEADERS

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_HEADER_FORMAT = {
    "textFormat": {"bold": True},
    "backgroundColor": {"red": 0.88, "green": 0.88, "blue": 0.88},
}


def _get_sheet():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def _ensure_tabs(sh):
    existing = {ws.title for ws in sh.worksheets()}
    for tab in ALL_TABS:
        if tab not in existing:
            ws = sh.add_worksheet(title=tab, rows=2000, cols=5)
            ws.append_row(HEADERS, value_input_option="RAW")
            ws.format("A1:E1", _HEADER_FORMAT)
            print(f"[sheets] created tab '{tab}'")


def _dedup_key(row: list) -> tuple:
    """Stable identity for a row: (date, time, event-name)."""
    return (
        row[0] if len(row) > 0 else "",
        row[1] if len(row) > 1 else "",
        row[2] if len(row) > 2 else "",
    )


def update_tab(tab_name: str, rows: list[list]):
    """Append only rows that don't already exist in the tab.

    Existing data is never deleted — the tab grows over time.
    Deduplication key: (date, time, event-name) — columns A, B, C.
    """
    if not rows:
        print(f"[sheets] '{tab_name}': 0 rows — nothing to add")
        return

    sh = _get_sheet()
    _ensure_tabs(sh)
    ws = sh.worksheet(tab_name)

    # Read all existing rows (skip header)
    existing_values = ws.get_all_values()
    existing_keys = {
        _dedup_key(r)
        for r in existing_values[1:]   # skip header row
        if r
    }

    # Filter to only truly new rows
    new_rows = [r for r in rows if _dedup_key(r) not in existing_keys]

    if not new_rows:
        print(f"[sheets] '{tab_name}': all {len(rows)} rows already present — nothing added")
        return

    # Sort new rows by date + time before appending
    def _sort_key(r):
        try:
            day, month, year = r[0].split("/")
            return f"{year}{month}{day}{r[1]}"
        except Exception:
            return r[0]

    new_rows.sort(key=_sort_key)
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    print(f"[sheets] '{tab_name}': appended {len(new_rows)} new rows ({len(rows) - len(new_rows)} duplicates skipped)")
