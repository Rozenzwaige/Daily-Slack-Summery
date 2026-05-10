"""Google Sheets integration — create tabs and rewrite event rows."""

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
            ws = sh.add_worksheet(title=tab, rows=1000, cols=5)
            ws.append_row(HEADERS, value_input_option="RAW")
            ws.format("A1:E1", _HEADER_FORMAT)
            print(f"[sheets] created tab '{tab}'")


def update_tab(tab_name: str, rows: list[list]):
    """Clear a tab and write fresh rows (header + data), sorted by date.

    If rows is empty the tab is left untouched so that a temporary scraper
    failure doesn't wipe out the previous day's data.
    """
    if not rows:
        print(f"[sheets] '{tab_name}': 0 rows — keeping existing data")
        return
    sh = _get_sheet()
    _ensure_tabs(sh)
    ws = sh.worksheet(tab_name)
    ws.clear()
    ws.append_row(HEADERS, value_input_option="RAW")
    ws.format("A1:E1", _HEADER_FORMAT)
    if rows:
        # Sort by date DD/MM/YYYY then time HH:MM
        def _sort_key(r):
            try:
                d = r[0]  # DD/MM/YYYY
                day, month, year = d.split("/")
                return f"{year}{month}{day}{r[1]}"
            except Exception:
                return r[0]

        rows.sort(key=_sort_key)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"[sheets] '{tab_name}': {len(rows)} rows written")
