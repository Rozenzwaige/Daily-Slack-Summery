"""One-time script: clears old transactions and re-imports with signed amounts."""
import json
from pathlib import Path
import sheets

svc = sheets.open_spreadsheet()

# Clear everything in the transactions sheet
svc.values().clear(
    spreadsheetId=sheets.SPREADSHEET_ID,
    range=sheets.TXN_SHEET,
).execute()
print("Cleared old transactions")

# Re-write headers with new סוג column
svc.values().update(
    spreadsheetId=sheets.SPREADSHEET_ID,
    range=f"'{sheets.TXN_SHEET}'!A1",
    valueInputOption="RAW",
    body={"values": [sheets.TXN_HEADERS]},
).execute()
print("Headers:", sheets.TXN_HEADERS)
print("Done — run processor.py to re-import transactions")
