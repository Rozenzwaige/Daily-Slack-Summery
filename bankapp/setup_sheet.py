#!/usr/bin/env python3
"""
One-time setup: renames Sheet1 → עסקאות, creates תקציב and יתרה tabs,
and fills default budgets.

Run once locally:
  pip install -r requirements.txt
  python setup_sheet.py
Requires: credentials.json in this folder (or GOOGLE_CREDENTIALS_JSON env var).
"""

import json
import os
import sys

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SPREADSHEET_ID = os.environ.get(
    "FINANCE_SPREADSHEET_ID",
    "1FgKHWqo8_C7VHT9VIckKhUsW27e07WBAIN5iNuC7LDg",
)

DEFAULT_BUDGETS = [
    ["קטגוריה",          "תקציב חודשי (₪)"],
    ["מזון וסופרמרקט",   3000],
    ["מסעדות וקפה",      1500],
    ["תחבורה ודלק",      800],
    ["פארם ובריאות",     500],
    ["ביגוד",            500],
    ["חינוך וילדים",     1000],
    ["בילוי ופנאי",      600],
    ["תשלומים קבועים",   3000],
    ["שונות",            500],
]


def main():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    info = json.loads(raw) if raw else json.load(open("credentials.json", encoding="utf-8"))
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False).spreadsheets()

    meta     = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}
    print(f"Existing sheets: {list(existing.keys())}")

    requests = []

    # Rename Sheet1 → עסקאות if needed
    if "Sheet1" in existing and "עסקאות" not in existing:
        requests.append({"updateSheetProperties": {
            "properties": {"sheetId": existing["Sheet1"], "title": "עסקאות"},
            "fields": "title",
        }})

    for title in ["תקציב", "יתרה"]:
        if title not in existing:
            requests.append({"addSheet": {"properties": {"title": title}}})

    if requests:
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
        print("✅ Sheets created/renamed")

    # Headers for עסקאות
    svc.values().update(
        spreadsheetId=SPREADSHEET_ID, range="עסקאות!A1",
        valueInputOption="RAW",
        body={"values": [["txn_id", "תאריך", "עסקה", "סכום", "מטבע", "קטגוריה", "חשבון", "מקור", "סטטוס"]]},
    ).execute()

    # Default budgets for תקציב
    svc.values().update(
        spreadsheetId=SPREADSHEET_ID, range="תקציב!A1",
        valueInputOption="USER_ENTERED",
        body={"values": DEFAULT_BUDGETS},
    ).execute()

    # Headers for יתרה
    svc.values().update(
        spreadsheetId=SPREADSHEET_ID, range="יתרה!A1",
        valueInputOption="RAW",
        body={"values": [["תאריך", "מקור", "חשבון", "יתרה"]]},
    ).execute()

    print(f"\n✅ Done! Open your sheet:")
    print(f"   https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print("\n📝 Edit the 'תקציב' tab to set your monthly budgets.")


if __name__ == "__main__":
    main()
