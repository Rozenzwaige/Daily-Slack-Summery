import os

SPREADSHEET_ID = os.getenv("CALENDAR_SHEET_ID", "1y9jz0HfuTA7E2El74iduuV8zvornxw6jE4FNoHNUt-I")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "calendar_aggregator/credentials.json")

TAB_KNESSET    = "כנסת"
TAB_GOVERNMENT = "ממשלה"
TAB_COURTS     = "בתי משפט"
TAB_HOLIDAYS   = "חגים"

ALL_TABS = [TAB_KNESSET, TAB_GOVERNMENT, TAB_COURTS, TAB_HOLIDAYS]
HEADERS  = ["תאריך", "שעה", "אירוע", "תיאור", "קישור"]

DAYS_AHEAD = 7
