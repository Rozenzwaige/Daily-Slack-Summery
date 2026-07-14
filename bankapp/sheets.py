"""
Google Sheets integration.
Spreadsheet: https://docs.google.com/spreadsheets/d/1FgKHWqo8_C7VHT9VIckKhUsW27e07WBAIN5iNuC7LDg

Sheet tabs:
  עסקאות  — all transactions (was Sheet1, renamed by setup_sheet.py)
  תקציב   — monthly budgets per category (user edits manually)
  יתרה    — daily balance log
"""

import json
import os
from datetime import datetime, timezone

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SPREADSHEET_ID = os.environ.get(
    "FINANCE_SPREADSHEET_ID",
    "1FgKHWqo8_C7VHT9VIckKhUsW27e07WBAIN5iNuC7LDg",
)

EXPENSE_SHEET  = "עו\"ש הוצאות"
INCOME_SHEET   = "עו\"ש הכנסות"
CREDIT_SHEET   = "אשראי"
BUDGET_SHEET   = "תקציב"
BALANCE_SHEET  = "יתרה"
HISTORY_SHEET  = "היסטוריה"
HOLDINGS_HISTORY_SHEET = "היסטוריה ניירות"

# Keep for backward compat / deduplication lookup
TXN_SHEET = EXPENSE_SHEET

TXN_HEADERS = [
    "txn_id", "תאריך", "עסקה", "סכום", "מטבע",
    "קטגוריה", "חשבון", "סטטוס",
]


def _creds():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    info = json.loads(raw) if raw else json.load(open("credentials.json", encoding="utf-8"))
    return Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )


def open_spreadsheet():
    return build("sheets", "v4", credentials=_creds(), cache_discovery=False).spreadsheets()


def _quoted(sheet: str) -> str:
    return f"'{sheet}'"


def _ensure_sheet_exists(svc, title: str) -> None:
    meta = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    existing = {s["properties"]["title"] for s in meta["sheets"]}
    if title not in existing:
        svc.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()


def _read(svc, sheet: str) -> list[list]:
    r = svc.values().get(spreadsheetId=SPREADSHEET_ID, range=_quoted(sheet)).execute()
    return r.get("values", [])


def _append(svc, sheet: str, rows: list[list]) -> None:
    svc.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{_quoted(sheet)}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def _ensure_sheet_headers(svc, sheet: str) -> None:
    _ensure_sheet_exists(svc, sheet)
    data = _read(svc, sheet)
    if not data or data[0] != TXN_HEADERS:
        svc.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{sheet}'!A1",
            valueInputOption="RAW",
            body={"values": [TXN_HEADERS]},
        ).execute()


def ensure_headers(svc) -> None:
    _ensure_sheet_headers(svc, EXPENSE_SHEET)
    _ensure_sheet_headers(svc, INCOME_SHEET)
    _ensure_sheet_headers(svc, CREDIT_SHEET)
    _ensure_sheet_exists(svc, BUDGET_SHEET)
    _ensure_sheet_exists(svc, BALANCE_SHEET)
    _ensure_sheet_exists(svc, HISTORY_SHEET)


def get_existing_txn_ids(svc) -> set[str]:
    ids = set()
    for sheet in [EXPENSE_SHEET, INCOME_SHEET, CREDIT_SHEET]:
        data = _read(svc, sheet)
        ids |= {row[0] for row in data[1:] if row}
    return ids


def _txn_row(t: dict) -> list:
    return [
        t["txn_id"], t["date"], t["description"],
        t["amount"],
        t.get("currency", "ILS"), t.get("category", "שונות"),
        t.get("account", ""), t.get("status", ""),
    ]


# All credit-card sources (route to the אשראי sheet)
CREDIT_SOURCES = {"cal", "cal2"}


def _route_sheet(t: dict) -> str:
    """Route transaction to the correct sheet."""
    if t.get("source") in CREDIT_SOURCES:
        return CREDIT_SHEET
    return INCOME_SHEET if t["amount"] >= 0 else EXPENSE_SHEET


def append_transactions(svc, txns: list[dict]) -> int:
    if not txns:
        return 0
    buckets: dict[str, list] = {EXPENSE_SHEET: [], INCOME_SHEET: [], CREDIT_SHEET: []}
    for t in txns:
        buckets[_route_sheet(t)].append(_txn_row(t))
    total = 0
    for sheet, rows in buckets.items():
        if rows:
            _append(svc, sheet, rows)
            total += len(rows)
    return total


def get_budgets(svc) -> dict[str, float]:
    budgets: dict[str, float] = {}
    for row in _read(svc, BUDGET_SHEET)[1:]:
        if len(row) >= 2:
            try:
                budgets[row[0].strip()] = float(str(row[1]).replace(",", ""))
            except ValueError:
                pass
    return budgets


def get_month_spending(svc, year: int, month: int) -> dict[str, float]:
    prefix = f"{year:04d}-{month:02d}"
    spending: dict[str, float] = {}
    for row in _read(svc, TXN_SHEET)[1:]:
        if len(row) < 6 or not row[1].startswith(prefix):
            continue
        try:
            amount = float(str(row[3]).replace(",", ""))
        except ValueError:
            continue
        if amount <= 0:
            continue
        cat = row[5] if len(row) > 5 else "שונות"
        spending[cat] = spending.get(cat, 0.0) + amount
    return spending


def log_daily_snapshot(svc, portfolio_value: float, bank_balance: float) -> None:
    """Save today's portfolio + bank balance snapshot (once per day)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data  = _read(svc, HISTORY_SHEET)

    # Check headers
    if not data or data[0] != ["תאריך", "תיק השקעות (₪)", "עו\"ש (₪)"]:
        svc.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{HISTORY_SHEET}'!A1",
            valueInputOption="RAW",
            body={"values": [["תאריך", "תיק השקעות (₪)", "עו\"ש (₪)"]]},
        ).execute()
        data = [["תאריך", "תיק השקעות (₪)", "עו\"ש (₪)"]]

    # Don't duplicate — one entry per day
    existing_dates = {row[0] for row in data[1:] if row}
    if today in existing_dates:
        return

    # Carry forward the last known good value when a scrape failed (value is 0)
    def _last_good(col: int) -> float:
        for row in reversed(data[1:]):
            if len(row) > col:
                try:
                    v = float(str(row[col]).replace(",", ""))
                    if v > 0:
                        return v
                except (ValueError, TypeError):
                    pass
        return 0.0

    if portfolio_value <= 0:
        portfolio_value = _last_good(1)
    if bank_balance <= 0:
        bank_balance = _last_good(2)

    _append(svc, HISTORY_SHEET, [[today, round(portfolio_value, 2), round(bank_balance, 2)]])


def log_holdings_snapshot(svc, holdings: list[dict]) -> None:
    """
    Save per-holding values for today (once per day).
    holdings: list of {"id", "name", "value_ils", "price"}
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _ensure_sheet_exists(svc, HOLDINGS_HISTORY_SHEET)
    data  = _read(svc, HOLDINGS_HISTORY_SHEET)
    headers = ["תאריך", "מספר ני\"ע", "שם", "שווי (₪)", "שער"]

    if not data or data[0] != headers:
        svc.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{HOLDINGS_HISTORY_SHEET}'!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()
        data = [headers]

    existing = {(row[0], row[1]) for row in data[1:] if len(row) >= 2}
    rows = []
    for h in holdings:
        key = (today, str(h["id"]))
        if key in existing:
            continue
        rows.append([today, str(h["id"]), h["name"],
                     round(h["value_ils"], 2), round(h.get("price", 0), 4)])
    if rows:
        _append(svc, HOLDINGS_HISTORY_SHEET, rows)


def get_holding_values_on_or_before(svc, target_date: str) -> dict[str, float]:
    """Return {security_id: value_ils} from the closest snapshot date <= target_date."""
    try:
        data = _read(svc, HOLDINGS_HISTORY_SHEET)
    except Exception:
        return {}
    # Find the best (latest) date <= target
    best_date = ""
    for row in data[1:]:
        if len(row) < 4:
            continue
        d = str(row[0])[:10]
        if d <= target_date and d > best_date:
            best_date = d
    if not best_date:
        return {}
    result = {}
    for row in data[1:]:
        if len(row) < 4 or str(row[0])[:10] != best_date:
            continue
        try:
            result[str(row[1])] = float(str(row[3]).replace(",", ""))
        except (ValueError, TypeError):
            pass
    return result


def get_snapshot_on_or_before(svc, target_date: str) -> tuple[float, float]:
    """
    Return (portfolio_value, bank_balance) for dates <= target_date.
    Each column is taken from its latest non-zero snapshot independently, so a
    failed scrape (stored as 0) on a given day doesn't blank out the value.
    """
    data = _read(svc, HISTORY_SHEET)
    rows = []
    for row in data[1:]:
        if len(row) < 3:
            continue
        d = str(row[0])[:10]
        if d > target_date:
            continue
        try:
            p = float(str(row[1]).replace(",", ""))
            b = float(str(row[2]).replace(",", ""))
        except (ValueError, TypeError):
            continue
        rows.append((d, p, b))
    rows.sort(key=lambda r: r[0])

    best_portfolio = next((p for d, p, b in reversed(rows) if p > 0), 0.0)
    best_bank      = next((b for d, p, b in reversed(rows) if b > 0), 0.0)
    return best_portfolio, best_bank


def log_balance(svc, source: str, account: str, balance: float) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    data = _read(svc, BALANCE_SHEET)
    if not data:
        svc.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{_quoted(BALANCE_SHEET)}!A1",
            valueInputOption="RAW",
            body={"values": [["תאריך", "מקור", "חשבון", "יתרה"]]},
        ).execute()
    _append(svc, BALANCE_SHEET, [[now, source, account, balance]])


def get_recent_rows(svc, days: int = 7) -> list[dict]:
    """Return recent expense/credit rows as dicts for cross-day fuzzy dedup."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    result = []
    for sheet in [EXPENSE_SHEET, CREDIT_SHEET]:
        data = _read(svc, sheet)
        if not data or len(data) < 2:
            continue
        headers = data[0]
        date_col = next((i for i,h in enumerate(headers) if "תאריך" in str(h)), None)
        amt_col  = next((i for i,h in enumerate(headers) if "סכום" in str(h)), None)
        desc_col = next((i for i,h in enumerate(headers) if "עסקה" in str(h) or "תיאור" in str(h)), None)
        src_col  = next((i for i,h in enumerate(headers) if "חשבון" in str(h)), None)
        if date_col is None or amt_col is None:
            continue
        for row in data[1:]:
            d = str(row[date_col])[:10] if date_col < len(row) else ""
            if d < cutoff:
                continue
            try:
                amt = float(str(row[amt_col]).replace(",", ""))
            except (ValueError, TypeError):
                continue
            result.append({
                "date":    d,
                "amount":  amt,
                "desc":    str(row[desc_col]) if desc_col is not None and desc_col < len(row) else "",
                "account": str(row[src_col]) if src_col is not None and src_col < len(row) else "",
            })
    return result
