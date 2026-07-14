#!/usr/bin/env python3
"""
Portfolio tracker — Israeli mutual funds + foreign stocks.
- Fetches daily prices from TASE API and Yahoo Finance
- Updates the "בורסה" Google Sheet tab
- Sends a Thursday WhatsApp report with AI analysis via Claude
"""

import os
import glob
from datetime import datetime, timezone, date
import requests
import yfinance as yf
import anthropic

import sheets
import whatsapp

PORTFOLIO_SHEET = "בורסה"

MAYA_API = "https://maya.tase.co.il/api/v1/funds/mutual/{fund_id}"
MAYA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://maya.tase.co.il/",
}

PORTFOLIO_HEADERS = [
    'שם ני"ע', "מספר ני\"ע", "סימבול", "כמות", "שער עלות (₪/$)",
    "שער נוכחי", "מטבע", "שווי נוכחי (₪)", "רווח/הפסד (₪)",
    "רווח/הפסד (%)", "שינוי יומי (₪)", "שינוי יומי (%)",
    "אחוז מהתיק", "תאריך רכישה", "עדכון אחרון",
]

# ─── Holdings (from Excel export 03/06/2026) ──────────────────────────────────
HOLDINGS = [
    # Israeli funds: cost in ₪ per unit (converted from agorots by /100)
    {"name": 'AI אילים מניות',       "id": "5137534",    "symbol": None,   "qty": 3147,  "cost": 1.6136,  "currency": "ILS", "buy_date": "14.09.2025"},
    {"name": 'S&P 500 אלטש',         "id": "5105531",    "symbol": None,   "qty": 8650,  "cost": 3.3952,  "currency": "ILS", "buy_date": "25.06.2024"},
    {"name": 'מיטב אג עד5% מנ',     "id": "5126008",    "symbol": None,   "qty": 8030,  "cost": 1.2447,  "currency": "ILS", "buy_date": "02.06.2026"},
    {"name": '125 מיטב ת"א',         "id": "5100474",    "symbol": None,   "qty": 957,   "cost": 15.1198, "currency": "ILS", "buy_date": "02.06.2026"},
    {"name": 'קא ענק טכנ חול',      "id": "5100151",    "symbol": None,   "qty": 3206,  "cost": 4.7966,  "currency": "ILS", "buy_date": "02.06.2026"},
    {"name": 'תמיר עקב 3 בינל',     "id": "5127121",    "symbol": None,   "qty": 12684, "cost": 1.5775,  "currency": "ILS", "buy_date": "14.09.2025"},
    {"name": 'אגם מדינ FOREST',      "id": "5106562",    "symbol": None,   "qty": 17600, "cost": 1.6945,  "currency": "ILS", "buy_date": "25.06.2024"},
    # Foreign stocks: cost in USD per share
    {"name": 'ALPHABET INC - CL C',  "id": "5171349906", "symbol": "GOOG", "qty": 13,    "cost": 313.16,  "currency": "USD", "buy_date": "10.04.2026"},
]

# ─── Price fetching ───────────────────────────────────────────────────────────

def get_usd_ils() -> float:
    try:
        return float(yf.Ticker("USDILS=X").fast_info.last_price)
    except Exception:
        return float(os.environ.get("USD_TO_ILS", "3.65"))


def get_maya_price(fund_id: str) -> tuple[float | None, float | None]:
    """Fetch current NAV price from MAYA API. Returns (price, daily_change_pct)."""
    try:
        r = requests.get(MAYA_API.format(fund_id=fund_id), headers=MAYA_HEADERS, timeout=10)
        if not r.ok:
            return None, None
        d = r.json()
        price = d.get("redemptionPrice") or d.get("purchasePrice")
        yields = d.get("yields") or {}
        daily_pct = yields.get("dailyYield")
        # Israeli fund prices are in agorots (1/100 shekel) — convert to shekels
        return (float(price) / 100 if price else None), (float(daily_pct) if daily_pct else None)
    except Exception as e:
        print(f"  MAYA API error for {fund_id}: {e}")
        return None, None


def get_yahoo_price(symbol: str, usd_ils: float) -> tuple[float | None, float | None, float | None, float | None]:
    """Returns (price_usd, price_ils, daily_change_ils, daily_change_pct)."""
    try:
        info = yf.Ticker(symbol).fast_info
        price = float(info.last_price)
        prev  = float(info.previous_close)
        chg   = price - prev
        chg_pct = (chg / prev * 100) if prev else 0
        return price, price * usd_ils, chg * usd_ils, chg_pct
    except Exception as e:
        print(f"  Yahoo Finance error for {symbol}: {e}")
    return None, None, None, None


# ─── Sheet helpers ────────────────────────────────────────────────────────────

def _ensure_portfolio_sheet(svc) -> None:
    sheets._ensure_sheet_exists(svc, PORTFOLIO_SHEET)
    data = sheets._read(svc, PORTFOLIO_SHEET)
    if not data or data[0] != PORTFOLIO_HEADERS:
        svc.values().update(
            spreadsheetId=sheets.SPREADSHEET_ID,
            range=f"'{PORTFOLIO_SHEET}'!A1",
            valueInputOption="RAW",
            body={"values": [PORTFOLIO_HEADERS]},
        ).execute()


def _get_portfolio_rows(svc) -> dict[str, int]:
    """Returns {security_id: row_number (1-indexed)}."""
    data = sheets._read(svc, PORTFOLIO_SHEET)
    mapping = {}
    for i, row in enumerate(data[1:], start=2):
        if len(row) > 1 and row[1]:
            mapping[str(row[1])] = i
    return mapping


def _write_row(svc, row_num: int, values: list) -> None:
    svc.values().update(
        spreadsheetId=sheets.SPREADSHEET_ID,
        range=f"'{PORTFOLIO_SHEET}'!A{row_num}",
        valueInputOption="USER_ENTERED",
        body={"values": [values]},
    ).execute()


def _append_portfolio_row(svc, values: list) -> None:
    svc.values().append(
        spreadsheetId=sheets.SPREADSHEET_ID,
        range=f"'{PORTFOLIO_SHEET}'!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [values]},
    ).execute()


# ─── Main update ─────────────────────────────────────────────────────────────

def update_portfolio(svc) -> tuple:
    print("\n📈 Updating portfolio...")
    _ensure_portfolio_sheet(svc)

    usd_ils = get_usd_ils()
    print(f"  USD/ILS: {usd_ils:.3f}")

    # Israeli fund prices come from MAYA API (public, no auth needed)

    existing_rows = _get_portfolio_rows(svc)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    enriched = []
    for h in HOLDINGS:
        current_ils = None
        daily_chg_ils = None
        daily_chg_pct = None

        if h["symbol"]:  # Foreign stock — Yahoo Finance
            price_usd, price_ils, chg_ils, chg_pct = get_yahoo_price(h["symbol"], usd_ils)
            if price_usd:
                current_ils = price_ils
                daily_chg_ils = chg_ils * h["qty"] if chg_ils else None
                daily_chg_pct = chg_pct
                print(f"  {h['name']}: ${price_usd:.2f} = ₪{price_ils:.2f} ({chg_pct:+.2f}%)")
        else:  # Israeli fund — MAYA API
            price, daily_pct = get_maya_price(h["id"])
            if price:
                current_ils = price
                daily_chg_pct = daily_pct
                daily_chg_ils = None
                chg_str = f"{daily_pct:+.2f}%" if daily_pct is not None else "n/a"
                print(f"  {h['name']}: ₪{price:.2f} ({chg_str})")
            else:
                print(f"  {h['name']}: MAYA API failed")

        enriched.append({**h, "current_ils": current_ils, "daily_chg_ils": daily_chg_ils, "daily_chg_pct": daily_chg_pct})

    # Calculate portfolio total
    total_value = sum(
        (e["current_ils"] or 0) * e["qty"] * (usd_ils if e["currency"] == "USD" and e["symbol"] else 1)
        for e in enriched if e["current_ils"]
    )
    # Correct: for ILS funds current_ils is already the per-unit ILS price
    # For USD stocks, current_ils is already converted
    total_value = sum(
        (e["current_ils"] * e["qty"]) for e in enriched if e["current_ils"]
    )

    for e in enriched:
        if not e["current_ils"]:
            continue

        current = e["current_ils"]
        qty     = e["qty"]
        cost    = e["cost"]
        cur     = e["currency"]

        value_ils   = current * qty
        cost_ils    = cost * qty * (usd_ils if cur == "USD" else 1)
        pnl_ils     = value_ils - cost_ils
        pnl_pct     = (pnl_ils / cost_ils * 100) if cost_ils else 0
        pct_of_port = (value_ils / total_value * 100) if total_value else 0

        row = [
            e["name"],
            e["id"],
            e["symbol"] or "",
            qty,
            round(cost, 2),
            round(current, 2),
            "₪" if cur == "ILS" else "$",
            round(value_ils, 2),
            round(pnl_ils, 2),
            round(pnl_pct, 2),
            round(e["daily_chg_ils"], 2) if e["daily_chg_ils"] is not None else "",
            round(e["daily_chg_pct"], 2) if e["daily_chg_pct"] is not None else "",
            round(pct_of_port, 2),
            e["buy_date"],
            now_str,
        ]

        if e["id"] in existing_rows:
            _write_row(svc, existing_rows[e["id"]], row)
        else:
            _append_portfolio_row(svc, row)

    print(f"✅ Portfolio updated. Total value: ₪{total_value:,.0f}")
    return enriched, total_value, usd_ils


# ─── Thursday WhatsApp report ─────────────────────────────────────────────────

def _build_portfolio_text(enriched: list, total_value: float, usd_ils: float) -> str:
    lines = []
    for e in enriched:
        if not e["current_ils"]:
            continue
        value = e["current_ils"] * e["qty"]
        cost  = e["cost"] * e["qty"] * (usd_ils if e["currency"] == "USD" else 1)
        pnl_pct = ((value - cost) / cost * 100) if cost else 0
        day_pct = e["daily_chg_pct"] or 0
        lines.append(
            f"- {e['name']} ({e['id']}): ₪{value:,.0f} | "
            f"מתחרכישה: {pnl_pct:+.1f}% | יומי: {day_pct:+.2f}%"
        )
    return "\n".join(lines)


def send_thursday_report(svc) -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("⚠️  ANTHROPIC_API_KEY not set — skipping Thursday report")
        return

    print("\n📋 Building Thursday portfolio report...")
    enriched, total_value, usd_ils = update_portfolio(svc)
    portfolio_text = _build_portfolio_text(enriched, total_value, usd_ils)

    client = anthropic.Anthropic(api_key=api_key)

    prompt = f"""אתה יועץ השקעות ישראלי המנתח תיק השקעות שבועי.

נתוני התיק (נכון להיום):
שווי כולל: ₪{total_value:,.0f}
שער דולר: ₪{usd_ils:.3f}

פירוט אחזקות:
{portfolio_text}

אנא כתוב בעברית דוח קצר ומעשי הכולל:

1. *ביצועי התיק השבוע* — מי עלה, מי ירד, מה בלט
2. *חדשות רלוונטיות* — האם ידוע על דוחות רווחים, הודעות חשובות של החברות/הקרנות (בעיקר Alphabet/GOOG, קרנות מחקות S&P 500 ות"א 125)
3. *2 המלצות שכדאי לשקול* — מניות או קרנות ספציפיות עם הסבר קצר למה
4. *המלצת אופטימיזציה לתיק* — בהתבסס על הרכב התיק הנוכחי, מה כדאי לשקול לשנות

פורמט לווטסאפ — ללא markdown, השתמש באמוג'י. היה קצר וממוקד."""

    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}]
    )

    report = response.content[0].text
    whatsapp.send(f"📊 *דוח שבועי — תיק ההשקעות*\n\n{report}")
    print("✅ Thursday report sent to WhatsApp")


# ─── Entry point ──────────────────────────────────────────────────────────────

def run(svc) -> float:
    """Update portfolio prices + save per-holding snapshot. Returns total ILS value."""
    result = update_portfolio(svc)
    if not (isinstance(result, tuple) and len(result) >= 3):
        return 0.0
    enriched, total_value, usd_ils = result

    # Save per-holding snapshot for weekly/monthly per-asset change tracking
    holdings = []
    for e in enriched:
        if not e.get("current_ils"):
            continue
        holdings.append({
            "id":        e["id"],
            "name":      e["name"],
            "value_ils": e["current_ils"] * e["qty"],
            "price":     e["current_ils"],
        })
    if holdings:
        sheets.log_holdings_snapshot(svc, holdings)

    return total_value
    # Reports are sent by cron-job.org → GitHub Actions (reports.yml)
