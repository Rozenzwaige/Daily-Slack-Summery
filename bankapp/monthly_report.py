#!/usr/bin/env python3
"""
Monthly report — sent on the 1st of each month at 08:00:
  1. Spending summary for last month with Claude analysis (savings tips)
  2. Market overview: TASE + NYSE indices + portfolio monthly performance
"""

import os
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from calendar import monthrange

import anthropic
import yfinance as yf

import sheets
import telegram_notify as whatsapp
import portfolio as port
from weekly_report import (
    _parse_date, _parse_amount, _read_expense_rows,
    _get_week_income, EXCLUDED_FROM_SPENDING, _pct_diff, _change_line
)

ISRAEL_TZ_OFFSET = 3


def _last_month() -> tuple[int, int]:
    today = (datetime.now(timezone.utc) + timedelta(hours=ISRAEL_TZ_OFFSET)).date()
    first = today.replace(day=1)
    last_m = first - timedelta(days=1)
    return last_m.year, last_m.month


def _month_range(year: int, month: int) -> tuple[str, str]:
    last_day = monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


# ─── Message 1: Monthly spending ──────────────────────────────────────────────

def _get_month_spending(rows: list[list], start: str, end: str) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    for row in rows:
        if len(row) < 6:
            continue
        d = _parse_date(row[1])
        if not (start <= d <= end):
            continue
        amount = _parse_amount(row[3])
        if amount >= 0:
            continue
        cat = row[5] or "שונות"
        if cat in EXCLUDED_FROM_SPENDING:
            continue
        totals[cat] += abs(amount)
    return dict(totals)


def _get_month_investments(rows: list[list], start: str, end: str) -> float:
    total = 0.0
    for row in rows:
        if len(row) < 6:
            continue
        d = _parse_date(row[1])
        if not (start <= d <= end):
            continue
        amount = _parse_amount(row[3])
        if amount >= 0:
            continue
        if (row[5] or "") == "השקעות":
            total += abs(amount)
    return total


def _get_monthly_averages_all(rows: list[list]) -> dict[str, float]:
    """Average monthly spending per category across all history."""
    by_month: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        if len(row) < 6:
            continue
        d_str = _parse_date(row[1])
        if not d_str:
            continue
        amount = _parse_amount(row[3])
        if amount >= 0:
            continue
        cat = row[5] or "שונות"
        if cat in EXCLUDED_FROM_SPENDING:
            continue
        month_key = d_str[:7]  # YYYY-MM
        by_month[month_key][cat] += abs(amount)

    cat_totals: dict[str, list[float]] = defaultdict(list)
    for month_data in by_month.values():
        for cat, amt in month_data.items():
            cat_totals[cat].append(amt)

    return {cat: sum(v) / len(v) for cat, v in cat_totals.items()}


def _get_month_income(svc, start: str, end: str) -> float:
    data = sheets._read(svc, sheets.INCOME_SHEET)
    total = 0.0
    for row in data[1:]:
        if len(row) < 4:
            continue
        d = _parse_date(row[1])
        if not (start <= d <= end):
            continue
        amount = _parse_amount(row[3])
        if amount > 0:
            total += amount
    return total


def build_monthly_spending_message(svc) -> str:
    year, month = _last_month()
    start, end = _month_range(year, month)
    month_name = datetime(year, month, 1).strftime("%B %Y")

    all_rows   = _read_expense_rows(svc)
    this_month = _get_month_spending(all_rows, start, end)
    averages   = _get_monthly_averages_all(all_rows)
    income     = _get_month_income(svc, start, end)
    invested   = _get_month_investments(all_rows, start, end)

    total_spent = sum(this_month.values())
    avg_total   = sum(averages.get(c, 0) for c in this_month)
    overall_pct = _pct_diff(total_spent, avg_total) if avg_total > 0 else ""
    sorted_cats = sorted(this_month.items(), key=lambda x: -x[1])

    # Build data text for Claude
    cat_lines = "\n".join(
        f"- {cat}: ₪{spent:,.0f} (ממוצע: ₪{averages.get(cat,0):,.0f})"
        for cat, spent in sorted_cats
    )

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    client  = anthropic.Anthropic(api_key=api_key)

    analysis = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=800,
        messages=[{"role": "user", "content": f"""אתה יועץ פיננסי. נתח את ההוצאות החודשיות הבאות ותן המלצות.

חודש: {month_name}
הכנסות: ₪{income:,.0f}
הוצאות: ₪{total_spent:,.0f}
הועבר להשקעות: ₪{invested:,.0f}

פירוט לפי קטגוריה (בהשוואה לממוצע החודשי):
{cat_lines}

כתוב בעברית, 3-4 משפטים:
1. איפה הוצאתם יותר מדי השבוע
2. איפה הייתם יכולים לחסוך
3. המלצה אחת מעשית לחודש הבא
פורמט לווטסאפ, ללא markdown."""}]
    ).content[0].text

    lines = [
        f"📅 *סיכום חודשי — {month_name}*",
        "",
        f"💰 הכנסות: ₪{income:,.0f}",
        f"💸 הוצאות: ₪{total_spent:,.0f} {overall_pct}",
    ]
    if invested > 0:
        lines.append(f"📈 הועבר להשקעות: ₪{invested:,.0f}")
    lines += [
        f"🏦 מאזן: {'➕' if income >= total_spent else '➖'} ₪{abs(income - total_spent):,.0f}",
        "",
        "*פירוט לפי קטגוריה:*",
    ]
    for cat, spent in sorted_cats:
        avg = averages.get(cat, 0)
        pct = _pct_diff(spent, avg)
        lines.append(f"  • {cat}: ₪{spent:,.0f} {pct}")

    # Bank balance: start of month vs now
    start_pf, start_bank = sheets.get_snapshot_on_or_before(svc, start)
    now_pf, now_bank     = sheets.get_snapshot_on_or_before(svc, end)
    if now_bank > 0 or start_bank > 0:
        lines.append("")
        lines.append(_change_line("יתרת עו\"ש", start_bank, now_bank, "🏦"))

    lines += ["", "💡 *ניתוח Claude:*", analysis]
    return "\n".join(lines)


# ─── Message 2: Market overview ───────────────────────────────────────────────

def _fetch_index(ticker: str, period: str = "1mo") -> tuple[float, float, str]:
    """Returns (current, change_pct, name). Safe — returns (0,0,'') on error."""
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period=period)
        if len(hist) < 2:
            return 0.0, 0.0, ""
        current    = float(hist["Close"].iloc[-1])
        month_ago  = float(hist["Close"].iloc[0])
        change_pct = (current - month_ago) / month_ago * 100
        name       = t.info.get("shortName") or t.info.get("longName") or ticker
        return current, change_pct, name
    except Exception:
        return 0.0, 0.0, ""


INDICES = [
    # Israeli (TA-35 not on Yahoo Finance — use TA-125 only)
    ("^TA125.TA", "ת\"א 125"),
    # US
    ("^GSPC",     "S&P 500"),
    ("^IXIC",     "Nasdaq"),
    ("^DJI",      "Dow Jones"),
]


def _market_lines() -> list[str]:
    lines = []
    for ticker, label in INDICES:
        price, pct, name = _fetch_index(ticker)
        if price == 0:
            continue
        icon = "📈" if pct >= 0 else "📉"
        lines.append(f"  {icon} {label}: {pct:+.1f}% החודש")
    return lines


def _portfolio_monthly_change(svc) -> tuple[float, str]:
    """Current total vs cost basis."""
    data = sheets._read(svc, port.PORTFOLIO_SHEET)
    total_value = 0.0
    total_cost  = 0.0
    for row in data[1:]:
        if len(row) < 10:
            continue
        try:
            total_value += float(str(row[7]).replace(",", ""))
            # cost = qty * cost_per_unit
            qty  = float(str(row[3]).replace(",", ""))
            cost = float(str(row[4]).replace(",", ""))
            total_cost += qty * cost
        except (ValueError, IndexError):
            continue
    if total_cost > 0:
        pct = (total_value - total_cost) / total_cost * 100
        return total_value, f"{pct:+.1f}%"
    return total_value, "n/a"


def build_monthly_market_message(svc) -> str:
    year, month = _last_month()
    month_name  = datetime(year, month, 1).strftime("%B %Y")

    market_lines          = _market_lines()
    portfolio_val, port_pct = _portfolio_monthly_change(svc)

    # Portfolio value: start of month vs now
    start, end = _month_range(year, month)
    start_pf, _ = sheets.get_snapshot_on_or_before(svc, start)

    # Holdings text for Claude
    holdings_data = sheets._read(svc, port.PORTFOLIO_SHEET)
    holdings_text = "\n".join(
        f"- {row[0]}: ₪{row[7]}" for row in holdings_data[1:] if len(row) > 7
    )

    market_text = "\n".join(market_lines) or "נתוני שוק לא זמינים"

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    client  = anthropic.Anthropic(api_key=api_key)

    analysis = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=900,
        messages=[{"role": "user", "content": f"""אתה אנליסט שוק. כתוב סיכום שוק חודשי בעברית.

חודש: {month_name}
תיק ההשקעות: ₪{portfolio_val:,.0f} ({port_pct} מעלות הרכישה)

אחזקות:
{holdings_text}

מדדים ראשיים החודש:
{market_text}

כתוב 4-5 משפטים:
1. מה קרה בשוק הישראלי החודש (ת"א 35, ת"א 125)
2. מה קרה בשוק האמריקאי (S&P, Nasdaq)
3. מה עשה עלייה/ירידה משמעותית
4. השפעה על התיק שלי
5. המלצה אחת — האם להגדיל/להקטין חשיפה כלשהי

פורמט לווטסאפ, ללא markdown, עם אמוג'י."""}]
    ).content[0].text

    lines = [
        f"🌍 *סקירת שוק חודשית — {month_name}*",
        "",
        "*מדדים ראשיים:*",
    ] + market_lines + [""]

    if start_pf > 0:
        lines.append(_change_line("שווי התיק", start_pf, portfolio_val, "💼"))
        lines.append(f"   (רווח מצטבר מעלות: {port_pct})")
    else:
        lines.append(f"💼 *התיק שלך:* ₪{portfolio_val:,.0f} ({port_pct} מעלות)")

    lines += ["", "🤖 *ניתוח:*", analysis]
    return "\n".join(lines)


# ─── Entry point ─────────────────────────────────────────────────────────────

def send_monthly_reports(svc) -> None:
    print("\n📅 Sending monthly reports...")
    whatsapp._send(build_monthly_spending_message(svc))
    print("✅ Monthly spending report sent")
    whatsapp._send(build_monthly_market_message(svc))
    print("✅ Monthly market report sent")
