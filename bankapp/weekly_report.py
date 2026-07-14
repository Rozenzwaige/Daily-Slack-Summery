#!/usr/bin/env python3
"""
Thursday 21:00 weekly report — two WhatsApp messages:
  1. Spending summary: this week vs historical average, by category
  2. Portfolio summary: Claude analysis + performance + recommendations
"""

import os
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone

import anthropic
import yfinance as yf

import sheets
import telegram_notify as whatsapp
import portfolio as port
from categorizer import EXCLUDED_FROM_SPENDING

ISRAEL_TZ_OFFSET = 3  # UTC+3

# Excel serial date epoch
_EXCEL_EPOCH = datetime(1899, 12, 30)


def _parse_date(val) -> str:
    """Convert Excel serial date or ISO string to YYYY-MM-DD."""
    if not val:
        return ""
    try:
        serial = int(float(str(val)))
        return (_EXCEL_EPOCH + timedelta(days=serial)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(val)[:10]


def _israel_today() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=ISRAEL_TZ_OFFSET)).date()


def _week_range(ref: date) -> tuple[str, str]:
    """Return (monday, sunday) of the week containing ref."""
    monday = ref - timedelta(days=ref.weekday())
    sunday = monday + timedelta(days=6)
    return monday.isoformat(), sunday.isoformat()


# ─── Message 1: Spending summary ─────────────────────────────────────────────

def _read_expense_rows(svc) -> list[list]:
    rows = []
    for sheet in [sheets.EXPENSE_SHEET, sheets.CREDIT_SHEET]:
        data = sheets._read(svc, sheet)
        rows.extend(data[1:])  # skip header
    return rows


def _parse_amount(val) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _get_week_investments(rows: list[list], start: str, end: str) -> float:
    """Sum investment transfers (excluded from expenses) for a date range."""
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
        cat = row[5] or "שונות"
        if cat == "השקעות":
            total += abs(amount)
    return total


def _get_week_spending(rows: list[list], start: str, end: str) -> dict[str, float]:
    """Sum absolute expenses by category for a date range."""
    totals: dict[str, float] = defaultdict(float)
    for row in rows:
        if len(row) < 6:
            continue
        d = _parse_date(row[1])
        if not (start <= d <= end):
            continue
        amount = _parse_amount(row[3])
        if amount >= 0:
            continue  # skip credits / refunds
        cat = row[5] or "שונות"
        if cat in EXCLUDED_FROM_SPENDING:
            continue  # skip credit card payments & investment transfers
        totals[cat] += abs(amount)
    return dict(totals)


def _get_week_income(svc, start: str, end: str) -> float:
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


def _get_weekly_averages(rows: list[list]) -> dict[str, float]:
    """
    Calculate average weekly spending per category over all history.
    Only counts weeks that have at least 1 expense.
    """
    by_week: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
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
        try:
            d = date.fromisoformat(d_str)
        except ValueError:
            continue
        monday = (d - timedelta(days=d.weekday())).isoformat()
        by_week[monday][cat] += abs(amount)

    # Average over weeks that have data for each category
    cat_totals: dict[str, list[float]] = defaultdict(list)
    for week_data in by_week.values():
        for cat, amt in week_data.items():
            cat_totals[cat].append(amt)

    return {cat: sum(vals) / len(vals) for cat, vals in cat_totals.items()}


def _pct_diff(current: float, avg: float) -> str:
    if avg <= 0:
        return ""
    diff = (current - avg) / avg * 100
    sign = "+" if diff >= 0 else ""
    return f"({sign}{diff:.0f}%)"


def _change_line(label: str, start_val: float, end_val: float, emoji: str) -> str:
    """Format a 'start → now' change line with absolute amounts."""
    if start_val <= 0:
        # No historical snapshot yet — show current value only
        return (
            f"{emoji} {label}: ₪{end_val:,.0f}\n"
            f"   (השוואה לתחילת התקופה תופיע בדוח הבא)"
        )
    diff = end_val - start_val
    pct = diff / start_val * 100
    pct_str = f" ({'+' if diff >= 0 else ''}{pct:.1f}%)"
    arrow = "📈" if diff >= 0 else "📉"
    return (
        f"{emoji} {label}:\n"
        f"   תחילת התקופה: ₪{start_val:,.0f}\n"
        f"   עכשיו: ₪{end_val:,.0f}\n"
        f"   {arrow} שינוי: {'+' if diff >= 0 else ''}₪{diff:,.0f}{pct_str}"
    )


def build_spending_message(svc) -> str:
    today = _israel_today()
    start, end = _week_range(today)
    start_portfolio, start_bank = sheets.get_snapshot_on_or_before(svc, start)
    now_portfolio, now_bank     = sheets.get_snapshot_on_or_before(svc, end)

    all_rows   = _read_expense_rows(svc)
    this_week  = _get_week_spending(all_rows, start, end)
    averages   = _get_weekly_averages(all_rows)
    income     = _get_week_income(svc, start, end)
    invested   = _get_week_investments(all_rows, start, end)

    total_spent = sum(this_week.values())
    avg_total   = sum(averages.get(c, 0) for c in this_week)
    overall_pct = _pct_diff(total_spent, avg_total) if avg_total > 0 else ""

    # Sort categories by spend (descending)
    sorted_cats = sorted(this_week.items(), key=lambda x: -x[1])

    lines = [
        f"📊 *סיכום הוצאות שבועי*",
        f"{start} עד {end}",
        "",
        f"💰 הכנסות השבוע: ₪{income:,.0f}",
        f"💸 הוצאות השבוע: ₪{total_spent:,.0f} {overall_pct}",
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

    if avg_total > 0:
        sign = "יותר" if total_spent > avg_total else "פחות"
        diff_pct = abs(total_spent - avg_total) / avg_total * 100
        lines.append(f"\n📊 סה\"כ הוצאת *{diff_pct:.0f}% {sign}* מהממוצע השבועי שלכם")

    # Bank balance: start of week vs now
    if now_bank > 0 or start_bank > 0:
        lines.append("")
        lines.append(_change_line("יתרת עו\"ש", start_bank, now_bank, "🏦"))

    return "\n".join(lines)


# ─── Message 2: Portfolio summary ────────────────────────────────────────────

def _get_goog_weekly_change() -> tuple[float, float]:
    """Returns (price_now, weekly_change_pct) for GOOG."""
    try:
        ticker = yf.Ticker("GOOG")
        hist = ticker.history(period="8d")
        if len(hist) >= 5:
            price_now  = float(hist["Close"].iloc[-1])
            price_week = float(hist["Close"].iloc[-6])
            change_pct = (price_now - price_week) / price_week * 100
            return price_now, change_pct
        price_now = float(ticker.fast_info.last_price)
        return price_now, 0.0
    except Exception:
        return 0.0, 0.0


def _safe_float(val) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _build_holdings_list(svc, start_date: str) -> tuple[str, float, str]:
    """
    Build a deterministic per-holding list from the בורסה sheet.
    Returns (whatsapp_text, total_value, plain_text_for_claude).

    Each line shows: name, current ₪ value, weekly change (from snapshot),
    profit/loss since purchase, and % of portfolio.
    """
    data = sheets._read(svc, port.PORTFOLIO_SHEET)
    start_values = sheets.get_holding_values_on_or_before(svc, start_date)

    holdings = []
    total = 0.0
    for row in data[1:]:
        if len(row) < 10:
            continue
        sec_id  = str(row[1])
        name    = row[0]
        value   = _safe_float(row[7])   # שווי נוכחי (₪)
        pnl_pct = _safe_float(row[9])   # רווח/הפסד מאז רכישה (%)
        total  += value
        start_val = start_values.get(sec_id, 0.0)
        week_chg  = value - start_val if start_val > 0 else None
        week_pct  = (week_chg / start_val * 100) if (start_val > 0 and week_chg is not None) else None
        holdings.append({
            "name": name, "value": value, "pnl_pct": pnl_pct,
            "week_chg": week_chg, "week_pct": week_pct,
        })

    # Sort by current value (largest holding first)
    holdings.sort(key=lambda h: -h["value"])

    wa_lines    = []
    claude_lines = []
    for h in holdings:
        pct_of_port = (h["value"] / total * 100) if total else 0
        # Weekly change segment
        if h["week_pct"] is not None:
            wk_arrow = "📈" if h["week_chg"] >= 0 else ("📉" if h["week_chg"] < 0 else "➡️")
            wk_seg = f" | השבוע: {'+' if h['week_chg'] >= 0 else ''}₪{h['week_chg']:,.0f} ({h['week_pct']:+.1f}%)"
        else:
            wk_arrow = "•"
            wk_seg = ""
        wa_lines.append(
            f"{wk_arrow} {h['name']}\n"
            f"   💰 ₪{h['value']:,.0f} ({pct_of_port:.1f}% מהתיק){wk_seg}\n"
            f"   מאז רכישה: {h['pnl_pct']:+.1f}%"
        )
        claude_lines.append(
            f"- {h['name']}: ₪{h['value']:,.0f} ({pct_of_port:.1f}% מהתיק), "
            f"רווח מאז רכישה {h['pnl_pct']:+.1f}%"
            + (f", שינוי שבועי {h['week_pct']:+.1f}%" if h["week_pct"] is not None else "")
        )

    return "\n\n".join(wa_lines), total, "\n".join(claude_lines)


def build_portfolio_message(svc) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "⚠️ ANTHROPIC_API_KEY לא מוגדר"

    usd_ils = port.get_usd_ils()
    today = _israel_today()
    start, end = _week_range(today)

    # Deterministic per-holding list (exact ₪ amounts, real weekly change)
    holdings_wa, total_value, holdings_for_claude = _build_holdings_list(svc, start)

    # Portfolio total: start of week vs now (from history snapshots)
    start_portfolio, _ = sheets.get_snapshot_on_or_before(svc, start)

    # Claude generates ONLY the analysis + recommendations (not the raw numbers)
    client = anthropic.Anthropic(api_key=api_key)
    analysis = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=1000,
        messages=[{"role": "user", "content": f"""אתה יועץ השקעות. להלן תיק ההשקעות של הלקוח (הנתונים מדויקים — אל תמציא מספרים אחרים):

שווי תיק כולל: ₪{total_value:,.0f}

אחזקות:
{holdings_for_claude}

כתוב ניתוח קצר בעברית הכולל רק:
1. *מבנה התיק* — חשיפה ישראלית מול בינלאומית, ריכוזיות חריגה אם יש (משפט-שניים)
2. *2 המלצות להוספה* — מניות/קרנות ספציפיות עם הסבר קצר
3. *שיקול להחלפה* — אם יש נכס חלש שכדאי לשקול להחליף, ובמה

אל תחזור על רשימת האחזקות (היא כבר מוצגת ללקוח). התמקד בתובנות בלבד.
פורמט לווטסאפ: ללא markdown כבד, השתמש באמוג'י. קצר וממוקד."""}]
    ).content[0].text

    change_block = ""
    if start_portfolio > 0:
        change_block = _change_line("שווי התיק", start_portfolio, total_value, "💼") + "\n\n"
    else:
        change_block = f"💼 שווי כולל: ₪{total_value:,.0f}\n\n"

    msg = (
        f"📈 *סיכום תיק ההשקעות השבועי*\n\n"
        f"{change_block}"
        f"💵 שער דולר: ₪{usd_ils:.3f}\n\n"
        f"*📊 פירוט האחזקות:*\n\n"
        f"{holdings_wa}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🤖 *ניתוח והמלצות:*\n\n"
        f"{analysis}"
    )
    return msg


# ─── Entry point ─────────────────────────────────────────────────────────────

def send_weekly_reports(svc) -> None:
    print("\n📬 Sending Thursday weekly reports...")

    msg1 = build_spending_message(svc)
    whatsapp._send(msg1)
    print("✅ Spending report sent")

    msg2 = build_portfolio_message(svc)
    whatsapp._send(msg2)
    print("✅ Portfolio report sent")
