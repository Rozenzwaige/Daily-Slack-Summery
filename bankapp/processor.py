#!/usr/bin/env python3
"""
Main entry point. Run after scrape.js.
Reads scraped_data.json → updates Sheets → checks budgets → WhatsApp alerts.
"""

import hashlib
import re
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from collections import defaultdict
from datetime import date, timedelta

import sheets
import telegram_notify as whatsapp
import portfolio
from categorizer import categorize, EXCLUDED_FROM_SPENDING

SCRAPED_FILE           = Path(__file__).parent / "scraped_data.json"
HIGH_BALANCE_THRESHOLD = float(os.environ.get("HIGH_BALANCE_THRESHOLD", "20000"))


# ─── Parse scraped JSON ───────────────────────────────────────────────────────

_BRANCH_SUFFIX = re.compile(r"[\-\s]+(צמרת|2015|2016|2017|2018|2019|2020|2021|סניף.*|branch.*)$", re.IGNORECASE)

def _normalize_desc(desc: str) -> str:
    """Strip branch/year suffixes so same transaction with updated name isn't double-counted."""
    return _BRANCH_SUFFIX.sub("", desc.strip()).strip()

def _txn_id(source: str, account: str, date: str, desc: str, amount: float) -> str:
    norm = _normalize_desc(desc)
    raw = f"{source}|{account}|{date}|{norm}|{amount}"
    return "t" + hashlib.md5(raw.encode()).hexdigest()[:15]



def _cross_day_dedup(txns: list[dict]) -> list[dict]:
    """Remove duplicate transactions where כאל updated date/desc between scrapes.
    Rule: same source+account+amount, normalized desc matches, dates within 2 days → keep later date."""
    kept = []
    removed = set()
    for i, a in enumerate(txns):
        if i in removed:
            continue
        d_a = date.fromisoformat(a["date"]) if a["date"] else date.today()
        for j, b in enumerate(txns):
            if j <= i or j in removed:
                continue
            if a["source"] != b["source"] or a["account"] != b["account"]:
                continue
            if abs(a["amount"] - b["amount"]) > 0.5:
                continue
            d_b = date.fromisoformat(b["date"]) if b["date"] else date.today()
            if abs((d_b - d_a).days) > 2:
                continue
            if _normalize_desc(a["description"]) != _normalize_desc(b["description"]):
                continue
            # Same transaction — keep the one with the later date
            removed.add(i if d_b >= d_a else j)
        if i not in removed:
            kept.append(a)
    return kept


def _fuzzy_match_existing(new_txns: list[dict], existing_rows: list[dict]) -> list[dict]:
    """Filter out new transactions that already exist in Sheets (cross-day version).
    existing_rows: list of {date, account, amount, desc} from recent Sheets data."""
    result = []
    for t in new_txns:
        d_t = date.fromisoformat(t["date"]) if t["date"] else date.today()
        norm_t = _normalize_desc(t["description"])
        duplicate = False
        for e in existing_rows:
            if t["account"] != e["account"]:
                continue
            if abs(t["amount"] - e["amount"]) > 0.5:
                continue
            d_e = date.fromisoformat(e["date"]) if e["date"] else date.today()
            if abs((d_t - d_e).days) > 2:
                continue
            if norm_t == _normalize_desc(e["desc"]):
                duplicate = True
                break
        if not duplicate:
            result.append(t)
    return result


def parse_transactions(data: dict) -> list[dict]:
    txns = []
    for source, result in data.items():
        if source == "scraped_at" or not result.get("success"):
            continue
        for acc in result.get("accounts", []):
            acct_no = acc.get("accountNumber", "unknown")
            for t in acc.get("txns", []):
                amount = t.get("chargedAmount", t.get("originalAmount", 0))
                desc   = t.get("description", "")
                memo   = t.get("memo", "")
                date   = t.get("date", "")[:10]
                txns.append({
                    "txn_id":      _txn_id(source, acct_no, date, desc, amount),
                    "date":        date,
                    "description": desc,
                    "amount":      amount,   # signed: negative=חובה, positive=זכות
                    "currency":    t.get("originalCurrency", "ILS"),
                    "category":    t.get("category") or categorize(desc, memo),
                    "account":     acct_no,
                    "source":      source,
                    "status":      t.get("status", ""),
                    "raw_amount":  amount,
                })
    return txns


def parse_balances(data: dict) -> list[dict]:
    balances = []
    for source, result in data.items():
        if source == "scraped_at" or not result.get("success"):
            continue
        for acc in result.get("accounts", []):
            b = acc.get("balance")
            if b is not None:
                balances.append({
                    "source":  source,
                    "account": acc.get("accountNumber", "unknown"),
                    "balance": float(b),
                })
    return balances


# ─── Alerts ───────────────────────────────────────────────────────────────────

def check_budgets(svc, new_txns: list[dict]) -> None:
    budgets = sheets.get_budgets(svc)
    if not budgets:
        print("ℹ️  No budgets in Sheets — skipping")
        return

    now = datetime.now(timezone.utc)
    spending = sheets.get_month_spending(svc, now.year, now.month)

    for txn in new_txns:
        if txn["raw_amount"] >= 0:
            continue
        cat = txn["category"]
        if cat not in budgets:
            continue
        spent = spending.get(cat, 0.0)
        if spent >= budgets[cat]:
            print(f"🚨 {cat}: ₪{spent:,.0f} / ₪{budgets[cat]:,.0f}")
            whatsapp.alert_budget_exceeded(cat, spent, budgets[cat], txn["description"])


HIGH_BALANCE_STATE = Path(__file__).parent / "high_balance_state.json"


def _was_below_threshold_last_time() -> bool:
    """True if the previous run had balance below threshold (so a new crossing should alert)."""
    if not HIGH_BALANCE_STATE.exists():
        return True
    try:
        return not json.loads(HIGH_BALANCE_STATE.read_text()).get("above", False)
    except Exception:
        return True


def _save_high_balance_state(above: bool) -> None:
    HIGH_BALANCE_STATE.write_text(json.dumps({"above": above}))


def check_high_balance(svc, balances: list[dict], new_txns: list[dict]) -> None:
    fibi = next((b for b in balances if b["source"] == "fibi"), None)
    if not fibi:
        return
    current = fibi["balance"]
    above = current > HIGH_BALANCE_THRESHOLD

    # Alert only when CROSSING upward (was below last time, now above)
    if above and _was_below_threshold_last_time():
        deposits = [
            {"amount": t["amount"], "description": t["description"], "date": t["date"]}
            for t in new_txns
            if t.get("source") == "fibi" and t["amount"] > 0
        ]
        deposits.sort(key=lambda d: -d["amount"])
        print(f"💰 High balance crossed: ₪{current:,.0f}")
        whatsapp.alert_high_balance(current, HIGH_BALANCE_THRESHOLD, deposits[:5])
    elif above:
        print(f"💰 Balance still high (₪{current:,.0f}) — already alerted, skipping")

    _save_high_balance_state(above)


# ─── Real-time credit-card category spike alert ───────────────────────────────

CATEGORY_SPIKE_STATE = Path(__file__).parent / "category_spike_state.json"
SPIKE_THRESHOLD      = 1.0  # alert when week-to-date >= 100% of the weekly average

# Excel serial date epoch (Sheets may store dates as serial numbers)
_EXCEL_EPOCH = datetime(1899, 12, 30)


def _parse_sheet_date(val) -> str:
    """Convert an Excel serial date or ISO string to YYYY-MM-DD."""
    if not val:
        return ""
    try:
        serial = int(float(str(val)))
        return (_EXCEL_EPOCH + timedelta(days=serial)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(val)[:10]


def _parse_amount(val) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _current_week_monday() -> str:
    today = (datetime.now(timezone.utc) + timedelta(hours=3)).date()  # Israel UTC+3
    return (today - timedelta(days=today.weekday())).isoformat()


def _load_spike_state() -> dict:
    if not CATEGORY_SPIKE_STATE.exists():
        return {}
    try:
        return json.loads(CATEGORY_SPIKE_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_spike_state(state: dict) -> None:
    CATEGORY_SPIKE_STATE.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def check_category_spikes(svc, new_txns: list[dict]) -> None:
    """
    For new CREDIT (cal) expenses, alert in real time if the week-to-date spending
    in that category already exceeds the historical weekly average for that category.
    One alert per category per week.
    """
    cal_new = [
        t for t in new_txns
        if t.get("source") in sheets.CREDIT_SOURCES and t["amount"] < 0
        and t["category"] not in EXCLUDED_FROM_SPENDING
    ]
    if not cal_new:
        return

    affected = {t["category"] for t in cal_new}
    week_monday = _current_week_monday()
    week_sunday = (date.fromisoformat(week_monday) + timedelta(days=6)).isoformat()

    rows = sheets._read(svc, sheets.CREDIT_SHEET)[1:]  # skip header

    # Week-to-date per category (combined across cards) + per-card breakdown,
    # plus historical weekly averages (excluding the current week)
    wtd: dict[str, float] = defaultdict(float)
    wtd_by_card: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    by_week: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        if len(row) < 6:
            continue
        amount = _parse_amount(row[3])
        if amount >= 0:
            continue
        cat = row[5] or "שונות"
        if cat in EXCLUDED_FROM_SPENDING:
            continue
        d = _parse_sheet_date(row[1])
        if not d:
            continue
        if week_monday <= d <= week_sunday:
            wtd[cat] += abs(amount)
            card = str(row[6]) if len(row) > 6 and row[6] else "?"
            wtd_by_card[cat][card] += abs(amount)
        else:
            wk = (date.fromisoformat(d) - timedelta(days=date.fromisoformat(d).weekday())).isoformat() \
                 if _is_iso(d) else None
            if wk:
                by_week[wk][cat] += abs(amount)

    averages: dict[str, list] = defaultdict(list)
    for wk_data in by_week.values():
        for cat, amt in wk_data.items():
            averages[cat].append(amt)
    avg = {cat: sum(v) / len(v) for cat, v in averages.items()}

    state = _load_spike_state()
    already = set(state.get(week_monday, []))

    for cat in affected:
        a = avg.get(cat, 0.0)
        spent = wtd.get(cat, 0.0)
        if a > 0 and spent >= a * SPIKE_THRESHOLD and cat not in already:
            last = max(
                (t for t in cal_new if t["category"] == cat),
                key=lambda t: t["date"],
            )
            # Per-card breakdown (only if more than one card spent in this category)
            cards = wtd_by_card.get(cat, {})
            by_card = None
            if len(cards) > 1:
                by_card = [
                    {"card": c, "amount": amt}
                    for c, amt in sorted(cards.items(), key=lambda x: -x[1])
                ]
            print(f"🟠 Category spike: {cat} ₪{spent:,.0f} (avg ₪{a:,.0f})")
            whatsapp.alert_category_spike(cat, spent, a, last, by_card)
            already.add(cat)

    state[week_monday] = sorted(already)
    # Keep state small — only retain current week
    _save_spike_state({week_monday: state[week_monday]})


def _is_iso(d: str) -> bool:
    try:
        date.fromisoformat(d)
        return True
    except ValueError:
        return False


# ─── Daily summary ────────────────────────────────────────────────────────────

def build_summary(spending: dict, budgets: dict, balances: list, new_count: int) -> list[str]:
    lines = [f"עסקאות חדשות: {new_count}"]

    if balances:
        lines.append("\n*יתרות:*")
        for b in balances:
            label = "הבינלאומי" if b["source"] == "fibi" else "כאל"
            lines.append(f"  {label}: ₪{b['balance']:,.0f}")

    if spending:
        lines.append("\n*הוצאות החודש:*")
        for cat, spent in sorted(spending.items(), key=lambda x: -x[1]):
            bud = budgets.get(cat)
            if bud:
                pct  = int(spent / bud * 100)
                icon = "🔴" if pct >= 100 else ("🟡" if pct >= 80 else "🟢")
                lines.append(f"  {icon} {cat}: ₪{spent:,.0f} / ₪{bud:,.0f} ({pct}%)")
            else:
                lines.append(f"  • {cat}: ₪{spent:,.0f}")

    return lines


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not SCRAPED_FILE.exists():
        print(f"❌ {SCRAPED_FILE} not found — run scrape.js first")
        sys.exit(1)

    data = json.loads(SCRAPED_FILE.read_text(encoding="utf-8"))
    print(f"📂 Scraped at: {data.get('scraped_at', '?')}")

    svc = sheets.open_spreadsheet()
    sheets.ensure_headers(svc)

    # Load seen txn_ids from local file (reliable — not dependent on Sheets column format)
    SEEN_FILE = Path(__file__).parent / "seen_txn_ids.json"
    seen_ids: set[str] = set(json.loads(SEEN_FILE.read_text(encoding="utf-8")) if SEEN_FILE.exists() else [])

    all_txns  = parse_transactions(data)
    # Step 1: dedup within the current batch
    all_txns  = _cross_day_dedup(all_txns)
    # Step 2: skip known txn_ids
    new_txns  = [t for t in all_txns if t["txn_id"] not in seen_ids]
    # Step 3: cross-day fuzzy dedup against recent Sheets rows (90 days to cover full scrape window)
    recent_rows = sheets.get_recent_rows(svc, days=90)
    new_txns  = _fuzzy_match_existing(new_txns, recent_rows)

    print(f"📋 {len(all_txns)} transactions total, {len(new_txns)} new")
    added = sheets.append_transactions(svc, new_txns)
    print(f"✅ Added {added} rows to Sheets")

    # Persist all txn_ids (existing + new) to local file
    seen_ids.update(t["txn_id"] for t in all_txns)
    SEEN_FILE.write_text(json.dumps(list(seen_ids)), encoding="utf-8")

    balances = parse_balances(data)
    for b in balances:
        sheets.log_balance(svc, b["source"], b["account"], b["balance"])

    check_budgets(svc, new_txns)
    check_high_balance(svc, balances, new_txns)
    check_category_spikes(svc, new_txns)

    now      = datetime.now(timezone.utc)
    spending = sheets.get_month_spending(svc, now.year, now.month)
    budgets  = sheets.get_budgets(svc)
    summary  = build_summary(spending, budgets, balances, added)

    # Daily summary removed — weekly report sent every Thursday at 21:00 by portfolio.py

    # Portfolio tracking
    portfolio_value = portfolio.run(svc)

    # Save daily snapshot (portfolio value + bank balance) for trend tracking
    bank_balance = next((b["balance"] for b in balances if b["source"] == "fibi"), 0.0)
    sheets.log_daily_snapshot(svc, portfolio_value, bank_balance)
    print(f"📸 Snapshot saved — תיק: ₪{portfolio_value:,.0f}, עו\"ש: ₪{bank_balance:,.0f}")

    print("\n🎉 Done")


if __name__ == "__main__":
    main()
