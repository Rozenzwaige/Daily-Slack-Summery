import os
import requests

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")


def send(text: str) -> None:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print(f"[Telegram skip] {text[:120]}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Telegram send failed: {e}")


_send = send  # alias used by weekly_report / monthly_report


def alert_budget_exceeded(category: str, spent: float, budget: float, txn_desc: str) -> None:
    over = spent - budget
    pct = int(spent / budget * 100)
    send(
        f"🔴 חריגה מתקציב: {category}\n"
        f"₪{spent:,.0f} / ₪{budget:,.0f} ({pct}%)\n"
        f"חריגה: ₪{over:,.0f}\n"
        f"עסקה: {txn_desc}"
    )


def alert_high_balance(balance: float, threshold: float, recent_deposits=None) -> None:
    msg = f"💰 יתרה גבוהה: ₪{balance:,.0f}\nסף: ₪{threshold:,.0f}"
    if recent_deposits:
        tops = "\n".join(
            f"  • {d['description']}: ₪{d['amount']:,.0f}"
            for d in recent_deposits[:5]
        )
        msg += f"\nהפקדות אחרונות:\n{tops}"
    send(msg)


def alert_category_spike(category: str, spent: float, avg: float, last_txn, by_card=None) -> None:
    pct = int(spent / avg * 100) if avg > 0 else 0
    last_desc = last_txn.get("description", "") if last_txn else ""
    last_date = last_txn.get("date", "")[:10] if last_txn else ""
    msg = (
        f"🟠 קפיצה בקטגוריה: {category}\n"
        f"שבוע נוכחי: ₪{spent:,.0f} (ממוצע: ₪{avg:,.0f}, {pct}%)\n"
        f"עסקה אחרונה: {last_desc} ({last_date})"
    )
    if by_card:
        card_lines = " | ".join(f"{c['card']}: ₪{c['amount']:,.0f}" for c in by_card)
        msg += f"\n{card_lines}"
    send(msg)


def send_daily_summary(lines_list: list) -> None:
    send("\n".join(lines_list))
