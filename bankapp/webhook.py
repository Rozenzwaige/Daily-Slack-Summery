#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WhatsApp two-way bot (webhook) — replies to follow-up questions about the
portfolio / spending, using Claude with the user's real data.

GreenAPI pushes every incoming message to POST /webhook. We answer only the
configured finance chat, build context from the Google Sheet, ask Claude,
and reply via GreenAPI.

Run locally:  gunicorn -b 0.0.0.0:8080 webhook:app
Deploy:       Fly.io (see BOT.md)

Env: GREEN_API_INSTANCE, GREEN_API_TOKEN, FINANCE_WHATSAPP_CHAT_ID,
     ANTHROPIC_API_KEY, GOOGLE_CREDENTIALS_JSON, FINANCE_SPREADSHEET_ID,
     WEBHOOK_TOKEN (optional shared secret)
"""

import base64
import os
import time
from collections import defaultdict

import anthropic
import requests
from flask import Flask, request, jsonify

# Allow passing Google credentials as base64 (avoids shell-quoting issues on deploy)
if os.environ.get("GOOGLE_CREDENTIALS_B64") and not os.environ.get("GOOGLE_CREDENTIALS_JSON"):
    os.environ["GOOGLE_CREDENTIALS_JSON"] = base64.b64decode(
        os.environ["GOOGLE_CREDENTIALS_B64"]
    ).decode("utf-8")

import sheets

_GREEN_INSTANCE = os.environ.get("GREEN_API_INSTANCE", "")
_GREEN_TOKEN    = os.environ.get("GREEN_API_TOKEN", "")


def _reply_greenapi(chat_id: str, text: str):
    if not (_GREEN_INSTANCE and _GREEN_TOKEN):
        print("⚠️  GreenAPI not configured — reply skipped")
        return
    url = f"https://api.green-api.com/waInstance{_GREEN_INSTANCE}/sendMessage/{_GREEN_TOKEN}"
    try:
        r = requests.post(url, json={"chatId": chat_id, "message": text}, timeout=15)
        r.raise_for_status()
        print("✅ GreenAPI reply sent")
    except Exception as e:
        print(f"❌ GreenAPI send failed: {e}")

app = Flask(__name__)

CHAT_ID       = os.environ.get("FINANCE_WHATSAPP_CHAT_ID", "")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
PORTFOLIO_SHEET = "בורסה"

_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

# Short per-chat history so follow-up questions keep context (resets on restart)
_history: dict[str, list] = defaultdict(list)
_MAX_TURNS = 8

# Cache the portfolio context for a few minutes (avoid hitting Sheets every msg)
_ctx_cache = {"text": "", "ts": 0.0}
_CTX_TTL = 300


def _safe_float(val) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _build_context() -> str:
    now = time.time()
    if _ctx_cache["text"] and now - _ctx_cache["ts"] < _CTX_TTL:
        return _ctx_cache["text"]

    svc = sheets.open_spreadsheet()

    # Portfolio holdings
    data = sheets._read(svc, PORTFOLIO_SHEET)
    lines, total = [], 0.0
    for row in data[1:]:
        if len(row) < 10:
            continue
        name = row[0]
        value = _safe_float(row[7])
        pnl = _safe_float(row[9])
        total += value
        lines.append((name, value, pnl))
    lines.sort(key=lambda x: -x[1])
    holdings = "\n".join(
        f"- {n}: ₪{v:,.0f} ({v/total*100:.1f}% מהתיק), רווח/הפסד מאז רכישה {p:+.1f}%"
        for n, v, p in lines
    ) if total else "אין נתוני תיק זמינים."

    # Latest bank balance
    _, bank = sheets.get_snapshot_on_or_before(
        svc, time.strftime("%Y-%m-%d")
    )

    ctx = (
        f"שווי תיק ההשקעות הכולל: ₪{total:,.0f}\n"
        f"יתרת עו\"ש נוכחית: ₪{bank:,.0f}\n\n"
        f"אחזקות בתיק:\n{holdings}"
    )
    _ctx_cache.update(text=ctx, ts=now)
    return ctx


SYSTEM_PROMPT = (
    "אתה עוזר פיננסי אישי בוואטסאפ של זוג ישראלי. אתה עונה על שאלות לגבי תיק "
    "ההשקעות וההוצאות שלהם. הנתונים שתקבל הם אמיתיים ומדויקים — אל תמציא מספרים "
    "אחרים. אם חסר לך מידע, אמור זאת בכנות. תן עצה מקצועית, מאוזנת וזהירה, "
    "והבהר שזו אינה ייעוץ השקעות מורשה. כתוב בעברית, קצר וברור, מתאים לוואטסאפ "
    "(ללא markdown כבד, מותר אמוג'י)."
)


def _answer(chat_id: str, question: str) -> str:
    ctx = _build_context()
    hist = _history[chat_id]
    hist.append({"role": "user", "content": question})
    hist[:] = hist[-_MAX_TURNS * 2:]  # keep last N turns

    try:
        resp = _client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=800,
            system=f"{SYSTEM_PROMPT}\n\n--- נתוני הלקוח (עדכניים) ---\n{ctx}",
            messages=hist,
        )
        answer = resp.content[0].text
    except Exception as e:
        print(f"❌ Claude error: {e}")
        return "מצטער, נתקלתי בתקלה זמנית. נסה שוב בעוד רגע."

    hist.append({"role": "assistant", "content": answer})
    return answer


@app.get("/")
def health():
    return "ok", 200


@app.post("/webhook")
def webhook():
    # Optional shared-secret check (GreenAPI sends it as Authorization: Bearer <token>)
    if WEBHOOK_TOKEN:
        auth = request.headers.get("Authorization", "").replace("Bearer ", "")
        if auth != WEBHOOK_TOKEN:
            return "", 403

    data = request.get_json(force=True, silent=True) or {}
    if data.get("typeWebhook") != "incomingMessageReceived":
        return jsonify(ok=True)  # ignore status/outgoing webhooks

    sender = data.get("senderData", {}).get("chatId", "")
    if CHAT_ID and sender != CHAT_ID:
        return jsonify(ignored=True)  # only answer the configured chat

    md = data.get("messageData", {})
    text = (
        (md.get("textMessageData") or {}).get("textMessage")
        or (md.get("extendedTextMessageData") or {}).get("text")
        or ""
    ).strip()
    if not text:
        return jsonify(ok=True)

    print(f"💬 Q: {text[:80]}")
    reply = _answer(sender, text)
    _reply_greenapi(sender, reply)
    print(f"✅ A: {reply[:80]}")
    return jsonify(ok=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
