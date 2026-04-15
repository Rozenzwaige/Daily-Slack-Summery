"""
slack_summary.py — שליחת סיכום יומי של פרסומים לסלאק
הרצה: python slack_summary.py

משתני סביבה נדרשים:
  SLACK_BOT_TOKEN       xoxb-...
  SLACK_CHANNEL_ID      C0123456789
  GOOGLE_CREDENTIALS_JSON  תוכן קובץ ה-JSON של Service Account (כמחרוזת)
  SPREADSHEET_ID        מזהה הספרדשיט
  SHEET_NAME            שם הגיליון (ברירת מחדל: עומדים ביחד פרסומים)
"""

import io
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ── Brand palette ─────────────────────────────────────────────────────────
ST_PALETTE = ["#8B1A9D", "#B55BC8", "#5C1070", "#D4A0DC",
              "#3D0950", "#E8C8EE", "#C94FDF", "#7B0F8F"]
BG_COLOR   = "#0f172a"
PANEL_COLOR = "#1e293b"
TEXT_COLOR  = "#f1f5f9"
ACCENT      = "#B55BC8"

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# Column constants (same as dashboard.py)
C_DATE = "תאריך"; C_TIME = "שעה"; C_SOURCE = "גוף תקשורת"
C_TITLE = "כותרת"; C_CONTENT = "תוכן"; C_LINK = "קישור"
C_LANG = "שפה"; C_MEDIA = "מדיה"; C_SENT = "סנטימנט"
C_PUBTYPE = "סוג פרסום"; C_TOPIC = "נושא"; C_SECTOR = "מגזר"
C_REACH = "חשיפה"; C_VALUE = "ערך"
C_SERIAL = "מספר סידורי"

ALL_COLS = [C_DATE, C_TIME, C_SOURCE, "מדור", C_TITLE, C_CONTENT,
            "כתב", "דמויות", C_LINK, C_SERIAL, C_LANG, C_MEDIA,
            C_SENT, C_PUBTYPE, C_TOPIC, C_SECTOR, C_REACH, C_VALUE]

_STOP = set("""
את של על עם אל לא כי הם הן זה זו כל יש אבל גם רק אם כך
הוא היא אנחנו אני אתה הם הן יהיה היה הייתה היו יהיו
מה מי ו ל מ ב כ ה לו לה לנו להם לי שלו שלה שלנו שלהם
אחד אחת כבר אין עוד כן לפי אחרי לפני בין רק גם עד
אשר שם לכן לאחר בזמן כאשר בגלל למרות אך אולם מאז
בשנת עוד אנו זאת אלה אלו כך כן לכן שהוא שהיא שהם
עומדים ביחד עומד יחד תנועת תנועה תנועות ישראל ישראלי
""".split())

SENT_EMOJI = {"חיובי": "😊", "ניטרלי": "😐", "שלילי": "😞"}
MEDIA_EMOJI = {"טלוויזיה": "📺", "רדיו": "📻", "עיתון": "📰",
               "אינטרנט": "🌐", "רשתות חברתיות": "📱"}


# ══ Data loading ═════════════════════════════════════════════════════════════

def _get_client() -> gspread.Client:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing")
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def load_sheet(client: gspread.Client, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    ws = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return pd.DataFrame(columns=ALL_COLS)

    df = pd.DataFrame(rows[1:], columns=rows[0])

    # Resolve columns by position (same logic as dashboard.py)
    col_by_pos = {i: df.columns[i] for i in range(len(df.columns))}
    _remap = {
        C_LANG:    col_by_pos.get(10, C_LANG),
        C_MEDIA:   col_by_pos.get(11, C_MEDIA),
        C_SENT:    col_by_pos.get(12, C_SENT),
        C_PUBTYPE: col_by_pos.get(13, C_PUBTYPE),
        C_TOPIC:   col_by_pos.get(14, C_TOPIC),
        C_SECTOR:  col_by_pos.get(15, C_SECTOR),
        C_REACH:   col_by_pos.get(16, C_REACH),
        C_VALUE:   col_by_pos.get(17, C_VALUE),
    }
    rename_map = {v: k for k, v in _remap.items() if v != k and v in df.columns}
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in ALL_COLS:
        if col not in df.columns:
            df[col] = ""

    parsed = pd.to_datetime(df[C_DATE], format="%d/%m/%Y", errors="coerce")
    bad = parsed.isna() & df[C_DATE].str.strip().astype(bool)
    if bad.any():
        parsed[bad] = pd.to_datetime(df.loc[bad, C_DATE], dayfirst=True, errors="coerce")
    df["_date"] = parsed

    df[C_REACH] = pd.to_numeric(df[C_REACH], errors="coerce")
    df[C_VALUE] = pd.to_numeric(df[C_VALUE], errors="coerce")
    df = df[df[C_TITLE].str.strip().astype(bool) | df[C_SOURCE].str.strip().astype(bool)]
    return df.reset_index(drop=True)


# ══ Date window ══════════════════════════════════════════════════════════════

def get_date_window() -> tuple[datetime, datetime, str]:
    """
    Returns (from_dt, to_dt, label).
    Sunday → window covers Fri 10:00 → Sun 10:00 (weekend summary).
    Mon–Thu → window covers previous day 10:00 → today 10:00.
    All times in Israel local (UTC+3).
    """
    israel_tz = timezone(timedelta(hours=3))
    now = datetime.now(israel_tz)
    today_10 = now.replace(hour=10, minute=0, second=0, microsecond=0)

    if now.weekday() == 6:  # Sunday
        # weekend window: Friday 10:00 → Sunday 10:00
        # weekday() → Mon=0 ... Fri=4 ... Sun=6
        days_since_friday = 2
        from_dt = today_10 - timedelta(days=days_since_friday)
        label = "סיכום סוף שבוע (שישי–ראשון)"
    else:
        from_dt = today_10 - timedelta(days=1)
        label = f"סיכום 24 שעות — {now.strftime('%d/%m/%Y')}"

    return from_dt, today_10, label


def filter_window(df: pd.DataFrame, from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    from_date = from_dt.date()
    to_date   = to_dt.date()
    mask = (df["_date"].dt.date >= from_date) & (df["_date"].dt.date <= to_date)
    return df[mask].copy()


# ══ Chart generation (matplotlib, dark theme) ════════════════════════════════

def _setup_mpl():
    plt.rcParams.update({
        "figure.facecolor":  BG_COLOR,
        "axes.facecolor":    PANEL_COLOR,
        "axes.edgecolor":    "#334155",
        "axes.labelcolor":   TEXT_COLOR,
        "xtick.color":       TEXT_COLOR,
        "ytick.color":       TEXT_COLOR,
        "text.color":        TEXT_COLOR,
        "grid.color":        "#334155",
        "grid.alpha":        0.5,
        "font.size":         11,
    })


def _find_hebrew_font() -> str | None:
    """Return a font path that supports Hebrew, or None."""
    candidates = [
        "Arial", "Tahoma", "David", "FrankRuehl",
        "Noto Sans Hebrew", "DejaVu Sans",
    ]
    for name in candidates:
        try:
            path = fm.findfont(fm.FontProperties(family=name), fallback_to_default=False)
            if path and "Last Resort" not in path:
                return path
        except Exception:
            pass
    return None


def _rtl(text: str) -> str:
    """Apply bidi algorithm for correct RTL display in matplotlib."""
    return get_display(text)


def make_bar_chart(counts: dict, title: str, xlabel: str = "") -> bytes:
    """Generate a horizontal bar chart PNG and return bytes."""
    _setup_mpl()
    items = sorted(counts.items(), key=lambda x: x[1])
    labels = [_rtl(k) for k, _ in items]
    values = [v for _, v in items]
    colors = [ST_PALETTE[i % len(ST_PALETTE)] for i in range(len(labels))]

    font_path = _find_hebrew_font()
    font_prop = fm.FontProperties(fname=font_path) if font_path else None

    fig, ax = plt.subplots(figsize=(8, max(3, len(labels) * 0.5 + 1)),
                           facecolor=BG_COLOR)
    ax.set_facecolor(PANEL_COLOR)

    bars = ax.barh(labels, values, color=colors, height=0.6)

    # Value labels on bars
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", ha="left",
                color=TEXT_COLOR, fontsize=10,
                fontproperties=font_prop)

    ax.set_xlabel(xlabel, color=TEXT_COLOR,
                  fontproperties=font_prop)
    ax.set_title(_rtl(title), color=TEXT_COLOR, fontsize=13, pad=10,
                 fontproperties=font_prop)
    ax.tick_params(colors=TEXT_COLOR)
    if font_prop:
        for lbl in ax.get_yticklabels():
            lbl.set_fontproperties(font_prop)
    ax.grid(axis="x", color="#334155", alpha=0.5)
    ax.spines[:].set_color("#334155")
    ax.set_xlim(0, max(values) * 1.18 if values else 1)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=BG_COLOR)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def make_wordcloud(text: str, title: str) -> bytes:
    """Generate a word-cloud PNG and return bytes."""
    if not text.strip():
        # Return a blank image with a message
        _setup_mpl()
        fig, ax = plt.subplots(figsize=(8, 4), facecolor=BG_COLOR)
        ax.set_facecolor(BG_COLOR)
        ax.text(0.5, 0.5, _rtl("אין מספיק טקסט ליצירת ענן מילים"),
                ha="center", va="center", color=TEXT_COLOR, fontsize=14,
                transform=ax.transAxes)
        ax.axis("off")
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                    facecolor=BG_COLOR)
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    font_path = _find_hebrew_font()

    wc_kwargs = dict(
        background_color=PANEL_COLOR,
        color_func=lambda *a, **kw: ST_PALETTE[hash(kw.get("word", "")) % len(ST_PALETTE)],
        width=1000, height=500,
        max_words=80,
        prefer_horizontal=0.85,
        collocations=False,
    )
    if font_path:
        wc_kwargs["font_path"] = font_path

    # Apply bidi to each word so RTL words render correctly
    words = [w for w in text.split() if len(w) > 1 and w not in _STOP]
    bidi_text = " ".join(get_display(w) for w in words)

    wc = WordCloud(**wc_kwargs).generate(bidi_text)

    _setup_mpl()
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=BG_COLOR)
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")

    font_path_for_title = _find_hebrew_font()
    font_prop = fm.FontProperties(fname=font_path_for_title) if font_path_for_title else None
    ax.set_title(_rtl(title), color=TEXT_COLOR, fontsize=13, pad=8,
                 fontproperties=font_prop)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=BG_COLOR)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def generate_charts(df: pd.DataFrame) -> list[tuple[str, bytes]]:
    """Return list of (filename, png_bytes) for the 3 summary charts."""
    charts = []

    # 1. Language distribution
    lang_counts = df[C_LANG].replace("", "לא ידוע").value_counts().to_dict()
    charts.append(("lang.png", make_bar_chart(lang_counts, "פרסומים לפי שפה")))

    # 2. Media distribution
    media_counts = df[C_MEDIA].replace("", "לא ידוע").value_counts().to_dict()
    charts.append(("media.png", make_bar_chart(media_counts, "פרסומים לפי סוג מדיה")))

    # 3. Word cloud from titles + content
    text_parts = []
    for _, row in df.iterrows():
        title   = str(row.get(C_TITLE, "")).strip()
        content = str(row.get(C_CONTENT, "")).strip()
        text_parts.append(title + " " + content)
    full_text = " ".join(text_parts)
    charts.append(("wordcloud.png", make_wordcloud(full_text, "ענן מילים — כותרות ותוכן")))

    return charts


# ══ Slack helpers ════════════════════════════════════════════════════════════

def _fmt_number(n) -> str:
    if pd.isna(n):
        return "—"
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _article_block(row) -> dict:
    """Build a Slack section block for a single article."""
    title  = str(row.get(C_TITLE, "")).strip() or "ללא כותרת"
    link   = str(row.get(C_LINK,  "")).strip()
    source = str(row.get(C_SOURCE,"")).strip() or "—"
    lang   = str(row.get(C_LANG,  "")).strip() or "—"
    sent   = str(row.get(C_SENT,  "")).strip() or "—"
    reach  = _fmt_number(row.get(C_REACH))
    value  = _fmt_number(row.get(C_VALUE))

    sent_icon  = SENT_EMOJI.get(sent, "😐")
    title_text = f"*<{link}|{title}>*" if link else f"*{title}*"
    line2 = f"🏢 {source}  |  🌐 {lang}  |  {sent_icon} {sent}"
    line3 = f"👁 {reach} חשיפה  |  💰 ₪{value} ערך"

    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"{title_text}\n{line2}\n{line3}"},
    }


def _divider() -> dict:
    return {"type": "divider"}


def upload_chart(client: WebClient, channel: str, filename: str,
                 png_bytes: bytes, title: str) -> str | None:
    """Upload a PNG to Slack and return the file permalink."""
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            f.write(png_bytes)
            tmp_path = f.name

        resp = client.files_upload_v2(
            channel=channel,
            file=tmp_path,
            filename=filename,
            title=title,
        )
        os.unlink(tmp_path)
        return resp.get("file", {}).get("permalink")
    except SlackApiError as e:
        print(f"[ERROR] Failed to upload {filename}: {e.response['error']}", file=sys.stderr)
        return None


def send_header(client: WebClient, channel: str, label: str,
                n_articles: int, from_dt: datetime, to_dt: datetime):
    """Send the summary header message."""
    date_range = f"{from_dt.strftime('%d/%m/%Y %H:%M')} — {to_dt.strftime('%d/%m/%Y %H:%M')}"
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📰 {label}", "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*תקופה:*\n{date_range}"},
                {"type": "mrkdwn", "text": f"*סה\"כ פרסומים:*\n{n_articles}"},
            ],
        },
    ]
    client.chat_postMessage(channel=channel, blocks=blocks, text=label)


def send_articles(client: WebClient, channel: str, df: pd.DataFrame):
    """Send article cards in chunks (Slack max 50 blocks/message)."""
    if df.empty:
        client.chat_postMessage(
            channel=channel,
            text="לא נמצאו פרסומים בתקופה זו.",
        )
        return

    # Sort by reach descending (most important first), NaN last
    df_sorted = df.sort_values(C_REACH, ascending=False, na_position="last")

    # Build all article blocks
    all_blocks = []
    for _, row in df_sorted.iterrows():
        all_blocks.append(_article_block(row))
        all_blocks.append(_divider())
    # Remove trailing divider
    if all_blocks and all_blocks[-1] == _divider():
        all_blocks.pop()

    # Slack allows max 50 blocks per message
    CHUNK = 48
    for i in range(0, len(all_blocks), CHUNK):
        chunk = all_blocks[i:i + CHUNK]
        client.chat_postMessage(
            channel=channel,
            blocks=chunk,
            text=f"פרסומים {i // 2 + 1}–{min((i + CHUNK) // 2, len(df_sorted))}",
        )


# ══ Main ═════════════════════════════════════════════════════════════════════

def main():
    token        = os.environ.get("SLACK_BOT_TOKEN", "")
    channel_id   = os.environ.get("SLACK_CHANNEL_ID", "")
    spreadsheet  = os.environ.get("SPREADSHEET_ID", "")
    sheet_name   = os.environ.get("SHEET_NAME", "עומדים ביחד פרסומים")

    missing = [k for k, v in {
        "SLACK_BOT_TOKEN": token,
        "SLACK_CHANNEL_ID": channel_id,
        "SPREADSHEET_ID": spreadsheet,
        "GOOGLE_CREDENTIALS_JSON": os.environ.get("GOOGLE_CREDENTIALS_JSON", ""),
    }.items() if not v]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    print("[INFO] Connecting to Google Sheets...")
    client_gs = _get_client()
    df_full = load_sheet(client_gs, spreadsheet, sheet_name)
    print(f"[INFO] Loaded {len(df_full)} total rows")

    from_dt, to_dt, label = get_date_window()
    df = filter_window(df_full, from_dt, to_dt)
    print(f"[INFO] {len(df)} articles in window: {label}")

    slack = WebClient(token=token)

    # Send header
    send_header(slack, channel_id, label, len(df), from_dt, to_dt)

    # Send article cards
    print("[INFO] Sending article cards...")
    send_articles(slack, channel_id, df)

    print("[INFO] Done.")


if __name__ == "__main__":
    main()
