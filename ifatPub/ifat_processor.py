#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ifat_processor.py
-----------------
עיבוד קבצי PDF מ-יפעת → Google Sheets + Google Drive

הפעלה:
  python ifat_processor.py                       # עבד את כל ה-PDFs החדשים בתיקייה
  python ifat_processor.py --process file.pdf    # עבד קובץ ספציפי
  python ifat_processor.py --watch               # האזן לתיקייה ועבד קבצים חדשים אוטומטית

עריכת רשימת דמויות: characters.json
הגדרות: ifat_config.json
"""

from __future__ import annotations

import sys
import io
# Force UTF-8 output on Windows console
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import json
import os
import re
import tempfile
import time
import traceback
import unicodedata
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Optional
from datetime import datetime
from urllib.parse import quote

import pdfplumber
from pypdf import PdfReader, PdfWriter
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from bidi.algorithm import get_display
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "ifat_config.json"
CHARACTERS_FILE = BASE_DIR / "characters.json"
STATE_FILE = BASE_DIR / "processed_pdfs.json"

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── Optional OCR support (for image-based print articles) ──────────────────
try:
    import pytesseract
    from PIL import Image as _PILImage  # noqa: F401

    pytesseract.pytesseract.tesseract_cmd = (
        r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    )
    _TESSDATA_DIR = str(BASE_DIR / "tessdata")   # local heb+eng tessdata
    _OCR_AVAILABLE = True
except Exception:
    _OCR_AVAILABLE = False

SHEET_HEADERS = [
    "תאריך",           # A
    "שעה",              # B
    "גוף תקשורת",      # C
    "מדור",             # D  מדור/תת-מקור מיפעת
    "כותרת",            # E
    "תוכן",             # F
    "כתב",              # G
    "דמויות",           # H
    "קישור",            # I
    "מספר סידורי",     # J  (לשימוש פנימי - מניעת כפילויות)
    "שפת פרסום",       # K  עברית / ערבית / אנגלית / רוסית
    "מדיה",             # L  אינטרנט / טלוויזיה / רדיו / עיתונות
    "סנטימנט",          # M  חיובי / ניטרלי / שלילי
    "סוג פרסום",        # N  dropdown: איזכור / אינסרט / ידיעה / …
    "נושא",             # O  dropdown (מרובה): דמויות ציבוריות / כן שלום / …
    "מגזר",             # P  ← ממולא מגיליון האינדקס (לפרסומים חדשים בלבד)
    "חשיפה",            # Q  audienceRating מיפעת
    "ערך",              # R  itemValue מיפעת
]

# ── Dropdown values ───────────────────────────────────────────────────────────
PUB_TYPE_OPTIONS = [
    "איזכור", "אינסרט", "ידיעה", "טור דעה", "כותרת",
    "לינק", "מודעה", "סינק", "סיקור", "פולו",
    "קרדיט", "ראיון", "תגובתי",
]

TOPIC_OPTIONS = [
    "דמויות ציבוריות",
    "המשמר ההומניטרי",
    "התנגדות למלחמה",
    "חברתי כלכלי",
    "חוסן",
    "כללי",
    "כן שלום",
    "מיגון",
    "מעגלים",
    "סביבתי",
    "סטודנטים",
    "עיר סגולה",
    "רוב העיר",
    "רוזה מדיה",
    "שיבושים",
    "שלום ישראלי פלסטיני",
]


# ============================================================
# Config + state
# ============================================================

def load_config() -> dict:
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


def load_characters() -> list:
    with open(CHARACTERS_FILE, encoding="utf-8") as f:
        return json.load(f)


def load_state() -> set:
    if not STATE_FILE.exists():
        return set()
    with open(STATE_FILE, encoding="utf-8") as f:
        return set(json.load(f).get("processed", []))


def save_state(processed: set):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"processed": sorted(processed)}, f, ensure_ascii=False, indent=2)


# ============================================================
# Character matching
# ============================================================

def _norm(text: str) -> str:
    """Normalize for fuzzy matching: NFC, lowercase, collapsed spaces."""
    text = unicodedata.normalize("NFC", text or "")
    return " ".join(text.lower().split())


def find_character(search_text: str, characters: list) -> str:
    """Return canonical name if any variant appears in search_text, else ''."""
    norm = _norm(search_text)
    for char in characters:
        for variant in char.get("variants", [char["canonical"]]):
            if _norm(variant) in norm:
                return char["canonical"]
    return ""


# ============================================================
# Regex patterns
# ============================================================

DATE_RE     = re.compile(r"\d{1,2}/\d{1,2}/\d{4}")
TIME_RE     = re.compile(r"\b\d{1,2}:\d{2}\b")
SERIAL_RE   = re.compile(r"\b(\d{7,9})\b")
URL_RE      = re.compile(r"https?://\S+")
PAGE_IND_RE = re.compile(r"^(\d+)/(\d+)$")          # e.g. "1/2"
DIM_LINE_RE = re.compile(r"\d+\.\d+x\d+\.\d+")      # Print article dimension table


# ============================================================
# Line utilities
# ============================================================

def _fix_rtl(line: str) -> str:
    """Fix RTL Hebrew text that was extracted in reversed order by pdfplumber."""
    if re.search(r"[\u0590-\u05FF\uFB1D-\uFB4F]", line):
        return get_display(line)
    return line


def extract_lines(text: str) -> list[str]:
    """Return non-empty lines from extracted PDF text, with bidi correction applied."""
    return [_fix_rtl(l.strip()) for l in (text or "").splitlines() if l.strip()]


def _is_interviewee_line(text: str) -> bool:
    """Check whether a raw or fixed line is a מרואיינים line."""
    return "מרואיינ" in text or "מרואיינ" in _fix_rtl(text)


# ============================================================
# Paragraph block detection (uses raw text to find blank-line breaks)
# ============================================================

def get_body_blocks(raw_text: str) -> list[list[str]]:
    """
    Parse the raw (un-stripped) extracted text into paragraph blocks.
    Blank lines separate paragraphs.  מרואיינים lines are skipped.

    Returns: list of paragraphs, each paragraph is a list of bidi-corrected lines.
    """
    raw_lines = raw_text.splitlines()

    # Find the first serial-bearing line that is NOT a URL line (start of body)
    serial_raw_idx = -1
    for i, rl in enumerate(raw_lines):
        s = rl.strip()
        if s and SERIAL_RE.search(s) and not URL_RE.search(s):
            serial_raw_idx = i
            break
    # Fallback: if serial only appears in URL, anchor on the date line
    if serial_raw_idx == -1:
        for i, rl in enumerate(raw_lines[:8]):
            if DATE_RE.search(rl):
                serial_raw_idx = i
                break
    if serial_raw_idx == -1:
        return []

    # Find the URL line (end of body)
    url_raw_idx = len(raw_lines)
    for i in range(serial_raw_idx, len(raw_lines)):
        if URL_RE.search(raw_lines[i]):
            url_raw_idx = i
            break

    # Group body text into paragraphs
    body_raw   = raw_lines[serial_raw_idx + 1 : url_raw_idx]
    blocks:  list[list[str]] = []
    current: list[str]       = []

    for rl in body_raw:
        stripped = rl.strip()
        if stripped:
            if _is_interviewee_line(stripped):
                # Flush current paragraph before skipping this line
                if current:
                    blocks.append(current)
                    current = []
                continue
            current.append(_fix_rtl(stripped))
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)

    return blocks


# ============================================================
# Field extractors (metadata)
# ============================================================

def find_url(lines: list[str]) -> tuple[Optional[str], Optional[int]]:
    for i, line in enumerate(lines):
        m = URL_RE.search(line)
        if m:
            return m.group(0), i
    return None, None


def find_date_time(lines: list[str]) -> tuple[str, str]:
    date_str = time_str = ""
    for line in lines[:8]:
        if not date_str:
            m = DATE_RE.search(line)
            if m:
                date_str = m.group(0)
        if not time_str:
            m = TIME_RE.search(line)
            if m:
                candidate = m.group(0)
                if not re.fullmatch(r"\d{4}", candidate):
                    time_str = candidate
        if date_str and time_str:
            break
    return date_str, time_str


def find_serial_and_reporter(lines: list[str]) -> tuple[str, str]:
    """
    Find the serial number and reporter name.
    - Skips מרואיינים lines (saves serial as fallback).
    - Skips URL lines (serial inside a URL is not a standalone serial).
    - 'כתב העיתון' / 'כתב' alone → no named reporter.
    """
    skip_prefixes = {"כתב העיתון", "כתב", ""}
    fallback_serial = ""

    for line in lines[:12]:
        m = SERIAL_RE.search(line)
        if not m:
            continue
        serial = m.group(1)
        # URL line → serial is embedded in a URL, not a standalone identifier
        if URL_RE.search(line):
            if not fallback_serial:
                fallback_serial = serial
            continue
        # מרואיינים line → save serial as fallback, don't use its text as reporter
        if _is_interviewee_line(line):
            if not fallback_serial:
                fallback_serial = serial
            continue
        # Normal line
        before = line[: m.start()].strip().rstrip(",").strip()
        before = before.rstrip("- /").strip()
        if before in skip_prefixes:
            return serial, ""
        return serial, before

    return fallback_serial, ""


def get_page_indicator(lines: list[str]) -> tuple[Optional[int], Optional[int]]:
    """Detect X/Y multi-page print indicator, e.g. 'haaretz, 1/2'."""
    for line in lines[:5]:
        parts = re.split(r"[,\s]+", line)
        for part in parts:
            m = PAGE_IND_RE.fullmatch(part.strip())
            if m:
                cur, total = int(m.group(1)), int(m.group(2))
                if 1 <= cur <= total <= 30:
                    return cur, total
    return None, None


def find_source_section(lines: list[str]) -> tuple[str, str]:
    """
    Find source (גוף תקשורת) and section (מדור) from page header.
    The source line is the first of the first 5 lines that is NOT a
    date/time/serial/URL/interviewee line.
    """
    for line in lines[:6]:
        if DATE_RE.fullmatch(line) or TIME_RE.fullmatch(line):
            continue
        if DATE_RE.search(line) and TIME_RE.search(line):
            continue
        if SERIAL_RE.fullmatch(line):
            continue
        if URL_RE.search(line):
            continue
        if _is_interviewee_line(line):
            continue
        raw = line
        # Strip page-indicator suffix, e.g. "haaretz, 1/2" → "haaretz"
        comma_pos = raw.rfind(",")
        if comma_pos != -1:
            right = raw[comma_pos + 1:].strip()
            if PAGE_IND_RE.fullmatch(right):
                raw = raw[:comma_pos].strip()
        if "," in raw:
            left, right = raw.split(",", 1)
            return left.strip(), right.strip()
        for sep in (" - ", " – ", "- ", " -"):
            if sep in raw:
                idx = raw.find(sep)
                return raw[:idx].strip(), raw[idx + len(sep):].strip()
        return raw.strip(), ""
    return "", ""


def find_interviewees(lines: list[str]) -> str:
    """Extract interviewee names; strip any trailing serial number."""
    for line in lines:
        if _is_interviewee_line(line):
            parts = line.split(":", 1)
            if len(parts) == 2:
                names = parts[1].strip()
                # Remove trailing serial numbers
                names = SERIAL_RE.sub("", names).strip().rstrip(",- ").strip()
                return names
            return ""
    return ""


def find_serial_line_idx(lines: list[str]) -> Optional[int]:
    """Return the index of the first non-URL line that contains a serial number.
    Falls back to the date-line index when serial is only found inside URLs."""
    for i, line in enumerate(lines[:12]):
        if SERIAL_RE.search(line) and not URL_RE.search(line):
            return i
    # Serial only appears inside a URL — use last date/time line as boundary
    for i, line in enumerate(lines[:6]):
        if DATE_RE.search(line) or TIME_RE.search(line):
            last_header_idx = i
    try:
        return last_header_idx
    except UnboundLocalError:
        return None


def find_title_and_content(
    lines: list[str],
    url_idx: int,
    serial_idx: Optional[int],
    body_blocks: Optional[list[list[str]]] = None,
) -> tuple[str, str]:
    """
    For online articles: extract title and content.

    Logic (in order of priority):
    1. body_blocks (from raw-text paragraph detection):
       - Single paragraph ≤ 400 chars  → all = title, content = ""
       - Multiple paragraphs            → first = title, rest = content
    2. Fallback: first non-empty line = title, rest = content.

    מרואיינים lines are excluded from the body before processing.
    """
    start      = (serial_idx + 1) if serial_idx is not None else 0
    body_lines = [l for l in lines[start:url_idx] if l and not _is_interviewee_line(l)]

    if not body_lines:
        return "", ""

    if body_blocks:
        if len(body_blocks) == 1:
            joined = " ".join(body_blocks[0])
            if len(joined) <= 400:
                # Short single paragraph → all title (radio clips, quotes, etc.)
                return joined, ""
            # Long single paragraph → first line is title
            return body_blocks[0][0], "\n".join(body_blocks[0][1:])
        if len(body_blocks) >= 2:
            title        = " ".join(body_blocks[0])
            content_parts = [" ".join(b) for b in body_blocks[1:]]
            return title, "\n".join(content_parts)

    # Fallback
    return body_lines[0], "\n".join(body_lines[1:])


# ============================================================
# OCR helpers (image-based print articles)
# ============================================================

def _ocr_page(pdf_page, crop_top_pts: float = 0.0) -> str:
    """
    Rasterize `pdf_page` (below `crop_top_pts` in PDF points) and run Tesseract
    on the image.  Returns the raw OCR string, or "" if OCR is unavailable.
    """
    if not _OCR_AVAILABLE:
        return ""
    try:
        RESOLUTION = 200                     # dpi – good balance of speed / quality
        page_img   = pdf_page.to_image(resolution=RESOLUTION)
        pil_img    = page_img.original       # PIL.Image

        width, height = pil_img.size
        scale         = RESOLUTION / 72.0   # PDF points → pixels
        crop_top_px   = int(crop_top_pts * scale) + 5

        if crop_top_px >= height - 20:
            return ""

        article_img = pil_img.crop((0, crop_top_px, width, height))
        # Set TESSDATA_PREFIX via env so Tesseract finds the language files
        env_backup = os.environ.get("TESSDATA_PREFIX", "")
        os.environ["TESSDATA_PREFIX"] = _TESSDATA_DIR
        try:
            ocr_text = pytesseract.image_to_string(
                article_img,
                lang="heb+eng",
                config="--oem 3 --psm 3",
            )
        finally:
            os.environ["TESSDATA_PREFIX"] = env_backup
        return ocr_text
    except Exception as exc:
        print(f"    אזהרה OCR: {exc}")
        return ""


def _is_ocr_noise(line: str) -> bool:
    """Return True if a line from OCR output looks like header/metadata noise."""
    s = line.strip()
    if len(s) < 4:
        return True
    # Pure date / time
    if DATE_RE.fullmatch(s) or TIME_RE.fullmatch(s):
        return True
    # Serial number alone
    if SERIAL_RE.fullmatch(s):
        return True
    # Dimension table (e.g. "20x15cm")
    if DIM_LINE_RE.search(s):
        return True
    # Newspaper name + date/page  e.g. "haaretz, 172 12/04/2026"
    if re.match(r"^[A-Za-z]", s) and DATE_RE.search(s):
        return True
    # Line that's almost entirely digits/punctuation (page numbers, etc.)
    non_digit = re.sub(r"[\d/\-\.\s,:*]", "", s)
    if len(non_digit) < 2:
        return True
    return False


def _parse_ocr_blocks(text: str) -> list[list[str]]:
    """
    Split raw OCR text into paragraph blocks (blank-line separated).
    Each block is a list of bidi-corrected non-empty lines.
    Metadata / noise lines are filtered out.
    """
    blocks: list[list[str]] = []
    current: list[str]      = []

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or len(stripped) <= 2:
            if current:
                blocks.append(current)
                current = []
            continue
        if _is_ocr_noise(stripped):
            # Treat noise as a block separator (flush current)
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(_fix_rtl(stripped))

    if current:
        blocks.append(current)
    return blocks


# ============================================================
# Print article: font-size based title / section extraction
# ============================================================

def _chars_to_line_groups(pdf_page) -> list[tuple[str, float]]:
    """
    Group page characters by y-position.
    Returns list of (raw_text, max_font_size) per visual line, sorted top→bottom.
    """
    groups: dict[int, list] = defaultdict(list)
    for ch in (getattr(pdf_page, "chars", []) or []):
        y_key = round(float(ch.get("top", 0)))
        groups[y_key].append(ch)

    result = []
    for y in sorted(groups):
        row  = sorted(groups[y], key=lambda c: float(c.get("x0", 0)))
        text = "".join(c.get("text", "") for c in row).strip()
        if not text:
            continue
        sizes    = [float(c.get("size", 10)) for c in row if c.get("size")]
        max_size = max(sizes) if sizes else 10.0
        result.append((text, max_size))
    return result


def find_print_title_content(
    lines: list[str],
    pdf_page,
    serial: str = "",
) -> tuple[str, str, str]:
    """
    For print articles:
    - Use font-size data from pdfplumber to find the headline (large font).
    - Extract section from the dimension-table line (e.g. "עמוד 6" or "haaretz-front").
    Returns (title, content, section_override).
    """
    line_data = _chars_to_line_groups(pdf_page)

    # ── Section detection ────────────────────────────────────────────────────
    section_override = ""
    for raw_text, _ in line_data:
        fixed = _fix_rtl(raw_text)
        # "עמוד N" or "N עמוד" (handles both pre- and post-bidi orderings)
        m = re.search(r"(?:(\d+)\s*עמוד|עמוד\s*(\d+))", fixed)
        if m:
            page_num         = m.group(1) or m.group(2)
            section_override = f"עמוד {page_num}"
            break
        if DIM_LINE_RE.search(raw_text):
            # Try section name like "haaretz-front"
            m2 = re.search(r"([A-Za-z][A-Za-z0-9]*-[A-Za-z][A-Za-z0-9]*)", raw_text)
            if m2:
                section_override = m2.group(1)
            break  # Dimension table found, stop searching for section

    # Fallback: scan plain lines list
    if not section_override:
        for line in lines:
            m = re.search(r"(?:(\d+)\s*עמוד|עמוד\s*(\d+))", line)
            if m:
                page_num         = m.group(1) or m.group(2)
                section_override = f"עמוד {page_num}"
                break

    if not line_data:
        return "", "", section_override

    # ── Find where the יפעת header ends ─────────────────────────────────────
    # Strategy 1: dimension table line (most reliable)
    header_end_idx = 0
    for i, (raw_text, _) in enumerate(line_data[:25]):
        if DIM_LINE_RE.search(raw_text):
            header_end_idx = i + 1
            break

    # Strategy 2: serial number in line_data (for articles without dim table)
    if not header_end_idx and serial:
        for i, (raw_text, _) in enumerate(line_data[:15]):
            if serial in raw_text:
                header_end_idx = i + 2   # Skip serial line + one more
                break

    # Strategy 3: use text-based serial line index as an approximation
    if not header_end_idx:
        serial_idx_approx = find_serial_line_idx(lines)
        if serial_idx_approx is not None:
            header_end_idx = min(serial_idx_approx + 2, len(line_data))
        else:
            header_end_idx = min(5, len(line_data) // 3)

    article_lines = line_data[header_end_idx:]

    # Filter remaining metadata-like lines
    article_clean: list[tuple[str, float]] = []
    for raw_text, size in article_lines:
        fixed = _fix_rtl(raw_text)
        if DIM_LINE_RE.search(raw_text):
            continue
        # Subject/keyword codes like "24542 - שלום ישראלי פלסטיני"
        if re.match(r"^\d{4,6}\s*[-–]", fixed):
            continue
        # Lines starting with ":" (יפעת metadata caption)
        if re.match(r"^:\s*\S", fixed):
            continue
        if len(raw_text.strip()) < 2:
            continue
        article_clean.append((fixed, size))

    if not article_clean:
        # Image-based PDF: no selectable text → try OCR
        if pdf_page is not None:
            # Estimate where the יפעת header ends in PDF points
            header_bottom_pts = 0.0

            if line_data:
                # We have character data: use y-coordinate of last header line
                groups: dict[int, list] = defaultdict(list)
                for ch in (getattr(pdf_page, "chars", []) or []):
                    y_key = round(float(ch.get("top", 0)))
                    groups[y_key].append(ch)
                sorted_ys = sorted(groups.keys())
                idx = min(max(header_end_idx, 1), len(sorted_ys)) - 1
                if sorted_ys:
                    header_bottom_pts = float(sorted_ys[idx])
            else:
                # No character data at all (truly rasterised page).
                # Estimate header height from the extracted text lines.
                serial_idx_approx = find_serial_line_idx(lines)
                if serial_idx_approx is not None:
                    # ~15 PDF points per text line, +1 line of margin
                    header_bottom_pts = (serial_idx_approx + 2) * 15.0
                else:
                    header_bottom_pts = 70.0   # safe default: ~70 pts ≈ top ~10%

            ocr_text = _ocr_page(pdf_page, crop_top_pts=header_bottom_pts)
            if ocr_text.strip():
                ocr_blocks = _parse_ocr_blocks(ocr_text)
                if ocr_blocks:
                    if len(ocr_blocks) == 1:
                        joined = " ".join(ocr_blocks[0])
                        if len(joined) <= 400:
                            return joined, "", section_override
                        return ocr_blocks[0][0], "\n".join(ocr_blocks[0][1:]), section_override
                    title        = " ".join(ocr_blocks[0])
                    content_parts = [" ".join(b) for b in ocr_blocks[1:]]
                    return title, "\n".join(content_parts), section_override
        return "", "", section_override

    # ── Determine body vs title font sizes ──────────────────────────────────
    body_candidates = [s for t, s in article_clean if len(t) >= 20]
    if body_candidates:
        body_size = median(body_candidates)
    else:
        body_size = min(s for _, s in article_clean)

    title_threshold = body_size * 1.3   # title font ≥ 130 % of body font

    # ── Collect title lines (large font) then content lines ─────────────────
    title_parts:   list[str] = []
    content_parts: list[str] = []
    past_title = False

    for text, size in article_clean:
        if not past_title and size >= title_threshold:
            title_parts.append(text)
        else:
            past_title = True
            content_parts.append(text)

    title   = " ".join(title_parts)
    content = "\n".join(content_parts)
    return title, content, section_override


# ============================================================
# Page parser
# ============================================================

def parse_page(
    lines: list[str],
    raw_text: str = "",
    pdf_page=None,
) -> Optional[dict]:
    if not lines:
        return None

    url, url_idx       = find_url(lines)
    is_print           = url is None
    date_str, time_str = find_date_time(lines)
    source, section    = find_source_section(lines)
    serial, reporter   = find_serial_and_reporter(lines)
    interviewees       = find_interviewees(lines)
    cur_pg, total_pg   = get_page_indicator(lines)

    if is_print:
        if pdf_page is not None:
            title, content, sec_override = find_print_title_content(lines, pdf_page, serial=serial)
            if sec_override:
                section = sec_override
        else:
            title = content = ""
        link = ""
    else:
        serial_idx  = find_serial_line_idx(lines)
        body_blocks = get_body_blocks(raw_text) if raw_text else None
        title, content = find_title_and_content(lines, url_idx, serial_idx, body_blocks)
        link = url

    # Language detection from article text
    lang = _detect_language((title or "") + " " + (content or "") + " " + (source or ""))

    return {
        "date":         date_str,
        "time":         time_str,
        "source":       source,
        "section":      section,
        "title":        title,
        "content":      content,
        "reporter":     reporter,
        "interviewees": interviewees,
        "link":         link,
        "serial":       serial,
        "is_print":     is_print,
        "page_num":     cur_pg,
        "total_pages":  total_pg,
        # new metadata columns
        "language":     lang,
        "media":        "עיתונות" if is_print else "אינטרנט",
        "sentiment":    "",     # not available for PDFs
        "pub_type":     "ידיעה",
    }


# ============================================================
# Enrich article with character matching
# ============================================================

def enrich(data: dict, characters: list) -> dict:
    """
    Assign reporter_col (G) and character_col (H):
    - G = explicit reporter (if not an ST figure); or non-ST interviewees when
          there is no explicit reporter.
    - H = only ST figures (from reporter, interviewees, or full-text scan).
    """
    reporter         = data.get("reporter", "")
    interviewees_str = data.get("interviewees", "")

    # Split interviewees into individual names
    interviewee_names = (
        [n.strip() for n in re.split(r"[,،]", interviewees_str) if n.strip()]
        if interviewees_str else []
    )

    # Classify each interviewee: ST figure → H, otherwise → G
    st_interviewees:     list[str] = []
    non_st_interviewees: list[str] = []
    for name in interviewee_names:
        char = find_character(name, characters)
        if char:
            if char not in st_interviewees:
                st_interviewees.append(char)
        else:
            non_st_interviewees.append(name)

    # Check reporter
    reporter_char = find_character(reporter, characters) if reporter else ""

    # ── Column G (כתב) ──────────────────────────────────────────────────────
    if reporter_char:
        # Reporter is an ST figure → move to H, G gets non-ST interviewees
        g_col = ", ".join(non_st_interviewees)
    elif reporter:
        # Regular reporter in G
        g_col = reporter
    else:
        # No explicit reporter → non-ST interviewees go to G
        g_col = ", ".join(non_st_interviewees)

    # ── Column H (דמויות) ────────────────────────────────────────────────────
    h_figures: list[str] = []
    if reporter_char and reporter_char not in h_figures:
        h_figures.append(reporter_char)
    for name in st_interviewees:
        if name not in h_figures:
            h_figures.append(name)

    # Also scan full text for any additional ST mentions not yet captured
    full_text = " ".join([
        data.get("title",   ""),
        data.get("content", ""),
        interviewees_str,
        reporter,
    ])
    for char in characters:
        canonical = char["canonical"]
        if canonical not in h_figures and find_character(full_text, [char]):
            h_figures.append(canonical)

    data["reporter_col"]  = g_col
    data["character_col"] = ", ".join(h_figures)
    return data


# ============================================================
# Google Drive – upload print article pages
# ============================================================

def _get_drive_service(config: dict):
    creds_file = BASE_DIR / config["credentials_file"]
    creds = Credentials.from_service_account_file(str(creds_file), scopes=GOOGLE_SCOPES)
    return build("drive", "v3", credentials=creds)


def _get_or_create_drive_folder(drive, parent_id: str, folder_name: str) -> str:
    """Return the Drive folder-id for `folder_name` inside `parent_id`,
    creating the folder if it does not exist yet."""
    q = (
        f"name='{folder_name}' "
        "and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents "
        "and trashed=false"
    )
    res   = drive.files().list(
        q=q, fields="files(id)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    folder = drive.files().create(
        body={
            "name":     folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents":  [parent_id],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return folder["id"]


def upload_pages_to_drive(
    pdf_path: Path,
    page_indices: list[int],
    article_data: dict,
    config: dict,
) -> str:
    """
    Extract the given pages from the source PDF, upload to Google Drive inside a
    year/month/day subfolder hierarchy, and return the shareable webViewLink.
    """
    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for idx in page_indices:
        writer.add_page(reader.pages[idx])

    date_str = article_data.get("date", "unknown").replace("/", "-")   # DD-MM-YYYY
    source   = re.sub(r"[^\w\-]", "_", article_data.get("source", "unknown"))[:30]
    serial   = article_data.get("serial", "")
    fname    = f"{date_str}_{source}_{serial}.pdf"

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = tmp.name
            writer.write(tmp)

        drive     = _get_drive_service(config)
        folder_id = config.get("drive_folder_id", "")

        # Build year / month / day subfolder hierarchy
        target_folder = folder_id
        if folder_id:
            try:
                dt           = datetime.strptime(article_data.get("date", ""), "%d/%m/%Y")
                year_folder  = _get_or_create_drive_folder(drive, folder_id,   str(dt.year))
                month_folder = _get_or_create_drive_folder(drive, year_folder,  f"{dt.month:02d}")
                day_folder   = _get_or_create_drive_folder(drive, month_folder, f"{dt.day:02d}")
                target_folder = day_folder
            except Exception:
                pass  # Fall back to root folder on any date-parse or API error

        uploaded = drive.files().create(
            body={"name": fname, "parents": [target_folder]},
            media_body=MediaFileUpload(tmp_path, mimetype="application/pdf", resumable=False),
            fields="id, webViewLink",
            supportsAllDrives=True,
        ).execute()

        # Make file readable by anyone with the link
        drive.permissions().create(
            fileId=uploaded["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()

        return uploaded.get("webViewLink", "")

    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except PermissionError:
                pass


# ============================================================
# Full PDF processing (handles multi-page print articles)
# ============================================================

def process_pdf(
    pdf_path: Path,
    characters: list,
    config: dict,
    skip_upload: bool = False,
) -> list[dict]:
    """Process an entire PDF and return a list of enriched article dicts.

    skip_upload=True  → don't upload print pages to Drive (used in --update-titles
                        mode where the Drive link already exists in the sheet).
    """
    articles:  list[dict]       = []
    multi_buf: Optional[dict]   = None   # {total, pages:[int], data:dict}

    def flush_multi(buf: dict):
        if not skip_upload:
            drive_link = upload_pages_to_drive(pdf_path, buf["pages"], buf["data"], config)
            buf["data"]["link"] = drive_link
        enrich(buf["data"], characters)
        articles.append(buf["data"])

    def flush_single(data: dict, page_idx: int):
        if data["is_print"] and not skip_upload:
            drive_link   = upload_pages_to_drive(pdf_path, [page_idx], data, config)
            data["link"] = drive_link
        enrich(data, characters)
        articles.append(data)

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_idx, pdf_page in enumerate(pdf.pages):
            text  = pdf_page.extract_text() or ""
            lines = extract_lines(text)
            if not lines:
                continue

            data = parse_page(lines, raw_text=text, pdf_page=pdf_page)
            if data is None:
                continue

            cur_pg   = data.get("page_num")
            total_pg = data.get("total_pages")
            is_multi = cur_pg and total_pg and total_pg > 1

            if is_multi:
                if cur_pg == 1:
                    if multi_buf:
                        flush_multi(multi_buf)
                    multi_buf = {"total": total_pg, "pages": [page_idx], "data": data}
                else:
                    if multi_buf and len(multi_buf["pages"]) == cur_pg - 1:
                        multi_buf["pages"].append(page_idx)
                        if cur_pg == multi_buf["total"]:
                            flush_multi(multi_buf)
                            multi_buf = None
                    else:
                        if multi_buf:
                            flush_multi(multi_buf)
                            multi_buf = None
                        flush_single(data, page_idx)
            else:
                if multi_buf:
                    flush_multi(multi_buf)
                    multi_buf = None
                flush_single(data, page_idx)

    if multi_buf:
        flush_multi(multi_buf)

    return articles


# ============================================================
# Google Sheets
# ============================================================

def _encode_url(url: str) -> str:
    """Percent-encode non-ASCII characters (e.g. Hebrew) in a URL so that
    Google Sheets recognises the value as a clickable hyperlink."""
    if not url or not url.startswith("http"):
        return url
    return quote(url, safe=";/?:@&=+$,#%-._~!'()*[]")


# Shared gspread client + spreadsheet — created once per run to avoid
# duplicate-auth race conditions that can cause partial get_all_values() reads.
_gspread_client_cache: dict = {}

def _get_spreadsheet(config: dict):
    """Return a cached (client, spreadsheet) pair for the configured spreadsheet."""
    key = config["spreadsheet_id"]
    if key not in _gspread_client_cache:
        creds_file  = BASE_DIR / config["credentials_file"]
        creds       = Credentials.from_service_account_file(str(creds_file), scopes=GOOGLE_SCOPES)
        client      = gspread.authorize(creds)
        spreadsheet = client.open_by_key(key)
        _gspread_client_cache[key] = (client, spreadsheet)
    return _gspread_client_cache[key]


def _get_worksheet(config: dict):
    _, spreadsheet = _get_spreadsheet(config)
    sheet_name = config.get("sheet_name", "פרסומים")

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows="2000", cols="20")

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(SHEET_HEADERS)

    return ws


def _row_sort_key(row: list) -> datetime:
    """Sort key for a data row: parse date (col A) + time (col B)."""
    date_str = row[0] if len(row) > 0 else ""
    time_str = row[1] if len(row) > 1 else ""
    try:
        if time_str:
            return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
        return datetime.strptime(date_str, "%d/%m/%Y")
    except ValueError:
        return datetime.min


def _set_dropdown_validation(ws, num_data_rows: int):
    """
    Set data-validation dropdowns on columns N (סוג פרסום) and O (נושא).
    Column N  → index 13 (0-based) — strict single-select from PUB_TYPE_OPTIONS
    Column O  → index 14           — non-strict (allows free text / multi-value)
                                     from TOPIC_OPTIONS
    """
    if num_data_rows <= 0:
        return

    def _validation_request(col_idx: int, options: list[str], strict: bool) -> dict:
        return {
            "setDataValidation": {
                "range": {
                    "sheetId":          ws.id,
                    "startRowIndex":    1,
                    "endRowIndex":      1 + num_data_rows,
                    "startColumnIndex": col_idx,
                    "endColumnIndex":   col_idx + 1,
                },
                "rule": {
                    "condition": {
                        "type":   "ONE_OF_LIST",
                        "values": [{"userEnteredValue": v} for v in options],
                    },
                    "strict":      strict,
                    "showCustomUi": True,
                },
            }
        }

    try:
        ws.spreadsheet.batch_update({"requests": [
            _validation_request(13, PUB_TYPE_OPTIONS, strict=False),
            _validation_request(14, TOPIC_OPTIONS,    strict=False),
        ]})
    except Exception as e:
        print(f"    אזהרה: לא ניתן להגדיר dropdown: {e}")


def _set_row_heights(ws, num_data_rows: int, height_px: int = 21):
    """Set all data rows (header excluded) to a fixed pixel height and CLIP
    wrap strategy so that multi-line content never auto-expands a row."""
    if num_data_rows <= 0:
        return
    try:
        body = {
            "requests": [
                # 1. Fixed pixel height
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId":    ws.id,
                            "dimension":  "ROWS",
                            "startIndex": 1,           # 0-indexed; skip header row
                            "endIndex":   1 + num_data_rows,
                        },
                        "properties": {"pixelSize": height_px},
                        "fields":     "pixelSize",
                    }
                },
                # 2. CLIP wrap so long / multi-line text never expands the row
                {
                    "repeatCell": {
                        "range": {
                            "sheetId":       ws.id,
                            "startRowIndex": 1,
                            "endRowIndex":   1 + num_data_rows,
                        },
                        "cell": {
                            "userEnteredFormat": {"wrapStrategy": "CLIP"}
                        },
                        "fields": "userEnteredFormat.wrapStrategy",
                    }
                },
            ]
        }
        ws.spreadsheet.batch_update(body)
    except Exception as e:
        print(f"    אזהרה: לא ניתן לקבוע גובה שורות: {e}")


def append_to_sheet(articles: list[dict], config: dict, update_empty: bool = False,
                    sheet_name: Optional[str] = None):
    """Write articles to the Google Sheet — APPEND ONLY, never clears the sheet.

    New rows are added at the bottom.
    Existing rows (matched by serial in column J) are updated in-place
    only for cells that are currently empty.
    Manually-entered rows are never touched.
    """
    if sheet_name:
        config = {**config, "sheet_name": sheet_name}
    if not articles:
        return

    ws = _get_worksheet(config)

    # --- Read existing serials + row positions ---
    # We only need column J (serial, index 9) to detect duplicates,
    # and the full row only for rows we might need to update.
    all_values = ws.get_all_values()
    raw_data   = all_values[1:] if len(all_values) > 1 else []

    # Build serial → (sheet_row_number_1based, row_data)
    # sheet_row_number: header=1, first data row=2, …
    serial_to_sheetrow: dict[str, tuple] = {}
    for i, row in enumerate(raw_data, start=2):   # 1-based; row 1 = header
        s = row[9].strip() if len(row) > 9 else ""
        if s and s not in serial_to_sheetrow:
            serial_to_sheetrow[s] = (i, row)

    known_serials: set[str] = set(serial_to_sheetrow.keys())

    # --- Classify each article ---
    new_rows:    list[list]            = []   # articles to append
    row_updates: list[tuple[int,list]] = []   # (sheet_row_num, full_row_data)
    skipped = updated = 0

    for a in articles:
        serial    = a.get("serial", "").strip()
        new_title = a.get("title",  "").strip()

        if serial and serial in known_serials:
            sheet_row, old_row = serial_to_sheetrow[serial]

            existing_title = old_row[4].strip()  if len(old_row) > 4  else ""
            existing_chars = old_row[7].strip()  if len(old_row) > 7  else ""
            existing_lang  = old_row[10].strip() if len(old_row) > 10 else ""
            existing_media = old_row[11].strip() if len(old_row) > 11 else ""
            existing_sent  = old_row[12].strip() if len(old_row) > 12 else ""
            existing_ptype = old_row[13].strip() if len(old_row) > 13 else ""
            existing_topic = old_row[14].strip() if len(old_row) > 14 else ""

            new_chars    = a.get("character_col", "").strip()
            new_reporter = a.get("reporter_col",  "").strip()
            new_lang     = a.get("language",      "").strip()
            new_media    = a.get("media",         "").strip()
            new_sent     = a.get("sentiment",     "").strip()
            new_ptype    = a.get("pub_type",      "").strip()
            new_topic    = a.get("topic",         "").strip()

            needs_update = False
            if update_empty and new_title and not existing_title:
                needs_update = True
            if new_chars and not existing_chars:
                needs_update = True
            if (new_lang or new_media or new_sent or new_ptype) and not (
                    existing_lang and existing_media and existing_sent and existing_ptype):
                needs_update = True
            if new_topic and not existing_topic:
                needs_update = True

            if needs_update:
                existing_link = old_row[8] if len(old_row) > 8 else ""
                updated_row = [
                    a.get("date",    "") or (old_row[0] if len(old_row) > 0 else ""),
                    a.get("time",    "") or (old_row[1] if len(old_row) > 1 else ""),
                    a.get("source",  "") or (old_row[2] if len(old_row) > 2 else ""),
                    a.get("section", "") or (old_row[3] if len(old_row) > 3 else ""),
                    new_title     or existing_title,
                    a.get("content", "") or (old_row[5] if len(old_row) > 5 else ""),
                    new_reporter  or (old_row[6] if len(old_row) > 6 else ""),
                    new_chars     or existing_chars,
                    existing_link,
                    serial,
                    new_lang  or existing_lang,
                    new_media or existing_media,
                    new_sent  or existing_sent,
                    new_ptype or existing_ptype,
                    new_topic or existing_topic,
                ]
                row_updates.append((sheet_row, updated_row))
                updated += 1
            else:
                skipped += 1
            continue

        # New article — build row and queue for appending
        link = _encode_url(a.get("link", ""))
        new_rows.append([
            a.get("date",         ""),   # A
            a.get("time",         ""),   # B
            a.get("source",       ""),   # C
            a.get("section",      ""),   # D - מדור (יפעת subsource)
            a.get("title",        ""),   # E
            a.get("content",      ""),   # F
            a.get("reporter_col", ""),   # G
            a.get("character_col",""),   # H
            link,                        # I
            serial,                      # J
            a.get("language",     ""),   # K
            a.get("media",        ""),   # L
            a.get("sentiment",    ""),   # M
            a.get("pub_type",     ""),   # N
            a.get("topic",        ""),   # O
            a.get("sector",       ""),   # P - מגזר (מהאינדקס)
            a.get("audience",     ""),   # Q - חשיפה
            a.get("item_value",   ""),   # R - ערך
        ])
        if serial:
            known_serials.add(serial)

    # --- Apply in-place updates (one ws.update call per changed row) ---
    for sheet_row, row_data in row_updates:
        try:
            end_col = chr(ord('A') + len(row_data) - 1)   # 'O' for 15 columns
            ws.update(f"A{sheet_row}:{end_col}{sheet_row}",
                      [row_data], value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"    אזהרה: לא ניתן לעדכן שורה {sheet_row}: {e}")

    # --- Append genuinely new rows at the bottom ---
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED",
                       insert_data_option="INSERT_ROWS")

    # --- Formatting (dropdowns + row heights) for new rows only ---
    if new_rows or row_updates:
        total_rows = len(raw_data) + len(new_rows)
        _set_row_heights(ws, total_rows)
        _set_dropdown_validation(ws, total_rows)

    # --- Print summary ---
    if skipped:
        print(f"    דולגו {skipped} כתבות כפולות")
    if updated:
        print(f"    עודכנו {updated} כתבות קיימות")
    if new_rows:
        total = len(raw_data) + len(new_rows)
        print(f"    נוספו {len(new_rows)} כתבות חדשות (סה\"כ ~{total} בגיליון)")
    if not new_rows and not updated:
        print("    אין כתבות חדשות להוסיף")


# ============================================================
# יפעת API – fetch articles directly (replaces PDF workflow)
# ============================================================

_IFAT_API_BASE = "https://media.ifat.com/data/api/customer"

# JS snippet used by Playwright to call GetArticles from within the browser
_FETCH_ARTICLES_JS = """
async ([token, pageNum, pageSize]) => {
    const resp = await fetch(
        `https://media.ifat.com/data/api/customer/GetArticles?PageNumber=${pageNum}&PageSize=${pageSize}`,
        {
            method: 'POST',
            headers: {
                'Authorization': 'bearer ' + token,
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/plain, */*',
            },
            body: JSON.stringify({
                ItemType: '', Sort: 'desc', SortField: 'publishdate',
                Source: '', SubjectID: ''
            })
        }
    );
    return await resp.json();
}
"""


def _ifat_browser_login(config: dict):
    """
    Launch headless Chromium, log in to יפעת, and return (playwright, browser, page, token).
    The browser must be closed by the caller.
    """
    from playwright.sync_api import sync_playwright

    pw      = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context()
    bpage   = context.new_page()

    # Intercept the Login response to capture the JWT token
    token_holder: dict = {}

    def _on_resp(response):
        if "Login" in response.url and response.status == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and "token" in data:
                    token_holder["token"] = data["token"]
            except Exception:
                pass

    bpage.on("response", _on_resp)

    bpage.goto("https://media.ifat.com/login", wait_until="load", timeout=60000)

    # Dismiss cookie consent popup if present
    try:
        consent = bpage.locator("button:has-text('אישור')")
        if consent.first.is_visible(timeout=2000):
            consent.first.click()
            bpage.wait_for_timeout(300)
    except Exception:
        pass

    bpage.wait_for_selector("input", timeout=10000)

    # Switch to password tab if present
    try:
        tab = bpage.get_by_text("התחברות עם סיסמה")
        if tab.is_visible(timeout=2000):
            tab.click()
            bpage.wait_for_timeout(300)
    except Exception:
        pass

    # Fill credentials and submit
    bpage.locator("input").nth(0).fill(config["ifat_username"])
    bpage.locator("input").nth(1).fill(config["ifat_password"])
    bpage.locator("input").nth(1).press("Enter")
    bpage.wait_for_url("**/dashboard**", timeout=40000)

    # Extract token (from intercepted response or localStorage)
    token = token_holder.get("token", "")
    if not token:
        token = bpage.evaluate("""() => {
            for (let k of Object.keys(localStorage)) {
                const v = localStorage.getItem(k);
                if (v && v.startsWith('ey')) return v;
            }
            return null;
        }""")

    if not token:
        browser.close()
        pw.stop()
        raise RuntimeError("Login נכשל: לא נמצא token")

    return pw, browser, bpage, token


def _ifat_fetch_page(bpage, token: str, page: int, page_size: int = 100) -> list:
    """Fetch one page of articles by running fetch() inside the browser."""
    result = bpage.evaluate(_FETCH_ARTICLES_JS, [token, page, page_size])
    if isinstance(result, list):
        return result
    return result.get("items", result.get("Items", []))


# ── HTTP-based login (no browser required) ────────────────────────────────────

def _ifat_http_login(config: dict):
    """
    Login to יפעת via direct HTTP POST — no browser needed.
    Tries several common endpoint/payload combinations.
    Returns (requests.Session, token_str).
    Raises RuntimeError if all attempts fail.
    """
    import requests as _requests

    session = _requests.Session()
    session.headers.update({
        "Content-Type":  "application/json",
        "Accept":        "application/json, text/plain, */*",
        "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin":        "https://media.ifat.com",
        "Referer":       "https://media.ifat.com/login",
    })

    username = config["ifat_username"]
    password = config["ifat_password"]

    login_urls = [
        "https://media.ifat.com/data/api/customer/Login",
        "https://media.ifat.com/api/Login",
        "https://media.ifat.com/api/customer/Login",
    ]
    payloads = [
        {"UserName": username, "Password": password},
        {"username": username, "password": password},
        {"Username": username, "Password": password},
    ]

    for url in login_urls:
        for payload in payloads:
            try:
                resp = session.post(url, json=payload, timeout=30)
                print(f"  [DEBUG] {url} → status={resp.status_code} body={resp.text[:200]!r}")
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        token = data.get("token") or data.get("Token") or ""
                        if token and token.startswith("ey"):
                            print(f"  HTTP login הצליח דרך {url}")
                            return session, token
            except Exception as e:
                print(f"  [DEBUG] {url} → exception: {e}")
                continue

    raise RuntimeError("HTTP login נכשל — לא הצלחנו לקבל token מאף endpoint")


def _ifat_fetch_page_http(session, token: str, page: int, page_size: int = 100) -> list:
    """Fetch one page of articles via direct HTTP (no browser)."""
    url  = (
        f"https://media.ifat.com/data/api/customer/GetArticles"
        f"?PageNumber={page}&PageSize={page_size}"
    )
    body = {"ItemType": "", "Sort": "desc", "SortField": "publishdate",
            "Source": "", "SubjectID": ""}
    resp = session.post(
        url, json=body,
        headers={"Authorization": f"bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    if isinstance(result, list):
        return result
    return result.get("items", result.get("Items", []))


def _api_item_to_dict(item: dict, source_index: dict | None = None,
                      language_index: dict | None = None) -> dict:
    """Convert a single יפעת API article object to our internal format."""
    pub = (item.get("publishdate", "") or "").strip()
    try:
        dt       = datetime.fromisoformat(pub[:19])
        date_str = dt.strftime("%d/%m/%Y")
        time_str = dt.strftime("%H:%M")
    except Exception:
        date_str = time_str = ""

    summery   = (item.get("summery",      "") or "").strip()
    subtitle  = (item.get("itemsubtitle", "") or "").strip()
    share_url = (item.get("shareUrl",     "") or "").strip()

    # For online articles summery IS the article URL; subtitle has the lead text
    if summery.startswith("http"):
        link    = share_url or summery
        content = subtitle
    else:
        link    = share_url
        content = summery or subtitle

    # keywords field: e.g. "אלון לי גרין / Alon-Lee Green - מנכ"ל תנועת עומדים ביחד"
    # May contain multiple entries separated by semicolons or commas
    raw_keywords = (item.get("keywords", "") or "").strip()
    # Normalise: keep only the Hebrew name before " / " or " - "
    kw_parts = []
    for kw in re.split(r"[;،]", raw_keywords):
        kw = kw.strip()
        if kw:
            # "אלון לי גרין / Alon-Lee Green - מנכ"ל..." → "אלון לי גרין"
            kw = re.split(r"\s*/\s*|\s+-\s+", kw)[0].strip()
            if kw:
                kw_parts.append(kw)
    interviewees = ", ".join(kw_parts)

    # Detect "שלום ישראלי פלסטיני" topic — used later for routing to separate sheet
    subsubject_names = item.get("subsubjectNames") or []
    if isinstance(subsubject_names, str):
        subsubject_names = [subsubject_names]
    all_topic_text = raw_keywords + " " + " ".join(subsubject_names)
    peace_topic = "שלום ישראלי פלסטיני" in all_topic_text

    raw_itemtype = item.get("itemtype", 1)
    title_str    = (item.get("itemtitle", "") or "").strip()

    source_str = (item.get("source", "") or "").strip()
    # Column D = יפעת subsource (מדור); Column P = index-based sector (מגזר)
    # subsource מגיע כ-"שם מקור - מדור" או "source-section" — מסירים את קידומת המקור
    _raw_sub  = (item.get("subsource", "") or "").strip()
    if " - " in _raw_sub:
        # "הארץ - כותרת"  →  "כותרת"
        subsource = _raw_sub.split(" - ", 1)[1].strip()
    elif "-" in _raw_sub and source_str:
        # "haaretz-front" (כאשר source="haaretz")  →  "front"
        _src_lo  = source_str.lower()
        _sub_lo  = _raw_sub.lower()
        if _sub_lo.startswith(_src_lo + "-"):
            subsource = _raw_sub[len(source_str):].lstrip("-").strip()
        else:
            subsource = _raw_sub
    else:
        subsource = _raw_sub
    sector     = lookup_sector(source_str, source_index or {}) if source_index else ""

    return {
        "date":         date_str,
        "time":         time_str,
        "source":       source_str,
        "section":      subsource,   # D - מדור (יפעת subsource)
        "sector":       sector,      # P - מגזר (from index)
        "title":        title_str,
        "content":      content,
        "reporter":     (item.get("reporter",  "") or "").strip(),
        "interviewees": interviewees,
        "link":         link,
        "serial":       str(item.get("itemid", "")),
        "is_print":     False,    # no Drive upload needed
        "peace_topic":  peace_topic,
        # metadata for new sheet columns
        "_itemtype":    raw_itemtype,
        "language":     (lookup_language(source_str, language_index or {})
                         or _api_language(item, title_str + " " + content)),
        "media":        _detect_media(raw_itemtype),
        "sentiment":    _translate_sentiment(item.get("sentiment", "")),
        # pub_type and topic depend on character_col (set after enrich())
        "pub_type":     "",
        "topic":        "",
        "audience":     item.get("audienceRating") or "",  # Q - חשיפה
        "item_value":   item.get("itemValue")      or "",  # R - ערך
    }


_OMETZ_KEYWORD = "עומדים ביחד"

# ── Source index (loaded once per run) ───────────────────────────────────────
# גיליון "אינדקס": A=שם מקור, B=מגזר, C=שפה
_source_index_cache:   dict[str, str] | None = None
_language_index_cache: dict[str, str] | None = None


def load_source_index(config: dict) -> dict:
    """
    Read the 'אינדקס' sheet and return a lowercase-normalised dict:
        { normalised_source_name → sector_string }
    Also populates _language_index_cache from column C (שפה).
    Reuses the shared gspread connection. Results are cached per run.
    """
    global _source_index_cache, _language_index_cache
    if _source_index_cache is not None:
        return _source_index_cache

    try:
        _, spreadsheet = _get_spreadsheet(config)
        ws             = spreadsheet.worksheet("אינדקס")
        rows           = ws.get_all_values()
    except Exception as e:
        print(f"[אזהרה] לא ניתן לטעון גיליון אינדקס: {e}")
        _source_index_cache   = {}
        _language_index_cache = {}
        return _source_index_cache

    sector_idx:   dict[str, str] = {}
    language_idx: dict[str, str] = {}
    for row in rows[1:]:   # skip header
        if not (len(row) >= 2 and row[0].strip()):
            continue
        key = row[0].strip().lower()
        sector_idx[key] = row[1].strip()
        if len(row) >= 3 and row[2].strip():          # עמודה C = שפה
            language_idx[key] = row[2].strip()

    _source_index_cache   = sector_idx
    _language_index_cache = language_idx
    return _source_index_cache


def lookup_sector(source: str, index: dict[str, str]) -> str:
    """
    Case-insensitive lookup of source in the index.
    Falls back to partial (substring) match if exact match not found.
    """
    if not source or not index:
        return ""
    s = source.strip().lower()
    if s in index:
        return index[s]
    for key, sector in index.items():
        if key in s or s in key:
            return sector
    return ""


def lookup_language(source: str, index: dict[str, str]) -> str:
    """
    Case-insensitive lookup of source language from the index (column C).
    Falls back to partial (substring) match if exact match not found.
    """
    if not source or not index:
        return ""
    s = source.strip().lower()
    if s in index:
        return index[s]
    for key, lang in index.items():
        if key in s or s in key:
            return lang
    return ""


# ── Topic auto-detection ──────────────────────────────────────────────────────
def _detect_topic(art: dict) -> str:
    """
    Auto-detect topic(s) for column O.
    Returns comma-separated string of applicable topics.
    """
    topics: list[str] = []

    if art.get("character_col", "").strip():
        topics.append("דמויות ציבוריות")

    if art.get("peace_topic"):
        topics.append("שלום ישראלי פלסטיני")

    return ", ".join(topics)

# ── Media type ────────────────────────────────────────────────────────────────
_ITEMTYPE_TO_MEDIA: dict[int, str] = {
    0:  "עיתונות",
    1:  "אינטרנט",
    2:  "רדיו",
    10: "טלוויזיה",
}

def _detect_media(itemtype) -> str:
    return _ITEMTYPE_TO_MEDIA.get(int(itemtype) if itemtype is not None else 1, "אינטרנט")


# ── Language ──────────────────────────────────────────────────────────────────

# Numeric language IDs that Ifat may return (languageid field)
_LANGUAGE_ID_MAP: dict = {
    1: 'עברית',    '1': 'עברית',
    2: 'ערבית',    '2': 'ערבית',
    3: 'אנגלית',   '3': 'אנגלית',
    4: 'רוסית',    '4': 'רוסית',
    5: 'צרפתית',   '5': 'צרפתית',
    6: 'ספרדית',   '6': 'ספרדית',
    7: 'גרמנית',   '7': 'גרמנית',
    8: 'אמהרית',   '8': 'אמהרית',
    9: 'תיגרינית', '9': 'תיגרינית',
}

# String language names (lowercase) that Ifat may return (language field)
_LANGUAGE_NAME_MAP: dict = {
    'hebrew':    'עברית',   'heb': 'עברית',   'עברית': 'עברית',
    'arabic':    'ערבית',   'ara': 'ערבית',   'ערבית': 'ערבית',
    'english':   'אנגלית',  'eng': 'אנגלית',  'אנגלית': 'אנגלית',
    'russian':   'רוסית',   'rus': 'רוסית',   'רוסית': 'רוסית',
    'french':    'צרפתית',  'fre': 'צרפתית',  'צרפתית': 'צרפתית',
    'spanish':   'ספרדית',  'spa': 'ספרדית',  'ספרדית': 'ספרדית',
    'german':    'גרמנית',  'ger': 'גרמנית',  'גרמנית': 'גרמנית',
    'amharic':   'אמהרית',                    'אמהרית': 'אמהרית',
    'tigrinya':  'תיגרינית','tigrigna': 'תיגרינית', 'תיגרינית': 'תיגרינית',
}


def _api_language(item: dict, fallback_text: str = "") -> str:
    """
    Read language from a יפעת API item dict.
    Priority:
      1. languageid (numeric)  →  _LANGUAGE_ID_MAP
      2. language / lang (string)  →  _LANGUAGE_NAME_MAP
      3. Text-based detection on fallback_text (last resort)
    """
    # 1. Numeric ID
    lang_id = (item.get("languageid") or item.get("LanguageId")
               or item.get("languageId") or item.get("language_id"))
    if lang_id is not None:
        mapped = _LANGUAGE_ID_MAP.get(lang_id) or _LANGUAGE_ID_MAP.get(str(lang_id))
        if mapped:
            return mapped

    # 2. String name (various casings)
    lang_str = (
        item.get("language") or item.get("Language")
        or item.get("lang")  or item.get("Lang") or ""
    ).strip()
    if lang_str:
        mapped = (_LANGUAGE_NAME_MAP.get(lang_str.lower())
                  or _LANGUAGE_NAME_MAP.get(lang_str))
        if mapped:
            return mapped

    # 3. Fallback: detect from text
    return _detect_language(fallback_text) if fallback_text else 'עברית'


def _detect_language(text: str) -> str:
    """Last-resort language detection by counting Unicode character ranges."""
    hebrew   = len(re.findall(r'[\u0590-\u05FF\uFB1D-\uFB4F]', text))
    arabic   = len(re.findall(r'[\u0600-\u06FF\uFB50-\uFDFF\uFE70-\uFEFF]', text))
    cyrillic = len(re.findall(r'[\u0400-\u04FF]', text))
    latin    = len(re.findall(r'[A-Za-z]', text))
    scores   = [('עברית', hebrew), ('ערבית', arabic), ('רוסית', cyrillic), ('אנגלית', latin)]
    best     = max(scores, key=lambda x: x[1])
    return best[0] if best[1] > 0 else 'עברית'


# ── Sentiment ─────────────────────────────────────────────────────────────────
_SENTIMENT_MAP = {
    'חיובי': 'חיובי', 'positive': 'חיובי',
    'שלילי': 'שלילי', 'negative': 'שלילי',
    'נייטרלי': 'נייטרלי', 'neutral': 'נייטרלי', 'ניטרלי': 'נייטרלי',
}

def _translate_sentiment(raw: str) -> str:
    return _SENTIMENT_MAP.get((raw or '').strip(), 'נייטרלי')


# ── Publication type ──────────────────────────────────────────────────────────
def _detect_pub_type(art: dict) -> str:
    """
    Best-effort classification. Conservative: uses 'איזכור' when an ST figure
    is mentioned (can't verify from API if actual audio/video clip exists).
    Types requiring human judgment (אינסרט, סינק, ראיון, טור דעה, etc.) are
    left for manual editing in the sheet.
    """
    content  = (art.get("content",       "") or "").strip()
    link     = (art.get("link",          "") or "").strip()
    char_col = (art.get("character_col", "") or "").strip()
    title    = (art.get("title",         "") or "").strip()

    # Pure link (no readable content)
    if not content and link:
        return "לינק"

    # Very short with no content → headline
    if not content and title and len(title) < 80:
        return "כותרת"

    # ST figure mentioned → "איזכור" (conservative; user can upgrade to
    # אינסרט / סינק / ראיון manually if there was actual audio/video)
    if char_col:
        return "איזכור"

    # Default for all media types
    return "ידיעה"


def _is_peace_only(art: dict) -> bool:
    """
    Return True when the article should go to the peace sheet instead of main:
      - tagged as peace_topic by יפעת
      - AND no ST character was detected
      - AND "עומדים ביחד" does not appear anywhere in the article text
    """
    if not art.get("peace_topic"):
        return False
    if art.get("character_col", "").strip():
        return False   # has an ST figure → main sheet
    full = " ".join([
        art.get("title",        ""),
        art.get("content",      ""),
        art.get("interviewees", ""),
        art.get("reporter",     ""),
    ])
    if _OMETZ_KEYWORD in full:
        return False   # mentions עומדים ביחד → main sheet
    return True


def fetch_api_articles(
    config: dict,
    characters: list,
    target_date: Optional[str] = None,
) -> tuple[list[dict], list[dict]]:
    """
    Login → fetch all articles for `target_date` (DD/MM/YYYY, default: yesterday)
    → enrich with character matching
    → return (main_articles, peace_articles).
      main_articles  – go to the regular sheet
      peace_articles – tagged שלום ישראלי פלסטיני with no ST connection
    """
    from datetime import timedelta

    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

    try:
        target_dt = datetime.strptime(target_date, "%d/%m/%Y").date()
    except ValueError:
        raise ValueError(f"פורמט תאריך שגוי: {target_date}  (צפוי DD/MM/YYYY)")

    source_index   = load_source_index(config)
    language_index = _language_index_cache or {}
    if language_index:
        print(f"  נטענו {len(language_index)} מקורות עם שפה מהאינדקס")
    else:
        print(f"  [שים לב] אין עמודת שפה באינדקס — נשתמש בזיהוי אוטומטי")
    print(f"מתחבר ל-API יפעת...")
    pw, browser, bpage, token = _ifat_browser_login(config)
    print(f"מחובר. מושך כתבות עבור {target_date}...")

    main_articles:  list[dict] = []
    peace_articles: list[dict] = []
    PAGE_SIZE = 100

    try:
        for page in range(1, 9999):
            items = _ifat_fetch_page(bpage, token, page=page, page_size=PAGE_SIZE)
            if not items:
                break

            past_target = False
            for item in items:
                pub = (item.get("publishdate", "") or "")[:19]
                try:
                    item_dt = datetime.fromisoformat(pub).date()
                except Exception:
                    continue

                if item_dt == target_dt:
                    art = _api_item_to_dict(item, source_index=source_index,
                                            language_index=language_index)
                    enrich(art, characters)
                    art["pub_type"] = _detect_pub_type(art)
                    art["topic"]    = _detect_topic(art)
                    if _is_peace_only(art):
                        peace_articles.append(art)
                    else:
                        main_articles.append(art)
                elif item_dt < target_dt:
                    past_target = True
                    break

            if past_target or len(items) < PAGE_SIZE:
                break
    finally:
        browser.close()
        pw.stop()

    print(f"נמצאו {len(main_articles)} כתבות ראשיות + {len(peace_articles)} כתבות שלום ישראלי-פלסטיני עבור {target_date}")
    return main_articles, peace_articles


# ============================================================
# Archive: fetch a date range → "ארכיון" sheet
# ============================================================

def fetch_archive_range(
    config: dict,
    characters: list,
    from_date_str: str = "01/01/2020",
    to_date_str: Optional[str] = None,
    write_batch_size: int = 300,
) -> int:
    """
    Fetch ALL articles from Yifat API in [from_date, to_date] and write
    them to the archive sheet (config["archive_sheet_name"], default "ארכיון").

    This is a ONE-TIME backfill operation.  It does NOT touch the main
    "עומדים ביחד פרסומים" or "שלום ישראלי פלסטיני" tabs and therefore
    cannot interfere with the daily --fetch-api runs.

    Returns the total number of articles written.
    """
    from datetime import timedelta

    if to_date_str is None:
        to_date_str = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

    try:
        from_dt = datetime.strptime(from_date_str, "%d/%m/%Y").date()
        to_dt   = datetime.strptime(to_date_str,   "%d/%m/%Y").date()
    except ValueError as exc:
        raise ValueError(f"פורמט תאריך שגוי ({exc}).  השתמש ב-DD/MM/YYYY") from exc

    archive_sheet = config.get("archive_sheet_name", "ארכיון")
    archive_cfg   = {**config, "sheet_name": archive_sheet}

    print(f"\n{'='*60}")
    print(f"  ארכיון יפעת: {from_date_str} → {to_date_str}")
    print(f"  טאב יעד: '{archive_sheet}'")
    print(f"  גודל אצווה לכתיבה: {write_batch_size} כתבות")
    print(f"{'='*60}\n")

    source_index = load_source_index(config)
    print("מתחבר ל-API יפעת...")
    pw, browser, bpage, token = _ifat_browser_login(config)
    print("מחובר. מתחיל שליפה...\n")

    PAGE_SIZE   = 100
    batch:      list[dict] = []
    total_written           = 0
    pages_fetched           = 0
    newest_seen: Optional[str] = None
    oldest_seen: Optional[str] = None

    try:
        for page in range(1, 99_999):
            items = _ifat_fetch_page(bpage, token, page=page, page_size=PAGE_SIZE)
            if not items:
                print("  ← אין עוד כתבות ב-API.")
                break

            pages_fetched += 1
            in_range_this_page = 0
            past_range         = False

            for item in items:
                pub = (item.get("publishdate", "") or "")[:19]
                try:
                    item_dt = datetime.fromisoformat(pub).date()
                except Exception:
                    continue

                if item_dt > to_dt:
                    continue

                if item_dt < from_dt:
                    past_range = True
                    break

                art = _api_item_to_dict(item, source_index=source_index,
                                        language_index=_language_index_cache or {})
                enrich(art, characters)
                art["pub_type"] = _detect_pub_type(art)
                art["topic"]    = _detect_topic(art)
                batch.append(art)
                in_range_this_page += 1

                date_str = item_dt.strftime("%d/%m/%Y")
                if newest_seen is None:
                    newest_seen = date_str
                oldest_seen = date_str

            print(
                f"  דף {page:4d} | בטווח: {in_range_this_page:3d} | "
                f"אצווה: {len(batch):4d} | "
                f"סה\"כ נכתב: {total_written:5d} | "
                f"תאריך אחרון: {oldest_seen or '—'}"
            )

            if len(batch) >= write_batch_size:
                print(f"\n  → כותב {len(batch)} כתבות לטאב '{archive_sheet}'...")
                append_to_sheet(batch, archive_cfg)
                total_written += len(batch)
                batch = []
                print(f"  סה\"כ נכתב עד כה: {total_written}\n")

            if past_range:
                print(f"\n  ← הגענו לפני {from_date_str} — עוצרים.")
                break

            if len(items) < PAGE_SIZE:
                print("  ← דף חלקי — סוף הנתונים ב-API.")
                break
    finally:
        browser.close()
        pw.stop()

    # Write whatever remains in the last batch
    if batch:
        print(f"\n  → כותב {len(batch)} כתבות אחרונות לטאב '{archive_sheet}'...")
        append_to_sheet(batch, archive_cfg)
        total_written += len(batch)

    print(f"\n{'='*60}")
    print(f"  הסתיים!  סה\"כ {total_written} כתבות נכתבו לטאב '{archive_sheet}'")
    print(f"  דפים שנסרקו: {pages_fetched}")
    print(f"  טווח תאריכים שנכתב: {oldest_seen or '—'} → {newest_seen or '—'}")
    print(f"{'='*60}\n")
    return total_written


# ============================================================
# Folder watcher
# ============================================================

class PdfHandler(FileSystemEventHandler):
    def __init__(self, config: dict, characters: list, processed: set):
        self.config     = config
        self.characters = characters
        self.processed  = processed

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != ".pdf":
            return
        time.sleep(3)
        self._handle(path)

    def _handle(self, path: Path):
        if path.name in self.processed:
            return
        print(f"\nמעבד: {path.name}")
        try:
            articles = process_pdf(path, self.characters, self.config)
            append_to_sheet(articles, self.config)
            self.processed.add(path.name)
            save_state(self.processed)
            print(f"    הושלם ({len(articles)} פריטים)")
        except Exception:
            print(f"    שגיאה:")
            traceback.print_exc()


# ============================================================
# Main
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="עיבוד PDFs מיפעת → Google Sheets")
    parser.add_argument("--process",       metavar="FILE", help="עבד קובץ PDF ספציפי")
    parser.add_argument("--watch",         action="store_true", help="האזן לתיקייה ועבד קבצים חדשים אוטומטית")
    parser.add_argument("--update-titles", action="store_true",
                        help="סרוק מחדש את כל הPDFs בתיקייה ועדכן כותרות ריקות בגיליון (OCR)")
    parser.add_argument("--fetch-api",     action="store_true",
                        help="משוך כתבות מ-API יפעת ישירות והכנס לגיליון (ברירת מחדל: אתמול)")
    parser.add_argument("--date",          metavar="DD/MM/YYYY",
                        help="תאריך לשליפה עבור --fetch-api (ברירת מחדל: אתמול)")
    parser.add_argument("--archive",       action="store_true",
                        help="שלוף ארכיון היסטורי מ-API יפעת וכתוב לטאב 'ארכיון' (פעולה חד-פעמית)")
    parser.add_argument("--from-date",     metavar="DD/MM/YYYY", default="01/01/2020",
                        help="תאריך התחלה לארכיון (ברירת מחדל: 01/01/2020)")
    parser.add_argument("--to-date",       metavar="DD/MM/YYYY",
                        help="תאריך סיום לארכיון (ברירת מחדל: אתמול)")
    args = parser.parse_args()

    config     = load_config()
    characters = load_characters()
    processed  = load_state()
    watch_dir  = Path(config["watch_folder"])

    if args.process:
        pdf_path = Path(args.process)
        print(f"מעבד: {pdf_path.name}")
        articles = process_pdf(pdf_path, characters, config)
        append_to_sheet(articles, config)
        processed.add(pdf_path.name)
        save_state(processed)
        print(f"הושלם ({len(articles)} פריטים)")

    elif args.archive:
        if "ifat_username" not in config or "ifat_password" not in config:
            print("שגיאה: חסרים ifat_username / ifat_password ב-ifat_config.json")
            return
        fetch_archive_range(
            config,
            characters,
            from_date_str=args.from_date,
            to_date_str=args.to_date,   # None → ברירת מחדל: אתמול
        )

    elif args.fetch_api:
        if "ifat_username" not in config or "ifat_password" not in config:
            print("שגיאה: חסרים ifat_username / ifat_password ב-ifat_config.json")
            return
        if args.date:
            # תאריך ספציפי שצוין — שולף רק אותו
            dates_to_fetch = [args.date]
        else:
            # ברירת מחדל: אתמול + היום (בוקר רדיו / עיתונות מודפסת של הבוקר)
            from datetime import timedelta
            today_str     = datetime.now().strftime("%d/%m/%Y")
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
            dates_to_fetch = [yesterday_str, today_str]

        main_articles: list = []
        peace_articles: list = []
        for fetch_date in dates_to_fetch:
            m, p = fetch_api_articles(config, characters, target_date=fetch_date)
            main_articles.extend(m)
            peace_articles.extend(p)

        peace_sheet = config.get("peace_sheet_name", "שלום ישראלי פלסטיני")

        append_to_sheet(main_articles, config)
        if peace_articles:
            print(f"\nמוסיף {len(peace_articles)} כתבות לטאב '{peace_sheet}'...")
            append_to_sheet(peace_articles, config, sheet_name=peace_sheet)
        print("הושלם.")

    elif args.update_titles:
        # Scan all PDFs (already processed or not) and fill in missing titles via OCR.
        # Does NOT re-upload to Drive — existing Drive links are preserved in the sheet.
        pdfs = sorted(watch_dir.glob("*.pdf"))
        if not pdfs:
            print("אין קבצי PDF בתיקייה.")
            return
        print(f"מצב עדכון כותרות: {len(pdfs)} קבצים")
        print("(כתבות עם כותרת קיימת לא ישתנו)\n")
        for pdf_path in pdfs:
            print(f"סורק: {pdf_path.name}")
            try:
                articles = process_pdf(pdf_path, characters, config, skip_upload=True)
                append_to_sheet(articles, config, update_empty=True)
            except Exception:
                print("    שגיאה:")
                traceback.print_exc()
        print("\nסיום עדכון כותרות.")

    elif args.watch:
        print(f"מאזין לתיקייה: {watch_dir}")
        print("לעצירה: Ctrl+C\n")
        handler  = PdfHandler(config, characters, processed)
        observer = Observer()
        observer.schedule(handler, str(watch_dir), recursive=False)
        observer.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

    else:
        # Default: process all unprocessed PDFs in watch folder
        pdfs     = sorted(watch_dir.glob("*.pdf"))
        new_pdfs = [p for p in pdfs if p.name not in processed]
        if not new_pdfs:
            print("אין קבצי PDF חדשים לעיבוד.")
            return
        for pdf_path in new_pdfs:
            print(f"\nמעבד: {pdf_path.name}")
            try:
                articles = process_pdf(pdf_path, characters, config)
                append_to_sheet(articles, config)
                processed.add(pdf_path.name)
                save_state(processed)
                print(f"    הושלם ({len(articles)} פריטים)")
            except Exception:
                print("    שגיאה:")
                traceback.print_exc()


if __name__ == "__main__":
    main()
