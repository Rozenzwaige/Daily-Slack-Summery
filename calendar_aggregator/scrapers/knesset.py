"""Knesset sessions via the official OData open-data API.

Two sources are merged:
  • KNS_CommitteeSession — committee meetings (with StartDate + SessionUrl)
  • KNS_PlmSession       — plenary (מליאה) sessions

API base: https://knesset.gov.il/Odata/ParliamentInfo.svc/
Docs:     https://main.knesset.gov.il/Activity/pages/KnessetData.aspx

Dates come back as WCF /Date(ms)/ UTC milliseconds.
No browser automation required — the OData endpoint is public.
"""

import re
import requests
from datetime import datetime, timedelta, timezone

try:
    import pytz
    _IL_TZ = pytz.timezone("Asia/Jerusalem")
except ImportError:
    _IL_TZ = None

_BASE = "https://knesset.gov.il/Odata/ParliamentInfo.svc"
_REQ_HEADERS = {
    # Some WCF OData endpoints reject requests without a browser-like User-Agent
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8",
    "Referer": "https://main.knesset.gov.il/Activity/committees/Pages/AllCommitteesAgenda.aspx",
    "X-Requested-With": "XMLHttpRequest",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_israel(raw) -> tuple[str, str]:
    """Convert OData /Date(ms)/ or ISO string → (DD/MM/YYYY, HH:MM) Israel time."""
    if not raw:
        return "", ""
    m = re.match(r"/Date\((\d+)([+-]\d+)?\)/", str(raw))
    if m:
        dt_utc = datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc)
        dt = dt_utc.astimezone(_IL_TZ) if _IL_TZ else dt_utc + timedelta(hours=3)
        return dt.strftime("%d/%m/%Y"), dt.strftime("%H:%M")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(str(raw)[:19], fmt)
            return dt.strftime("%d/%m/%Y"), dt.strftime("%H:%M")
        except ValueError:
            pass
    return str(raw), ""


def _odata_filter(start: datetime, end: datetime) -> str:
    return (
        f"StartDate ge datetime'{start.strftime('%Y-%m-%dT00:00:00')}' "
        f"and StartDate le datetime'{end.strftime('%Y-%m-%dT23:59:59')}'"
    )


def _fetch(table: str, extra_params: dict) -> list[dict]:
    """Fetch all pages from an OData table, handling both v3 and v4 envelopes."""
    params = {"$format": "json", **extra_params}
    url = f"{_BASE}/{table}"
    items: list[dict] = []
    try:
        resp = requests.get(url, params=params, headers=_REQ_HEADERS, timeout=30)
        resp.raise_for_status()
        print(f"[knesset] {table}: HTTP {resp.status_code}, {len(resp.content)} bytes")
        if not resp.content:
            print(f"[knesset] {table}: empty body — server may geo-block non-Israeli IPs")
            return items
        body = resp.json()
        # OData v3: {"d": {"results": [...]}}
        # OData v4: {"value": [...]}
        items = (
            body.get("d", {}).get("results")
            or body.get("value")
            or []
        )
        print(f"[knesset] {table}: {len(items)} items parsed")
    except Exception as e:
        print(f"[knesset] {table} error: {e}")
    return items


# ---------------------------------------------------------------------------
# Agenda items (KNS_CmtSessionItem)
# ---------------------------------------------------------------------------

def _fetch_agenda_items(session_ids: list[int]) -> dict[int, list[str]]:
    """Return {CommitteeSessionID: [topic, ...]} for the given session IDs."""
    if not session_ids:
        return {}
    # Build filter in chunks to avoid URL length limits
    CHUNK = 50
    result: dict[int, list[str]] = {}
    for i in range(0, len(session_ids), CHUNK):
        chunk = session_ids[i : i + CHUNK]
        filter_str = " or ".join(f"CommitteeSessionID eq {sid}" for sid in chunk)
        items = _fetch("KNS_CmtSessionItem", {
            "$filter": filter_str,
            "$orderby": "CommitteeSessionID,Ordinal",
            "$top": "1000",
        })
        for item in items:
            sid  = item.get("CommitteeSessionID")
            name = (item.get("Name") or "").strip().replace("\n", " ")
            if sid and name:
                result.setdefault(sid, []).append(name)
    return result


# ---------------------------------------------------------------------------
# Committee sessions
# ---------------------------------------------------------------------------

def _committee_sessions(start: datetime, end: datetime) -> list[list]:
    items = _fetch("KNS_CommitteeSession", {
        "$filter": _odata_filter(start, end),
        "$expand": "KNS_Committee",
        "$orderby": "StartDate",
        "$top": "300",
    })

    # Pre-fetch all agenda items in one batch
    session_ids = [item["CommitteeSessionID"] for item in items if item.get("CommitteeSessionID")]
    agenda_map  = _fetch_agenda_items(session_ids)

    rows = []
    for item in items:
        # Skip cancelled / deleted
        status = (item.get("StatusDesc") or "").strip()
        if status in ("בוטלה", "נמחקה"):
            continue

        date_str, time_str = _to_israel(item.get("StartDate"))
        if not date_str:
            continue

        # Committee name (navigation property may be deferred if $expand didn't resolve)
        committee_obj = item.get("KNS_Committee") or {}
        if isinstance(committee_obj, dict) and "__deferred" in committee_obj:
            committee_obj = {}
        committee = committee_obj.get("Name", "ועדה")

        sid    = item.get("CommitteeSessionID")
        topics = agenda_map.get(sid, [])
        desc   = " | ".join(topics) if topics else ""

        link = (item.get("SessionUrl") or "").strip()
        if link and not link.startswith("http"):
            link = "https://main.knesset.gov.il" + link

        rows.append([date_str, time_str, committee, desc, link])
    return rows


# ---------------------------------------------------------------------------
# Plenary sessions
# ---------------------------------------------------------------------------

def _plenary_sessions(start: datetime, end: datetime) -> list[list]:
    items = _fetch("KNS_PlenumSession", {
        "$filter": _odata_filter(start, end),
        "$orderby": "StartDate",
        "$top": "50",
    })
    rows = []
    for item in items:
        date_str, time_str = _to_israel(item.get("StartDate"))
        if not date_str:
            continue
        name = (item.get("Name") or "ישיבת מליאה").strip()
        rows.append([date_str, time_str, "מליאת הכנסת", name, ""])
    return rows


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def get_events(days_ahead: int = 7) -> list[list]:
    now = datetime.now()
    end = now + timedelta(days=days_ahead)

    rows = _committee_sessions(now, end) + _plenary_sessions(now, end)

    # Sort by date + time
    def _key(r):
        try:
            d, t = r[0], r[1]
            day, month, year = d.split("/")
            return f"{year}{month}{day}{t}"
        except Exception:
            return r[0]

    rows.sort(key=_key)
    print(f"[knesset] total {len(rows)} events")
    return rows
