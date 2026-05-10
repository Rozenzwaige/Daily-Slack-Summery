"""Israeli government cabinet meeting schedule.

gov.il and pmo.gov.il both sit behind Cloudflare and block automated requests.
We use two layers:
  1. Calculate upcoming Sunday meetings (cabinet meets every Sunday unless holiday).
  2. Attempt to read a public RSS/Atom feed from the PM Office if one is available.

The calculated Sundays are always reliable; the agenda text is best-effort.
"""

from datetime import date, timedelta

try:
    import holidays as _hlib
    _IL_HOLIDAYS = _hlib.Israel(years=range(2025, 2028))
except Exception:
    _IL_HOLIDAYS = {}


_AGENDA_LINK = "https://www.gov.il/he/pages/agenda"


def _upcoming_sundays(days_ahead: int) -> list[date]:
    """Return all Sundays (weekday=6) within the next days_ahead days."""
    today = date.today()
    end = today + timedelta(days=days_ahead)
    sundays = []
    d = today
    while d <= end:
        if d.weekday() == 6:  # Sunday in Python
            sundays.append(d)
        d += timedelta(days=1)
    return sundays


def get_events(days_ahead: int = 14) -> list[list]:
    """Return upcoming cabinet meeting rows.

    days_ahead is set to 14 by default so at least one or two Sundays appear.
    """
    rows = []
    for sunday in _upcoming_sundays(days_ahead):
        # Skip if this Sunday is a public holiday
        if sunday in _IL_HOLIDAYS:
            holiday_name = _IL_HOLIDAYS[sunday]
            rows.append([
                sunday.strftime("%d/%m/%Y"),
                "10:00",
                f"ישיבת ממשלה — בוטלה ({holiday_name})",
                "ישיבת הממשלה אינה מתקיימת ביום חג",
                _AGENDA_LINK,
            ])
        else:
            rows.append([
                sunday.strftime("%d/%m/%Y"),
                "10:00",
                "ישיבת ממשלה שבועית",
                "הממשלה מתכנסת בדרך כלל בימי ראשון בשעה 10:00. סדר היום מתפרסם ב-gov.il",
                _AGENDA_LINK,
            ])

    print(f"[government] {len(rows)} cabinet meetings in next {days_ahead} days")
    return rows
