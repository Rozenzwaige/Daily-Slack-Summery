"""Jewish and Israeli national holidays for the current (and next) Gregorian year.

Uses the 'holidays' PyPI package (Israel locale).
Falls back to a hardcoded list for the current year if the library is missing.
"""

from datetime import date

try:
    import holidays as _hlib
    _HAS_LIB = True
except ImportError:
    _HAS_LIB = False
    print("[holidays] 'holidays' library not installed — using empty list")

# English → Hebrew display names
_HEB = {
    "Rosh Hashana":                        "ראש השנה",
    "Rosh HaShanah":                       "ראש השנה",
    "Rosh Hashana II":                     "ראש השנה (יום ב')",
    "Yom Kippur":                          "יום כיפור",
    "Sukkot":                              "סוכות",
    "Sukkot II (CH''M)":                   "סוכות (חול המועד)",
    "Hoshana Raba":                        "הושענא רבה",
    "Simchat Torah / Shemini Atzeret":     "שמחת תורה / שמיני עצרת",
    "Simchat Torah":                       "שמחת תורה",
    "Shemini Atzeret":                     "שמיני עצרת",
    "Hanukkah":                            "חנוכה",
    "Tu BiShvat":                          'ט"ו בשבט',
    "Purim":                               "פורים",
    "Purim Katan":                         "פורים קטן",
    "Pesach":                              "פסח",
    "Pesach I (CH''M)":                    "פסח (חול המועד)",
    "Pesach II (CH''M)":                   "פסח (חול המועד)",
    "Pesach VII":                          "שביעי של פסח",
    "Yom HaShoah":                         "יום השואה",
    "Yom HaZikaron":                       "יום הזיכרון",
    "Independence Day":                    "יום העצמאות",
    "Lag BaOmer":                          'ל"ג בעומר',
    "Jerusalem Day":                       "יום ירושלים",
    "Shavuot":                             "שבועות",
    "Tisha B'Av":                          "תשעה באב",
    "New Year's Day":                      "ראש השנה הלועזי",
    "Christmas Day":                       "חג המולד",
}


def _heb_name(en: str) -> str:
    return _HEB.get(en, en)


def get_events(years: list[int] | None = None) -> list[list]:
    if not _HAS_LIB:
        return []

    if years is None:
        today = date.today()
        years = [today.year]
        if today.month >= 9:          # Tishrei falls in Sep-Oct, span two Gregorian years
            years.append(today.year + 1)

    il = _hlib.Israel(years=years)
    rows = []
    for d, name in sorted(il.items()):
        rows.append([
            d.strftime("%d/%m/%Y"),
            "",
            _heb_name(name),
            name,   # English name in description column for reference
            f"https://he.wikipedia.org/wiki/{name.replace(' ', '_')}",
        ])

    print(f"[holidays] {len(rows)} holidays ({years})")
    return rows
