"""Daily calendar aggregator — fetches events from 4 sources and writes to Google Sheets.

Usage:
    python main.py                    # update all tabs
    python main.py knesset            # update a single tab by name
    python main.py knesset holidays   # update specific tabs
"""

import sys
import os

# Allow running from the repo root: `python calendar_aggregator/main.py`
sys.path.insert(0, os.path.dirname(__file__))

from scrapers import knesset, government, courts, holidays
from sheets import update_tab
from config import TAB_KNESSET, TAB_GOVERNMENT, TAB_COURTS, TAB_HOLIDAYS, DAYS_AHEAD

_SCRAPERS = [
    (TAB_KNESSET,    "knesset",    knesset.get_events),
    (TAB_GOVERNMENT, "government", government.get_events),
    (TAB_COURTS,     "courts",     courts.get_events),
    (TAB_HOLIDAYS,   "holidays",   holidays.get_events),
]


def main(targets: list[str] | None = None):
    errors = []
    for tab, key, scraper in _SCRAPERS:
        if targets and key not in targets:
            continue
        print(f"\n{'='*40}\n{tab}\n{'='*40}")
        try:
            if key == "holidays":
                rows = scraper()          # holidays doesn't take days_ahead
            else:
                rows = scraper(DAYS_AHEAD)
            update_tab(tab, rows)
        except Exception as e:
            print(f"[main] ERROR in {tab}: {e}")
            errors.append((tab, str(e)))

    if errors:
        print(f"\n{len(errors)} tab(s) failed: {[t for t, _ in errors]}")
        sys.exit(1)
    print("\nAll done.")


if __name__ == "__main__":
    args = sys.argv[1:]
    main(args if args else None)
