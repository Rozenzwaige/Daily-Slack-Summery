import os
import sys

sys.path.insert(0, '/home/nana-net/BankApp')
os.chdir('/home/nana-net/BankApp')

with open('.env') as f:
    for raw in f:
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        k = k.strip()
        v = v.strip()
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        os.environ.setdefault(k, v)

import sheets
import weekly_report

svc = sheets.open_spreadsheet()
weekly_report.send_weekly_reports(svc)
