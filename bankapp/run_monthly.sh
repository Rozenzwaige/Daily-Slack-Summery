#!/bin/bash
cd "$(dirname "$0")"
echo "[$(date)] Starting monthly report" >> run.log
set -a; source .env; set +a
./venv/bin/python -c "import sheets, monthly_report; svc = sheets.open_spreadsheet(); monthly_report.send_monthly_reports(svc)" >> run.log 2>&1
echo "[$(date)] Monthly report done" >> run.log
