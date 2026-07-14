#!/usr/bin/env python3
import sheets
import monthly_report

svc = sheets.open_spreadsheet()
monthly_report.send_monthly_reports(svc)
