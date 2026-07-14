#!/bin/bash
cd "$(dirname "$0")"
echo "[$(date)] Starting weekly report" >> run.log
./venv/bin/python /home/nana-net/BankApp/run_weekly.py >> run.log 2>&1
echo "[$(date)] Weekly report done" >> run.log
