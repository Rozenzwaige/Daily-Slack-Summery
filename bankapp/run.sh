#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "[$(date)] Starting finance scraper" >> run.log

# Export env vars for node (skip GOOGLE_CREDENTIALS_JSON — too long)
while IFS='=' read -r key value; do
    [[ -z "$key" || "$key" == \#* ]] && continue
    [[ "$key" == "GOOGLE_CREDENTIALS_JSON" ]] && continue
    [[ "$key" == "PUPPETEER_EXECUTABLE_PATH" ]] && continue
    export "$key=$value"
done < .env

# Scrape
cd scraper
node scrape.js >> ../run.log 2>&1
cd ..

# Process + Sheets + WhatsApp (python reads .env itself)
./venv/bin/python processor.py >> run.log 2>&1

echo "[$(date)] Done" >> run.log