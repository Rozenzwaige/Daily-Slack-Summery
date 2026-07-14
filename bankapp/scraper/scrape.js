#!/usr/bin/env node
/**
 * Scrapes הבינלאומי (fibi) and כאל (visaCal).
 * Writes ../scraped_data.json for processor.py.
 *
 * Env vars: FIBI_USERNAME, FIBI_PASSWORD, CAL_USERNAME, CAL_PASSWORD,
 *           CAL2_USERNAME, CAL2_PASSWORD (second Cal card, optional)
 */

const { createScraper } = require('israeli-bank-scrapers');
const fs = require('fs');
const path = require('path');

const OUTPUT = path.join(__dirname, '..', 'scraped_data.json');

async function scrapeOne(companyId, credentials, label) {
  console.log(`\n📥 Scraping ${label}...`);
  const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH || undefined;
  const scraper = createScraper({
    companyId,
    verbose: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    timeout: 90000,
    ...(executablePath ? { executablePath } : {}),
  });
  try {
    const result = await scraper.scrape(credentials);
    if (!result.success) {
      console.error(`❌ ${label}: ${result.errorMessage}`);
      return { success: false, error: result.errorMessage, accounts: [] };
    }
    const total = result.accounts.reduce((n, a) => n + a.txns.length, 0);
    console.log(`✅ ${label}: ${result.accounts.length} accounts, ${total} txns`);
    return { success: true, accounts: result.accounts };
  } catch (err) {
    console.error(`❌ ${label} exception:`, err.message);
    return { success: false, error: err.message, accounts: [] };
  }
}

async function main() {
  const out = { scraped_at: new Date().toISOString() };

  const fibiUser = process.env.FIBI_USERNAME;
  const fibiPass = process.env.FIBI_PASSWORD;
  if (fibiUser && fibiPass) {
    out.fibi = await scrapeOne('beinleumi', { username: fibiUser, password: fibiPass }, 'הבינלאומי');
  } else {
    console.warn('⚠️  FIBI credentials missing — skipping');
    out.fibi = { success: false, error: 'no credentials', accounts: [] };
  }

  const calUser = process.env.CAL_USERNAME;
  const calPass = process.env.CAL_PASSWORD;
  if (calUser && calPass) {
    out.cal = await scrapeOne('visaCal', { username: calUser, password: calPass }, 'כאל');
  } else {
    console.warn('⚠️  CAL credentials missing — skipping');
    out.cal = { success: false, error: 'no credentials', accounts: [] };
  }

  const cal2User = process.env.CAL2_USERNAME;
  const cal2Pass = process.env.CAL2_PASSWORD;
  if (cal2User && cal2Pass) {
    out.cal2 = await scrapeOne('visaCal', { username: cal2User, password: cal2Pass }, 'כאל (כרטיס 2)');
  } else {
    console.warn('ℹ️  CAL2 credentials missing — skipping second Cal card');
    out.cal2 = { success: false, error: 'no credentials', accounts: [] };
  }

  fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2), 'utf8');
  console.log(`\n💾 Saved → ${OUTPUT}`);
}

main().catch(err => { console.error(err); process.exit(1); });
