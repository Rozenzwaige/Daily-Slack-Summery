#!/usr/bin/env node
/**
 * Fetches current prices for Israeli mutual funds from the TASE website.
 * Uses Playwright to intercept the API calls that TASE's website makes internally.
 * Writes ../portfolio_prices.json
 */

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const OUTPUT = path.join(__dirname, '..', 'portfolio_prices.json');

const FUNDS = [
  { id: '5137534',  name: 'AI אילים מניות' },
  { id: '5105531',  name: 'S&P 500 אלטש' },
  { id: '5126008',  name: 'מיטב אג עד5% מנ' },
  { id: '5100474',  name: '125 מיטב ת"א' },
  { id: '5100151',  name: 'קא ענק טכנ חול' },
  { id: '5127121',  name: 'תמיר עקב 3 בינל' },
  { id: '5106562',  name: 'אגם מדינ FOREST' },
];

async function getFundPrice(page, fund) {
  const captured = {};

  // Intercept API responses from the TASE website
  const handler = async (response) => {
    const url = response.url();
    if (!url.includes('/api/') || !url.includes(fund.id)) return;
    try {
      const json = await response.json();
      // TASE API returns price in various field names — try all
      const price = json?.lastRate ?? json?.closingRate ?? json?.rate
        ?? json?.tradeData?.lastRate ?? json?.fund?.lastRate ?? null;
      const changePct = json?.dailyChangePercent ?? json?.changePercent
        ?? json?.tradeData?.dailyChangePercent ?? json?.percentageChange ?? null;
      const changeAmt = json?.dailyChangeRate ?? json?.dailyChange
        ?? json?.tradeData?.dailyChangeRate ?? null;

      if (price) {
        captured.price      = parseFloat(price);
        captured.changePct  = changePct != null ? parseFloat(changePct) : null;
        captured.changeAmt  = changeAmt != null ? parseFloat(changeAmt) : null;
      }
    } catch {}
  };

  page.on('response', handler);

  try {
    await page.goto(
      `https://www.tase.co.il/he/market-data/fund/${fund.id}/overview`,
      { waitUntil: 'networkidle', timeout: 30000 }
    );
    await page.waitForTimeout(2000); // Let async API calls complete
  } catch (e) {
    console.error(`  Timeout on ${fund.name}: ${e.message}`);
  }

  page.off('response', handler);

  // Fallback: try to read price directly from the DOM
  if (!captured.price) {
    try {
      const priceText = await page.locator('[class*="rate"],[class*="price"],[class*="Rate"]')
        .first().textContent({ timeout: 3000 });
      const num = parseFloat(priceText.replace(/[^0-9.]/g, ''));
      if (num > 0) captured.price = num;
    } catch {}
  }

  return captured;
}

async function main() {
  console.log('\n📈 Scraping TASE fund prices...');

  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
  });

  const context = await browser.newContext({
    locale: 'he-IL',
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  });

  const page = await context.newPage();
  const results = { scraped_at: new Date().toISOString(), funds: {} };

  for (const fund of FUNDS) {
    process.stdout.write(`  ${fund.name}... `);
    const data = await getFundPrice(page, fund);
    if (data.price) {
      results.funds[fund.id] = data;
      console.log(`₪${data.price.toFixed(2)} (${data.changePct != null ? (data.changePct > 0 ? '+' : '') + data.changePct.toFixed(2) + '%' : '?'})`);
    } else {
      results.funds[fund.id] = { price: null };
      console.log('❌ price not found');
    }
  }

  await browser.close();
  fs.writeFileSync(OUTPUT, JSON.stringify(results, null, 2), 'utf8');
  console.log(`\n💾 Saved → ${OUTPUT}`);
}

main().catch(err => { console.error(err); process.exit(1); });
