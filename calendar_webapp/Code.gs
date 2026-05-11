// ── Tab names ─────────────────────────────────────────────────────────────────
var TAB_KNESSET   = 'כנסת';
var TAB_GOV       = 'ממשלה';
var TAB_COURTS    = 'בתי משפט';
var TAB_WIKI      = 'ויקיפדיה';
var TAB_WCH       = 'WCH';
var TAB_SOCIALIST = 'לוח השנה הסוציאליסטי';
var TAB_PERSONAL  = 'אירועים';

// ── Serve the web app ─────────────────────────────────────────────────────────
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Calendar')
    .setTitle('לוח אירועים — רוזה')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

// ── Main data function ────────────────────────────────────────────────────────
// Returns { "DD/MM/YYYY": { allDay: [...], timed: [...] } }
function getCalendarData(startDateStr, nDays) {
  var ss          = SpreadsheetApp.getActiveSpreadsheet();
  var out         = {};
  var dates       = buildDateStrings(startDateStr, nDays);
  var currentYear = new Date().getFullYear();

  // fullMap:  "DD/MM/YYYY" → true   (for exact-year matching)
  // dmMap:    "DD/MM"      → "DD/MM/YYYY"  (day-month only, historical events)
  var fullMap = Object.create(null);
  var dmMap   = Object.create(null);
  dates.forEach(function(d) {
    fullMap[d]          = true;
    dmMap[d.slice(0,5)] = d;          // "11/05" → "11/05/2026"
    out[d] = { allDay: [], timed: [] };
  });

  // ── Timed sources: כנסת / ממשלה / בתי משפט ──────────────────────────────
  // Columns: A=date  B=time  C=title  D=description  E=link
  [TAB_KNESSET, TAB_GOV, TAB_COURTS].forEach(function(tab) {
    var ws = ss.getSheetByName(tab);
    if (!ws) return;
    ws.getDataRange().getValues().slice(1).forEach(function(row) {
      var d = normDate(row[0]);
      if (!fullMap[d]) return;
      var title = String(row[2]).trim();
      if (!title) return;
      out[d].timed.push({
        source: tab,
        time:   normTime(row[1]),
        title:  title,
        desc:   String(row[3]).trim(),
        link:   String(row[4]).trim()
      });
    });
  });

  // ── ויקיפדיה — exact-year match (scraper writes current-year dates) ───────
  // Columns: A=date  B=(empty)  C=title  D=description  E=link
  var wikiWs = ss.getSheetByName(TAB_WIKI);
  if (wikiWs) {
    var wikiRows = wikiWs.getDataRange().getValues().slice(1);
    Logger.log('[wiki] rows in sheet: ' + wikiRows.length);
    wikiRows.forEach(function(row) {
      var d = normDate(row[0]);
      if (!fullMap[d]) return;
      var title = String(row[2]).trim();
      if (!title) return;
      out[d].allDay.push({
        source: TAB_WIKI,
        title:  title,
        desc:   String(row[3]).trim(),
        link:   String(row[4]).trim()
      });
    });
  }

  // ── WCH — DD/MM match only; historical years; English titles translated ───
  // Columns: A=date(historical)  B=title(EN)  C=text(EN)  D=media link
  // Display: "לפני X שנה: [translated title]"
  //
  // Performance: collect all titles that need translation first, then batch-
  // translate them in one HTTP call per 20 titles (instead of 1 call/title).
  var wchWs = ss.getSheetByName(TAB_WCH);
  if (wchWs) {
    var wchItems = [];
    wchWs.getDataRange().getValues().slice(1).forEach(function(row) {
      var dm = normDateDM(row[0]);
      var d  = dmMap[dm];
      if (!d) return;
      var titleEn = String(row[1]).trim();
      if (!titleEn) return;
      wchItems.push({ d: d, titleEn: titleEn,
                      histYear: extractYear(row[0]),
                      link: String(row[3]).trim() });
    });

    // Batch-translate titles not yet in cache
    var scriptCache = CacheService.getScriptCache();
    var uncached = [], seenKeys = Object.create(null);
    wchItems.forEach(function(it) {
      var k = cacheKey(it.titleEn);
      if (!scriptCache.get(k) && !seenKeys[k]) { uncached.push(it.titleEn); seenKeys[k] = true; }
    });
    if (uncached.length) batchTranslate(uncached);   // fills cache in chunks of 20

    // Now assemble rows using warmed cache
    wchItems.forEach(function(it) {
      var yearsAgo = (it.histYear && it.histYear < currentYear) ? currentYear - it.histYear : 0;
      var titleHe  = translateCached(it.titleEn);
      out[it.d].allDay.push({
        source: TAB_WCH,
        title:  yearsAgo > 0 ? 'לפני ' + yearsAgo + ' שנה: ' + titleHe : titleHe,
        desc:   '',
        link:   it.link
      });
    });
  }

  // ── סוציאליסטי — DD/MM match only; historical years; Hebrew titles ────────
  // Columns: A=date(historical)  B=title(HE)
  // Display: "לפני X שנה: [title]"  (or just title if current year)
  var socialWs = ss.getSheetByName(TAB_SOCIALIST);
  if (socialWs) {
    socialWs.getDataRange().getValues().slice(1).forEach(function(row) {
      var dm = normDateDM(row[0]);
      var d  = dmMap[dm];
      if (!d) return;
      var title = String(row[1]).trim();
      if (!title) return;
      var histYear = extractYear(row[0]);
      var yearsAgo = (histYear && histYear < currentYear) ? currentYear - histYear : 0;
      out[d].allDay.push({
        source: TAB_SOCIALIST,
        title:  yearsAgo > 0 ? 'לפני ' + yearsAgo + ' שנה: ' + title : title,
        desc:   '',
        link:   ''
      });
    });
  }

  // ── Personal events (אירועים) — exact-year match ─────────────────────────
  // Columns: A=date  B=time  C=title  D=description  E=link
  var persWs = ss.getSheetByName(TAB_PERSONAL);
  if (persWs) {
    persWs.getDataRange().getValues().slice(1).forEach(function(row) {
      var d = normDate(row[0]);
      if (!fullMap[d]) return;
      var title = String(row[2]).trim();
      if (!title) return;
      var time  = normTime(row[1]);
      var item  = { source: TAB_PERSONAL, time: time, title: title,
                    desc: String(row[3]).trim(), link: String(row[4]).trim() };
      if (time) { out[d].timed.push(item); }
      else      { out[d].allDay.push(item); }
    });
  }

  // ── Sort timed events earliest-first ─────────────────────────────────────
  Object.keys(out).forEach(function(d) {
    out[d].timed.sort(function(a, b) {
      return (a.time || '99:99') < (b.time || '99:99') ? -1 : 1;
    });
  });

  return out;
}

// ── Add personal event ────────────────────────────────────────────────────────
function addEvent(data) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var ws = ss.getSheetByName(TAB_PERSONAL);
  if (!ws) {
    ws = ss.insertSheet(TAB_PERSONAL);
    ws.appendRow(['תאריך', 'שעה', 'אירוע', 'תיאור', 'קישור']);
  }
  ws.appendRow([data.date||'', data.time||'', data.title||'', data.desc||'', data.link||'']);
  return { ok: true };
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function buildDateStrings(startStr, n) {
  var p    = startStr.split('/');
  var base = new Date(+p[2], +p[1] - 1, +p[0]);
  var out  = [];
  for (var i = 0; i < n; i++) {
    var d = new Date(base);
    d.setDate(base.getDate() + i);
    out.push(p2(d.getDate()) + '/' + p2(d.getMonth() + 1) + '/' + d.getFullYear());
  }
  return out;
}

// Full "DD/MM/YYYY" from a cell value
function normDate(v) {
  if (v instanceof Date) {
    return p2(v.getDate()) + '/' + p2(v.getMonth() + 1) + '/' + v.getFullYear();
  }
  return String(v).trim();
}

// "DD/MM" only — ignores the year (used for historical-date tabs)
function normDateDM(v) {
  var full = normDate(v);
  return full.slice(0, 5);   // "DD/MM"
}

// Extract the 4-digit year from a cell value
function extractYear(v) {
  var s = normDate(v);
  var m = s.match(/\/(\d{4})$/);
  return m ? +m[1] : null;
}

// GAS returns time-only cells as Date objects with epoch date Dec 30 1899
function normTime(v) {
  if (!v && v !== 0) return '';
  if (v instanceof Date) {
    return p2(v.getHours()) + ':' + p2(v.getMinutes());
  }
  var s = String(v).trim();
  var m = s.match(/(\d{1,2}):(\d{2})/);
  return m ? p2(+m[1]) + ':' + m[2] : '';
}

function p2(n) { return n < 10 ? '0' + n : String(n); }

// ── Translation helpers ───────────────────────────────────────────────────────

function cacheKey(text) {
  return 'wch_' + text.slice(0, 80).replace(/\W/g, '_');
}

// Translate one title — reads from cache, falls back to a single HTTP call.
// Prefer calling batchTranslate() first to warm the cache in bulk.
function translateCached(text) {
  if (!text) return '';
  var cache = CacheService.getScriptCache();
  var key   = cacheKey(text);
  var hit   = cache.get(key);
  if (hit) return hit;

  // Single-item fallback (should rarely be needed after batchTranslate)
  try {
    var url  = 'https://translate.googleapis.com/translate_a/single'
             + '?client=gtx&sl=en&tl=iw&dt=t&q='
             + encodeURIComponent(text);
    var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true, deadline: 10 });
    var json = JSON.parse(resp.getContentText());
    var tr   = json[0].map(function(seg) { return seg[0]; }).join('');
    if (tr) { cache.put(key, tr, 86400); return tr; }
  } catch (e) {
    Logger.log('translateCached failed: ' + text.slice(0, 60) + ' — ' + e);
  }
  return text;
}

// Batch-translate an array of English strings → stores results in script cache.
// Uses the dict-chrome-ex endpoint which accepts multiple `q` params in one call.
// Processed in chunks of 20 to stay within URL length limits.
function batchTranslate(texts) {
  if (!texts || !texts.length) return;
  var cache = CacheService.getScriptCache();
  var CHUNK = 20;

  for (var i = 0; i < texts.length; i += CHUNK) {
    var chunk = texts.slice(i, i + CHUNK);
    var qs    = chunk.map(function(t) { return 'q=' + encodeURIComponent(t); }).join('&');
    var url   = 'https://translate.googleapis.com/translate_a/t'
              + '?client=dict-chrome-ex&sl=en&tl=iw&' + qs;
    try {
      var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true, deadline: 30 });
      var json = JSON.parse(resp.getContentText());
      // Response: [["tr1"],["tr2"],...] or ["tr1","tr2",...]
      chunk.forEach(function(original, j) {
        var item       = json[j];
        var translated = Array.isArray(item) ? (item[0] || '') : String(item || '');
        if (translated) cache.put(cacheKey(original), translated, 86400);
      });
    } catch (e) {
      Logger.log('batchTranslate chunk ' + i + ' failed: ' + e);
    }
  }
}
