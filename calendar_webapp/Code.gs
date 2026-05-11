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
  var wchWs = ss.getSheetByName(TAB_WCH);
  if (wchWs) {
    wchWs.getDataRange().getValues().slice(1).forEach(function(row) {
      var dm = normDateDM(row[0]);          // "DD/MM"
      var d  = dmMap[dm];
      if (!d) return;
      var titleEn = String(row[1]).trim();
      if (!titleEn) return;
      var histYear = extractYear(row[0]);
      var yearsAgo = (histYear && histYear < currentYear) ? currentYear - histYear : 0;
      var titleHe  = translateCached(titleEn);
      out[d].allDay.push({
        source: TAB_WCH,
        title:  yearsAgo > 0 ? 'לפני ' + yearsAgo + ' שנה: ' + titleHe : titleHe,
        desc:   '',
        link:   String(row[3]).trim()
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

function translateCached(text) {
  if (!text) return '';
  var cache = CacheService.getScriptCache();
  var key   = 'wch_' + text.slice(0, 80).replace(/\W/g, '_');
  var hit   = cache.get(key);
  if (hit) return hit;
  try {
    var tr = LanguageApp.translate(text, 'en', 'iw');
    cache.put(key, tr, 86400);
    return tr;
  } catch (e) {
    Logger.log('WCH translate failed: ' + text + ' — ' + e);
    return text;
  }
}
