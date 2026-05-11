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

  var fullMap = Object.create(null);   // "DD/MM/YYYY" → true
  var dmMap   = Object.create(null);   // "DD/MM"      → "DD/MM/YYYY"
  dates.forEach(function(d) {
    fullMap[d] = true;
    dmMap[d.slice(0, 5)] = d;
    out[d] = { allDay: [], timed: [] };
  });

  // ── Timed: כנסת / ממשלה / בתי משפט ──────────────────────────────────────
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

  // ── ויקיפדיה — exact-year match ──────────────────────────────────────────
  // Columns: A=date  B=(empty)  C=title  D=description  E=link
  var wikiWs = ss.getSheetByName(TAB_WIKI);
  if (wikiWs) {
    wikiWs.getDataRange().getValues().slice(1).forEach(function(row) {
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

  // ── WCH — DD/MM match; reads pre-translated Hebrew title from column E ────
  // Columns: A=date(hist)  B=title(EN)  C=text(EN)  D=link  E=title(HE)
  // Column E is populated once by running translateWCHSheet() from the editor.
  // Display: "לפני X שנה: [title]"
  var wchWs = ss.getSheetByName(TAB_WCH);
  if (wchWs) {
    wchWs.getDataRange().getValues().slice(1).forEach(function(row) {
      var dm = normDateDM(row[0]);
      var d  = dmMap[dm];
      if (!d) return;
      var titleHe = String(row[4] || '').trim();   // col E: pre-translated
      var titleEn = String(row[1] || '').trim();
      var title   = titleHe || titleEn;            // fallback to English if not yet translated
      if (!title) return;
      var histYear = extractYear(row[0]);
      var yearsAgo = (histYear && histYear < currentYear) ? currentYear - histYear : 0;
      out[d].allDay.push({
        source: TAB_WCH,
        title:  yearsAgo > 0 ? 'לפני ' + yearsAgo + ' שנה: ' + title : title,
        desc:   '',
        link:   String(row[3]).trim()
      });
    });
  }

  // ── סוציאליסטי — DD/MM match; Hebrew titles in column B ─────────────────
  // Columns: A=date(hist)  B=title(HE)
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

  // ── Personal events ───────────────────────────────────────────────────────
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

// ── translateWCHSheet — run ONCE from the Apps Script editor ─────────────────
// Adds column E ("כותרת עברית") to the WCH sheet by translating column B.
// After this runs, getCalendarData reads from column E — no runtime translation.
function translateWCHSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var ws = ss.getSheetByName(TAB_WCH);
  if (!ws) { Logger.log('ERROR: WCH sheet not found'); return; }

  var lastRow = ws.getLastRow();
  if (lastRow < 2) { Logger.log('WCH sheet is empty'); return; }

  // Write header for column E (row 1)
  ws.getRange(1, 5).setValue('כותרת עברית');

  // Read all English titles (col B) and existing translations (col E)
  var colB = ws.getRange(2, 2, lastRow - 1, 1).getValues();  // B2:B
  var colE = ws.getRange(2, 5, lastRow - 1, 1).getValues();  // E2:E

  // Collect rows that still need translation
  var todo = [];
  for (var r = 0; r < colB.length; r++) {
    var en = String(colB[r][0]).trim();
    var he = String(colE[r][0]).trim();
    if (en && !he) todo.push({ offset: r, en: en });
  }
  Logger.log('Rows to translate: ' + todo.length);
  if (!todo.length) { Logger.log('All rows already translated.'); return; }

  // Batch translate (20 titles per HTTP call) and write results immediately
  var CHUNK = 100;   // Cloud Translation API supports up to 128 strings per call
  var done  = 0;
  for (var i = 0; i < todo.length; i += CHUNK) {
    var chunk = todo.slice(i, i + CHUNK);
    var translations = _fetchTranslations(chunk.map(function(x) { return x.en; }));
    chunk.forEach(function(item, j) {
      if (translations[j]) {
        ws.getRange(item.offset + 2, 5).setValue(translations[j]);
        done++;
      }
    });
    SpreadsheetApp.flush();
    Logger.log('Progress: ' + done + ' / ' + todo.length);
    if (i + CHUNK < todo.length) Utilities.sleep(400);
  }
  Logger.log('translateWCHSheet complete — ' + done + ' rows translated.');
}

// Sends up to 128 titles in one HTTP POST to Cloud Translation API v2.
// Requires TRANSLATE_API_KEY in Script Properties.
function _fetchTranslations(texts) {
  var key = PropertiesService.getScriptProperties().getProperty('TRANSLATE_API_KEY');
  if (!key) {
    Logger.log('TRANSLATE_API_KEY not set in Script Properties');
    return texts.map(function() { return ''; });
  }
  var url  = 'https://translation.googleapis.com/language/translate/v2?key=' + key;
  var body = JSON.stringify({ q: texts, source: 'en', target: 'iw', format: 'text' });
  try {
    var resp = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'application/json',
      payload: body,
      muteHttpExceptions: true,
      deadline: 30
    });
    var json = JSON.parse(resp.getContentText());
    if (json.error) {
      Logger.log('Translation API error: ' + JSON.stringify(json.error));
      return texts.map(function() { return ''; });
    }
    return json.data.translations.map(function(t) { return t.translatedText || ''; });
  } catch (e) {
    Logger.log('_fetchTranslations failed: ' + e);
    return texts.map(function() { return ''; });
  }
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

function normDate(v) {
  if (v instanceof Date) {
    return p2(v.getDate()) + '/' + p2(v.getMonth() + 1) + '/' + v.getFullYear();
  }
  return String(v).trim();
}

function normDateDM(v) {
  return normDate(v).slice(0, 5);   // "DD/MM"
}

function extractYear(v) {
  var m = normDate(v).match(/\/(\d{4})$/);
  return m ? +m[1] : null;
}

function normTime(v) {
  if (!v && v !== 0) return '';
  if (v instanceof Date) return p2(v.getHours()) + ':' + p2(v.getMinutes());
  var m = String(v).match(/(\d{1,2}):(\d{2})/);
  return m ? p2(+m[1]) + ':' + m[2] : '';
}

function p2(n) { return n < 10 ? '0' + n : String(n); }
