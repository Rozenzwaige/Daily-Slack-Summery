// ── Tab names ─────────────────────────────────────────────────────────────────
var TAB_KNESSET   = 'כנסת';
var TAB_GOV       = 'ממשלה';
var TAB_COURTS    = 'בתי משפט';
var TAB_WIKI      = 'ויקיפדיה';
var TAB_WCH       = 'WCH';
var TAB_SOCIALIST = 'לוח השנה הסוציאליסטי';
var TAB_PERSONAL  = 'אירועים';           // personal events added via the webapp

// ── Serve the web app ─────────────────────────────────────────────────────────
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Calendar')
    .setTitle('לוח אירועים — רוזה')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

// ── Main data function ────────────────────────────────────────────────────────
// Returns { "DD/MM/YYYY": { allDay: [...], timed: [...] } }
// allDay item: { source, title, desc, link }
// timed item:  { source, time, title, desc, link }
function getCalendarData(startDateStr, nDays) {
  var ss  = SpreadsheetApp.getActiveSpreadsheet();
  var out = {};
  var dates   = buildDateStrings(startDateStr, nDays);
  var dateSet = Object.create(null);
  dates.forEach(function(d) {
    dateSet[d] = true;
    out[d] = { allDay: [], timed: [] };
  });

  // ── Timed: כנסת / ממשלה / בתי משפט
  // Columns: A=date  B=time  C=title  D=description  E=link
  [TAB_KNESSET, TAB_GOV, TAB_COURTS].forEach(function(tab) {
    var ws = ss.getSheetByName(tab);
    if (!ws) return;
    ws.getDataRange().getValues().slice(1).forEach(function(row) {
      var d = normDate(row[0]);
      if (!dateSet[d]) return;
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

  // ── All-day: ויקיפדיה / סוציאליסטי
  // Columns: A=date  B=(empty)  C=title  D=description  E=link
  [TAB_WIKI, TAB_SOCIALIST].forEach(function(tab) {
    var ws = ss.getSheetByName(tab);
    if (!ws) return;
    ws.getDataRange().getValues().slice(1).forEach(function(row) {
      var d = normDate(row[0]);
      if (!dateSet[d]) return;
      var title = String(row[2]).trim();
      if (!title) return;
      out[d].allDay.push({
        source: tab,
        title:  title,
        desc:   String(row[3]).trim(),
        link:   String(row[4]).trim()
      });
    });
  });

  // ── All-day: WCH (English titles translated, 4 columns)
  // Columns: A=date  B=title(EN)  C=body(EN)  D=media link
  var wchWs = ss.getSheetByName(TAB_WCH);
  if (wchWs) {
    wchWs.getDataRange().getValues().slice(1).forEach(function(row) {
      var d = normDate(row[0]);
      if (!dateSet[d]) return;
      var titleEn = String(row[1]).trim();
      if (!titleEn) return;
      out[d].allDay.push({
        source:  TAB_WCH,
        title:   translateCached(titleEn),
        titleEn: titleEn,
        desc:    '',
        link:    String(row[3]).trim()
      });
    });
  }

  // ── Personal events (אירועים)
  // Columns: A=date  B=time  C=title  D=description  E=link
  // Events without a time go to allDay; events with a time go to timed
  var persWs = ss.getSheetByName(TAB_PERSONAL);
  if (persWs) {
    persWs.getDataRange().getValues().slice(1).forEach(function(row) {
      var d = normDate(row[0]);
      if (!dateSet[d]) return;
      var title = String(row[2]).trim();
      if (!title) return;
      var time  = normTime(row[1]);
      var item  = {
        source: TAB_PERSONAL,
        time:   time,
        title:  title,
        desc:   String(row[3]).trim(),
        link:   String(row[4]).trim()
      };
      if (time) { out[d].timed.push(item); }
      else      { out[d].allDay.push(item); }
    });
  }

  // ── Sort timed events earliest-first
  Object.keys(out).forEach(function(d) {
    out[d].timed.sort(function(a, b) {
      var ta = a.time || '99:99';
      var tb = b.time || '99:99';
      return ta < tb ? -1 : ta > tb ? 1 : 0;
    });
  });

  return out;
}

// ── Add personal event ────────────────────────────────────────────────────────
// data: { date: "DD/MM/YYYY", time: "HH:MM" or "", title, desc, link }
function addEvent(data) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var ws = ss.getSheetByName(TAB_PERSONAL);
  if (!ws) {
    ws = ss.insertSheet(TAB_PERSONAL);
    ws.appendRow(['תאריך', 'שעה', 'אירוע', 'תיאור', 'קישור']);
  }
  ws.appendRow([
    data.date  || '',
    data.time  || '',
    data.title || '',
    data.desc  || '',
    data.link  || ''
  ]);
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

function normDate(v) {
  if (v instanceof Date) {
    return p2(v.getDate()) + '/' + p2(v.getMonth() + 1) + '/' + v.getFullYear();
  }
  return String(v).trim();
}

// GAS returns time-only cells as a Date object with epoch date Dec 30 1899.
// Extract just HH:MM regardless of whether we get a Date or a "HH:MM" string.
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
