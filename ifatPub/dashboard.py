"""
dashboard.py — דשבורד ניטור מדיה עומדים ביחד
הרצה מקומית:   streamlit run dashboard.py
Streamlit Cloud: הגדר st.secrets (ראה .streamlit/secrets.toml.example)
"""
import os, json, re, base64
from collections import Counter
from datetime import date, timedelta

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2.service_account import Credentials
import gspread

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCOPES   = ["https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"]

# ── Standing Together brand palette ───────────────────────────────────────
ST_PRIMARY  = "#8B1A9D"
ST_PALETTE  = ["#8B1A9D","#B55BC8","#5C1070","#D4A0DC","#3D0950","#E8C8EE","#C94FDF","#7B0F8F"]
ST_GRADIENT = ["#3D0950","#5C1070","#8B1A9D","#B55BC8","#D4A0DC","#E8C8EE"]
SENT_COLORS = {"חיובי":"#B55BC8","נייטרלי":"#6B7280","ניטרלי":"#6B7280","שלילי":"#3D0950"}

_CHART_CFG = {
    "displayModeBar": True,
    "toImageButtonOptions": {"format": "png", "scale": 2},
    "modeBarButtonsToRemove": ["select2d","lasso2d","autoScale2d"],
    "displaylogo": False,
}

# ── page config ────────────────────────────────────────────────────────────
st.set_page_config(page_title="ניטור מדיה — עומדים ביחד",
                   page_icon="📰", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown("""
<style>
  /* ── RTL base ── */
  html,body,[class*="css"],.stApp{direction:rtl !important;}
  section[data-testid="stSidebar"] *{direction:rtl !important;text-align:right !important;}
  .stDataFrame th{text-align:right !important;}
  h1,h2,h3,h4,h5,[data-testid="stHeading"],[data-testid="stSubheader"]{text-align:right !important;}
  div[data-testid="metric-container"]>div{text-align:center !important;}
  .block-container{padding-top:1.2rem !important;}

  /* ── 5. Tabs: icon-only, right-aligned ── */
  .stTabs [data-baseweb="tab-list"]{
    direction:rtl !important;
    justify-content:flex-start !important;
    flex-direction:row !important;
  }
  .stTabs [data-baseweb="tab"] {
    padding: 8px 16px !important;
    font-size: 18px !important;
  }

  /* ── KPI metric buttons (pure st.button, marker-targeted) ── */
  div:has(> span.kpi-btn-marker) + div button {
    min-height: 78px !important;
    border-radius: 10px !important;
    white-space: pre-line !important;
    line-height: 1.6 !important;
    font-size: 13px !important;
    transition: all 0.15s !important;
  }
  div:has(> span.kpi-btn-marker) + div button[data-testid="baseButton-primary"] {
    background: #f5eeff !important;
    color: #5C1070 !important;
    border: 2px solid #8B1A9D !important;
    font-weight: 700 !important;
  }
  div:has(> span.kpi-btn-marker) + div button[data-testid="baseButton-secondary"] {
    background: white !important;
    color: #1a1a2e !important;
    border: 1px solid #e2d4ee !important;
  }
  div:has(> span.kpi-btn-marker) + div button:hover {
    border-color: #8B1A9D !important;
    box-shadow: 0 3px 10px rgba(139,26,157,.15) !important;
  }

  /* ── 8. Lighter refresh button ── */
  section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] {
    background: transparent !important;
    border: 1px solid #B55BC8 !important;
    color: #8B1A9D !important;
    font-size: 12px !important;
    padding: 2px 10px !important;
    box-shadow: none !important;
    border-radius: 6px !important;
  }
  section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"]:hover {
    background: #f5eeff !important;
    border-color: #8B1A9D !important;
  }

  /* ── 9. Radio buttons as text-box toggles ── */
  div[role="radiogroup"] {display:flex !important; flex-wrap:wrap; gap:4px;}
  div[role="radiogroup"] label {
    border: 1.5px solid #c4a0d4 !important;
    border-radius: 6px !important;
    padding: 3px 14px !important;
    cursor: pointer !important;
    user-select: none;
    background: white;
    color: #5C1070;
    font-size: 13px;
    transition: all 0.15s;
  }
  div[role="radiogroup"] label:has(input:checked) {
    background: #f5eeff !important;
    color: #5C1070 !important;
    border-color: #8B1A9D !important;
    font-weight: 600 !important;
  }
  div[role="radiogroup"] input[type="radio"] {display:none !important;}
  div[role="radiogroup"] svg {display:none !important;}
  div[role="radiogroup"] label > div:first-child {
    display:none !important; width:0 !important; margin:0 !important; padding:0 !important;
  }

  /* ── KPI card style ── */
  .kpi-card {
    border-radius: 10px;
    padding: 14px 10px;
    text-align: center;
    transition: all 0.2s;
    cursor: pointer;
    height: 82px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
  }
  .kpi-card .kpi-label {font-size:11px; font-weight:500; color:#8B1A9D; margin-bottom:4px;}
  .kpi-card .kpi-value {font-size:22px; font-weight:700; color:#1a1a2e; line-height:1.1;}
  .kpi-card.active {border: 2.5px solid #8B1A9D; background:#f5eeff;}
  .kpi-card.inactive {border: 1px solid #e2d4ee; background:white;}
</style>""", unsafe_allow_html=True)


# ══ Config ══════════════════════════════════════════════════════════════════
def _local_cfg():
    with open(os.path.join(BASE_DIR,"ifat_config.json"),encoding="utf-8") as f:
        return json.load(f)

def _sheet_ids():
    try:
        if "spreadsheet_id" in st.secrets:
            return st.secrets["spreadsheet_id"],st.secrets["sheet_name"],st.secrets.get("peace_sheet_name","שלום ישראלי פלסטיני")
    except Exception:
        pass
    cfg=_local_cfg()
    return cfg["spreadsheet_id"],cfg["sheet_name"],cfg.get("peace_sheet_name","שלום ישראלי פלסטיני")

@st.cache_resource
def _get_client():
    try:
        if "gcp_service_account" in st.secrets:
            return gspread.authorize(Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]),scopes=SCOPES))
    except Exception:
        pass
    cfg=_local_cfg()
    return gspread.authorize(Credentials.from_service_account_file(os.path.join(BASE_DIR,cfg["credentials_file"]),scopes=SCOPES))


# ══ Column constants ════════════════════════════════════════════════════════
C_DATE="תאריך"; C_TIME="שעה"; C_SOURCE="גוף תקשורת"; C_SECTION="מדור"
C_TITLE="כותרת"; C_CONTENT="תוכן"; C_REPORTER="כתב"; C_CHARS="דמויות"
C_LINK="קישור"; C_SERIAL="מספר סידורי"; C_LANG="שפה"; C_MEDIA="מדיה"
C_SENT="סנטימנט"; C_PUBTYPE="סוג פרסום"; C_TOPIC="נושא"
C_SECTOR="מגזר"; C_REACH="חשיפה"; C_VALUE="ערך"

ALL_COLS=[C_DATE,C_TIME,C_SOURCE,C_SECTION,C_TITLE,C_CONTENT,
          C_REPORTER,C_CHARS,C_LINK,C_SERIAL,C_LANG,C_MEDIA,
          C_SENT,C_PUBTYPE,C_TOPIC,C_SECTOR,C_REACH,C_VALUE]

# ── Hebrew stopwords + Standing Together terms ─────────────────────────────
_STOP = set("""
את של על עם אל לא כי הם הן זה זו כל יש אבל גם רק אם כך
הוא היא אנחנו אני אתה הם הן יהיה היה הייתה היו יהיו
מה מי ו ל מ ב כ ה לו לה לנו להם לי שלו שלה שלנו שלהם
אחד אחת כבר אין עוד כן לפי אחרי לפני בין רק גם עד
אשר שם לכן לאחר בזמן כאשר בגלל למרות אך אולם מאז
בשנת עוד אנו זאת אלה אלו כך כן לכן שהוא שהיא שהם
עומדים ביחד עומד יחד תנועת תנועה תנועות ישראל ישראלי
""".split())


# ══ Data loading ════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner="טוען נתונים מ-Google Sheets...")
def load_sheet(sheet_name: str) -> pd.DataFrame:
    sid,_,_ = _sheet_ids()
    try:
        ws   = _get_client().open_by_key(sid).worksheet(sheet_name)
        rows = ws.get_all_values()
    except Exception as e:
        st.error(f"שגיאה בטעינת הגיליון: {e}")
        return pd.DataFrame(columns=ALL_COLS)
    if len(rows)<2:
        return pd.DataFrame(columns=ALL_COLS)

    df = pd.DataFrame(rows[1:], columns=rows[0])

    col_by_pos = {i: df.columns[i] for i in range(len(df.columns))}
    _remap = {
        C_LANG:    col_by_pos.get(10, C_LANG),
        C_MEDIA:   col_by_pos.get(11, C_MEDIA),
        C_SENT:    col_by_pos.get(12, C_SENT),
        C_PUBTYPE: col_by_pos.get(13, C_PUBTYPE),
        C_TOPIC:   col_by_pos.get(14, C_TOPIC),
        C_SECTOR:  col_by_pos.get(15, C_SECTOR),
        C_REACH:   col_by_pos.get(16, C_REACH),
        C_VALUE:   col_by_pos.get(17, C_VALUE),
    }
    rename_map = {v: k for k,v in _remap.items() if v != k and v in df.columns}
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in ALL_COLS:
        if col not in df.columns:
            df[col] = ""

    parsed = pd.to_datetime(df[C_DATE], format="%d/%m/%Y", errors="coerce")
    bad = parsed.isna() & df[C_DATE].str.strip().astype(bool)
    if bad.any():
        parsed[bad] = pd.to_datetime(df.loc[bad,C_DATE], dayfirst=True, errors="coerce")
    df["_date"] = parsed

    if C_REACH in df.columns and isinstance(df[C_REACH], pd.Series):
        df[C_REACH] = pd.to_numeric(df[C_REACH], errors="coerce")
    if C_VALUE in df.columns and isinstance(df[C_VALUE], pd.Series):
        df[C_VALUE] = pd.to_numeric(df[C_VALUE], errors="coerce")
    df = df[df[C_TITLE].str.strip().astype(bool)|df[C_SOURCE].str.strip().astype(bool)]
    return df.reset_index(drop=True)


# ══ Metric mode helpers ══════════════════════════════════════════════════════
if "metric_mode" not in st.session_state:
    st.session_state.metric_mode = "count"

_MODE_LABELS = {"count": "כמות פרסומים", "reach": "חשיפה כוללת", "value": "ערך כולל"}
_MODE_YAXIS  = {"count": "פרסומים", "reach": "חשיפה", "value": "ערך (₪)"}

def _agg_metric(df_filtered, groupby_col):
    """Group df_filtered by groupby_col and aggregate per current metric_mode."""
    mode = st.session_state.metric_mode
    valid = df_filtered[df_filtered[groupby_col].replace("", pd.NA).notna()]
    if valid.empty:
        return pd.Series(dtype=float)
    grp = valid.groupby(groupby_col)
    if mode == "reach":
        return pd.to_numeric(grp[C_REACH].sum(), errors="coerce").fillna(0).sort_values(ascending=False)
    elif mode == "value":
        return pd.to_numeric(grp[C_VALUE].sum(), errors="coerce").fillna(0).sort_values(ascending=False)
    else:
        return grp.size().sort_values(ascending=False)

def _agg_timeline(df_filtered):
    """Aggregate timeline (by _date) per current metric_mode."""
    mode = st.session_state.metric_mode
    grp = df_filtered.groupby("_date")
    if mode == "reach":
        return pd.to_numeric(grp[C_REACH].sum(), errors="coerce").fillna(0).reset_index(name="ערך")
    elif mode == "value":
        return pd.to_numeric(grp[C_VALUE].sum(), errors="coerce").fillna(0).reset_index(name="ערך")
    else:
        return grp.size().reset_index(name="ערך")


# ══ Chart helpers ════════════════════════════════════════════════════════════
def _plot(fig, height=360, key=None):
    fig.update_layout(
        paper_bgcolor="#ffffff", plot_bgcolor="#ffffff", font_color="#1a1a2e",
        height=height,
    )
    fig_json = fig.to_json()
    uid = (key or "") + str(abs(hash(fig_json[:80])) % 9999999)
    clip_path = "M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>body{{margin:0;padding:0;overflow:hidden;background:#ffffff;}}</style>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
</head><body>
<div id="g{uid}" style="width:100%;height:{height}px;"></div>
<script id="fd{uid}" type="application/json">{fig_json}</script>
<script>
var f=JSON.parse(document.getElementById('fd{uid}').textContent);
f.layout=f.layout||{{}};
f.layout.paper_bgcolor='#ffffff';
f.layout.plot_bgcolor='#ffffff';
f.layout.font={{color:'#1a1a2e'}};
Plotly.newPlot('g{uid}',f.data,f.layout,{{
  responsive:true,
  displayModeBar:true,
  displaylogo:false,
  modeBarButtonsToRemove:['select2d','lasso2d','autoScale2d'],
  toImageButtonOptions:{{format:'png',scale:2}},
  modeBarButtonsToAdd:[{{
    name:'copy',
    title:'העתק לקליפבורד',
    icon:{{width:24,height:24,path:'{clip_path}'}},
    click:async function(gd){{
      try{{
        var img=await Plotly.toImage(gd,{{format:'png',scale:2}});
        var res=await fetch(img);var blob=await res.blob();
        if(navigator.clipboard&&window.ClipboardItem){{
          await navigator.clipboard.write([new ClipboardItem({{'image/png':blob}})]);
        }}else{{
          var a=document.createElement('a');a.href=img;a.download='chart.png';a.click();
        }}
      }}catch(e){{console.error(e);}}
    }}
  }}]
}});
</script></body></html>"""
    components.html(html, height=height+52, scrolling=False)


def _bar_or_pie(values, names, label, key):
    chart_type = st.radio("סוג תצוגה", ["עמודות","עוגה"], horizontal=True,
                          key=f"ct_{key}", label_visibility="collapsed")

    max_lbl = max((len(str(n)) for n in names), default=5)
    lmargin = max(140, min(max_lbl * 9, 280))
    n_items = len(names)

    if chart_type == "עוגה":
        colors = (ST_PALETTE * ((n_items // len(ST_PALETTE)) + 1))[:n_items]
        fig = go.Figure(go.Pie(
            values=list(values),
            labels=list(names),
            hole=0.35,
            marker=dict(colors=colors),
            textinfo="percent",
            textposition="inside",
            insidetextorientation="radial",
            pull=[0.03]*n_items,
        ))
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="v", x=1.01, y=0.5, font=dict(size=11)),
            margin=dict(t=30, b=10, l=10, r=160),
        )
        _plot(fig, height=max(360, n_items * 22 + 80), key=f"pie_{key}")
    else:
        paired = sorted(zip(values, names), reverse=True)
        vals_sorted  = [v for v, _ in paired]
        names_sorted = [n for _, n in paired]
        palette_cycle = (ST_PALETTE * ((n_items // len(ST_PALETTE)) + 1))[:n_items]
        fig = go.Figure(go.Bar(
            x=list(names_sorted),
            y=list(vals_sorted),
            text=list(vals_sorted),
            textposition="outside",
            texttemplate="%{y:,.0f}",
            cliponaxis=False,
            marker_color=palette_cycle,
        ))
        max_lbl_len = max((len(str(n)) for n in names_sorted), default=5)
        bmargin = max(80, min(max_lbl_len * 7, 200))
        fig.update_layout(
            showlegend=False,
            xaxis_title="", yaxis_title=_MODE_YAXIS[st.session_state.metric_mode],
            margin=dict(t=40, b=bmargin, l=40, r=10),
            xaxis=dict(automargin=True, tickfont=dict(size=12), tickangle=-35),
            yaxis=dict(range=[0, max(vals_sorted) * 1.3 if vals_sorted else 1]),
        )
        _plot(fig, height=max(320, n_items * 35 + 100), key=f"bar_{key}")

def _top_words(series: pd.Series, n=25) -> pd.DataFrame:
    text  = " ".join(series.dropna().astype(str))
    words = [w for w in re.findall(r"[\u0590-\u05FF]{2,}", text) if w not in _STOP]
    return pd.DataFrame(Counter(words).most_common(n), columns=["מילה","ספירה"])

def _wordcloud(series: pd.Series):
    try:
        from wordcloud import WordCloud
        import matplotlib.pyplot as plt
        from bidi.algorithm import get_display
    except ImportError:
        return None
    text  = " ".join(series.dropna().astype(str))
    words = [w for w in re.findall(r"[\u0590-\u05FF]{2,}", text) if w not in _STOP]
    if not words:
        return None
    freq = Counter(words)
    freq_vis = {get_display(w): c for w, c in freq.items()}
    font_candidates = [os.path.join(BASE_DIR,"fonts","hebrew.ttf"),
                       "C:/Windows/Fonts/arial.ttf","C:/Windows/Fonts/ARIALUNI.TTF",
                       "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    font_path = next((p for p in font_candidates if os.path.exists(p)), None)
    wc = WordCloud(width=1200,height=360,background_color="#ffffff",
                   font_path=font_path,prefer_horizontal=1.0,max_words=80,
                   colormap="Purples").generate_from_frequencies(freq_vis)
    fig,ax = plt.subplots(figsize=(12,3.6))
    fig.patch.set_facecolor("#ffffff"); ax.set_facecolor("#ffffff")
    ax.imshow(wc,interpolation="bilinear"); ax.axis("off")
    plt.tight_layout(pad=0)
    return fig


# ══ SIDEBAR ══════════════════════════════════════════════════════════════════
_,SHEET_MAIN,SHEET_PEACE = _sheet_ids()

with st.sidebar:
    # 6. Logo instead of text title — accepts logo.png / logo.jpg / logo.jpeg
    _logo_path = next(
        (os.path.join(BASE_DIR, f) for f in ("logo.png","logo.jpg","logo.jpeg")
         if os.path.exists(os.path.join(BASE_DIR, f))),
        None
    )
    if _logo_path:
        st.image(_logo_path, use_container_width=True)
    else:
        st.markdown("""
        <div style="text-align:center;padding:8px 0 4px;">
          <span style="font-size:22px;font-weight:900;color:#8B1A9D;
                       font-family:Arial,sans-serif;letter-spacing:-1px;">
            עומדים ביחד
          </span>
        </div>
        """, unsafe_allow_html=True)

    sheet_choice = st.radio("גיליון",[SHEET_MAIN,SHEET_PEACE],index=0)
    df_full = load_sheet(sheet_choice)

    # 8. Refresh button — lighter style via CSS above
    if st.button("🔄 רענן נתונים"):
        st.cache_data.clear(); st.rerun()

    st.divider()
    st.subheader("סינונים")
    def _opts(col):
        return sorted(df_full[col].replace("",pd.NA).dropna().unique().tolist()) if col in df_full.columns else []

    # 7. placeholder="בחר" on all multiselects
    filt_lang   = st.multiselect("שפה",        _opts(C_LANG),   placeholder="בחר")
    filt_media  = st.multiselect("מדיה",       _opts(C_MEDIA),  placeholder="בחר")
    filt_sent   = st.multiselect("סנטימנט",    _opts(C_SENT),   placeholder="בחר")
    filt_sector = st.multiselect("מגזר",       _opts(C_SECTOR), placeholder="בחר")
    filt_source = st.multiselect("גוף תקשורת",_opts(C_SOURCE),  placeholder="בחר")


# ══ Date range ════════════════════════════════════════════════════════════════
valid_dates = df_full["_date"].dropna()
min_d = valid_dates.min().date() if not valid_dates.empty else date.today()-timedelta(days=90)
max_d = valid_dates.max().date() if not valid_dates.empty else date.today()

dr1,dr2,_ = st.columns([1,1,2])
_default_from = max(min_d, max_d - timedelta(days=7))
with dr1: d_from = st.date_input("מתאריך",   value=_default_from,min_value=min_d,max_value=max_d,format="DD/MM/YYYY")
with dr2: d_to   = st.date_input("עד תאריך", value=max_d,min_value=min_d,max_value=max_d,format="DD/MM/YYYY")


# ══ Apply filters ═════════════════════════════════════════════════════════════
df = df_full[(df_full["_date"].dt.date>=d_from)&(df_full["_date"].dt.date<=d_to)].copy()
if filt_lang:   df=df[df[C_LANG].isin(filt_lang)]
if filt_media:  df=df[df[C_MEDIA].isin(filt_media)]
if filt_sent:   df=df[df[C_SENT].isin(filt_sent)]
if filt_sector: df=df[df[C_SECTOR].isin(filt_sector)]
if filt_source: df=df[df[C_SOURCE].isin(filt_source)]


# ══ KPI row + metric toggle (sticky) ══════════════════════════════════════════
def _safe_col(df, col):
    if col not in df.columns: return pd.Series(dtype=float)
    s = df[col]
    return pd.to_numeric(s.iloc[:, 0] if isinstance(s, pd.DataFrame) else s, errors="coerce")

_reach_num = _safe_col(df, C_REACH)
_value_num = _safe_col(df, C_VALUE)
days = max((pd.Timestamp(d_to)-pd.Timestamp(d_from)).days,1)

# ── KPI metric buttons — pure st.button, CSS-styled via marker span ──
# The marker span lets us target only these buttons in CSS with :has()
st.markdown('<span class="kpi-btn-marker" style="display:none"></span>',
            unsafe_allow_html=True)
kpi_cols = st.columns([1,1,1,1])
_kpi_data = [
    ("count", "סה״כ פרסומים",  f"{len(df):,}"),
    ("reach", "חשיפה כוללת",   f"{int(_reach_num.sum()):,}" if _reach_num.sum()>0 else "—"),
    ("value", "ערך כולל (₪)",  f"{int(_value_num.sum()):,}" if _value_num.sum()>0 else "—"),
]
for col_ref, (mode, label, val) in zip(kpi_cols[:3], _kpi_data):
    with col_ref:
        is_active = st.session_state.metric_mode == mode
        if st.button(f"{val}\n{label}", key=f"kpi_{mode}",
                     use_container_width=True,
                     type="primary" if is_active else "secondary"):
            st.session_state.metric_mode = mode
            st.rerun()

with kpi_cols[3]:
    st.metric("ממוצע יומי", f"{len(df)/days:.1f}")

st.divider()

# ── JS: KPI card clicks + compact toolbar bar on scroll ──
components.html("""<script>
(function(){
  /* pwin / d = references to the PARENT (Streamlit app) frame */
  var pwin = window.parent;
  var d    = pwin.document;
  var BAR_ID = '_st_kpi_compact';

  /* ────────────────────────────────────────────────
     findKpiBtns — locate the three KPI st.buttons
     by matching Hebrew substrings in their text.
     Returns [{btn, mode, text, active}, ...]
  ──────────────────────────────────────────────── */
  var KPI_SIGS = [
    {mode:'count', sig:'\u05e4\u05e8\u05e1\u05d5\u05de\u05d9\u05dd'},   /* פרסומים */
    {mode:'reach', sig:'\u05d7\u05e9\u05d9\u05e4\u05d4'},                /* חשיפה   */
    {mode:'value', sig:'\u05e2\u05e8\u05da \u05db\u05d5\u05dc\u05dc'}    /* ערך כולל */
  ];

  function findKpiBtns() {
    var result = [];
    var allBtns = d.querySelectorAll('button[data-testid^="baseButton"]');
    allBtns.forEach(function(btn) {
      var txt = (btn.innerText || btn.textContent || '').trim();
      KPI_SIGS.forEach(function(k) {
        if (txt.indexOf(k.sig) !== -1) {
          result.push({
            btn:    btn,
            mode:   k.mode,
            text:   txt,
            active: btn.getAttribute('data-testid') === 'baseButton-primary'
          });
        }
      });
    });
    return result;
  }

  /* ────────────────────────────────────────────────
     _kpiClick — click the matching KPI st.button so
     Streamlit updates session_state and reruns
  ──────────────────────────────────────────────── */
  pwin._kpiClick = function(mode) {
    findKpiBtns().forEach(function(k) {
      if (k.mode === mode) k.btn.click();
    });
  };

  /* ────────────────────────────────────────────────
     Compact toolbar bar — injected into stToolbar
  ──────────────────────────────────────────────── */
  function getBar() {
    var bar = d.getElementById(BAR_ID);
    if (!bar) {
      bar = d.createElement('div');
      bar.id = BAR_ID;
      bar.style.cssText =
        'display:none;align-items:center;gap:6px;direction:rtl;' +
        'font-size:12px;flex:1 1 auto;padding:0 8px;overflow:hidden;min-width:0;';
      var toolbar = d.querySelector('[data-testid="stToolbar"]');
      if (toolbar) {
        toolbar.style.overflow = 'visible';
        toolbar.insertBefore(bar, toolbar.firstChild);
      }
    }
    return bar;
  }

  function buildBar() {
    var bar = getBar();
    /* clear */
    while (bar.firstChild) bar.removeChild(bar.firstChild);

    /* date range */
    var di = d.querySelectorAll('[data-testid="stDateInput"] input');
    if (di.length >= 2) {
      var from = (di[0].value||'').trim(), to = (di[1].value||'').trim();
      if (from && to) {
        var ds = d.createElement('span');
        ds.style.cssText = 'color:#999;white-space:nowrap;font-size:11px;';
        ds.textContent = to + ' \u2013 ' + from;
        bar.appendChild(ds);
        var sep = d.createElement('span');
        sep.style.cssText = 'color:#ddd;margin:0 2px;';
        sep.textContent = '\u2502';
        bar.appendChild(sep);
      }
    }

    /* KPI mini-chips — built from the live st.button elements */
    findKpiBtns().forEach(function(k) {
      var act  = k.active;
      var chip = d.createElement('span');
      chip.style.cssText =
        'border:1.5px solid '+(act?'#8B1A9D':'#d8c4e8')+';'+
        'background:'+(act?'#f5eeff':'white')+';border-radius:6px;'+
        'padding:2px 10px;cursor:pointer;white-space:nowrap;'+
        'color:'+(act?'#5C1070':'#999')+';font-weight:'+(act?'600':'400')+';';
      chip.textContent = k.text.replace(/\n/g, ' ');
      var m = k.mode;
      chip.addEventListener('click', function(){ pwin._kpiClick(m); });
      bar.appendChild(chip);
    });
  }

  function checkScroll() {
    var bar     = getBar();
    var kpiBtns = findKpiBtns();
    if (!kpiBtns.length) { bar.style.display = 'none'; return; }
    var rect = kpiBtns[0].btn.getBoundingClientRect();
    if (rect.bottom < 58) {
      buildBar();
      bar.style.display = 'flex';
    } else {
      bar.style.display = 'none';
    }
  }

  /* ────────────────────────────────────────────────
     Init + continuous maintenance
  ──────────────────────────────────────────────── */
  function init() {
    getBar();

    var mainEl = d.querySelector('section[data-testid="stMain"]');
    if (mainEl) mainEl.addEventListener('scroll', checkScroll, {passive:true});
    pwin.addEventListener('scroll', checkScroll, {passive:true});

    setInterval(function() {
      var bar = d.getElementById(BAR_ID);
      if (bar && bar.style.display !== 'none') buildBar();
      checkScroll();
    }, 700);

    /* re-check after Streamlit re-renders */
    new MutationObserver(function() { checkScroll(); })
      .observe(d.body, {childList:true, subtree:true});
  }

  /* Run after Streamlit finishes rendering */
  setTimeout(init, 400);
  setTimeout(init, 1200);
})();
</script>""", height=1, scrolling=False)


# ══ TABS — icon only, right-aligned ══════════════════════════════════════════
# 5. Labels are emoji-only; CSS right-aligns them
tab_charts, tab_search, tab_settings = st.tabs(["📊", "🔍", "⚙️"])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — CHARTS
# ─────────────────────────────────────────────────────────────────────────────
with tab_charts:
    if df.empty:
        st.info("אין נתונים לטווח התאריכים הנבחר.")
    else:
        st.caption("💡 ריחוף מעל גרף → סמל מצלמה → הורדת PNG")

        # ── Row 1: timeline ──────────────────────────────────────────────
        st.subheader(f"פרסומים לפי יום — {_MODE_LABELS[st.session_state.metric_mode]}")
        show_trend = st.checkbox("הצג קו מגמה (ממוצע נע 7 ימים)")
        tl = _agg_timeline(df)
        tl_max = tl["ערך"].max() if not tl.empty else 1
        fig_tl = go.Figure(go.Bar(
            x=list(tl["_date"]),
            y=list(tl["ערך"]),
            text=list(tl["ערך"]),
            textposition="outside",
            texttemplate="%{y:,.0f}",
            cliponaxis=False,
            marker_color=ST_PRIMARY,
            name=_MODE_LABELS[st.session_state.metric_mode],
        ))
        fig_tl.update_layout(
            xaxis_title="תאריך",
            yaxis_title=_MODE_YAXIS[st.session_state.metric_mode],
            yaxis=dict(range=[0, tl_max * 1.3]),
        )
        if show_trend and len(tl)>=7:
            tl["מגמה"] = tl["ערך"].rolling(7,min_periods=1).mean()
            fig_tl.add_trace(go.Scatter(x=tl["_date"],y=tl["מגמה"],mode="lines",
                                        name="ממוצע נע",
                                        line=dict(color="#E8C8EE",width=2,dash="dot")))
        fig_tl.update_xaxes(tickformat="%d/%m/%Y")
        fig_tl.update_layout(margin=dict(t=40,b=60,l=60,r=10))
        _plot(fig_tl, height=360, key="timeline")

        # ── Row 2: media + language ──────────────────────────────────────
        c1,c2 = st.columns(2)
        with c1:
            st.subheader("פילוג מדיה")
            mc = _agg_metric(df, C_MEDIA)
            _bar_or_pie(mc.values, mc.index.tolist(), C_MEDIA, "media")
        with c2:
            st.subheader("שפת פרסום")
            lc = _agg_metric(df, C_LANG)
            _bar_or_pie(lc.values, lc.index.tolist(), C_LANG, "lang")

        # ── Row 3: sentiment + sector ────────────────────────────────────
        c3,c4 = st.columns(2)
        with c3:
            st.subheader("סנטימנט")
            sc = _agg_metric(df, C_SENT)
            chart_type_sent = st.radio("סוג תצוגה",["עמודות","עוגה"],horizontal=True,
                                       key="ct_sent",label_visibility="collapsed")
            if not sc.empty:
                sc_labels = list(sc.index)
                sc_vals   = list(sc.values)
                sc_colors = [SENT_COLORS.get(s, ST_PRIMARY) for s in sc_labels]
                if chart_type_sent=="עוגה":
                    fig = go.Figure(go.Pie(
                        labels=sc_labels, values=sc_vals, hole=0.35,
                        marker=dict(colors=sc_colors),
                        textinfo="percent", textposition="inside",
                        insidetextorientation="radial",
                        pull=[0.03]*len(sc_labels),
                    ))
                    fig.update_layout(showlegend=True,legend=dict(orientation="v",x=1.01,y=0.5,font=dict(size=11)),
                                      margin=dict(t=30,b=10,l=10,r=160))
                    _plot(fig, height=320, key="sent_pie")
                else:
                    sc_max = max(sc_vals) if sc_vals else 1
                    fig = go.Figure(go.Bar(
                        x=sc_labels, y=sc_vals,
                        text=sc_vals, textposition="outside",
                        texttemplate="%{y:,.0f}",
                        cliponaxis=False,
                        marker_color=sc_colors,
                    ))
                    fig.update_layout(showlegend=False, xaxis_title="",
                                      yaxis_title=_MODE_YAXIS[st.session_state.metric_mode],
                                      yaxis=dict(range=[0, sc_max*1.3]),
                                      margin=dict(t=40,b=60,l=60,r=10))
                    _plot(fig, height=320, key="sent_bar")
        with c4:
            st.subheader("פילוג מגזרים")
            sec = _agg_metric(df, C_SECTOR)
            _bar_or_pie(sec.values, sec.index.tolist(), C_SECTOR, "sector")

        # ── Row 4: pub type + topic ──────────────────────────────────────
        c5,c6 = st.columns(2)
        with c5:
            st.subheader("סוג פרסום")
            pt_raw = df[C_PUBTYPE].replace("",pd.NA).dropna()
            pt_exploded = pt_raw.str.split(",").explode().str.strip().replace("",pd.NA).dropna()
            if st.session_state.metric_mode == "count":
                pt_cnt = pt_exploded.value_counts()
            else:
                # For reach/value: explode + merge back to get per-row metric
                pt_df = df[[C_PUBTYPE, C_REACH, C_VALUE]].copy()
                pt_df = pt_df[pt_df[C_PUBTYPE].replace("",pd.NA).notna()]
                pt_df = pt_df.assign(**{C_PUBTYPE: pt_df[C_PUBTYPE].str.split(",")}).explode(C_PUBTYPE)
                pt_df[C_PUBTYPE] = pt_df[C_PUBTYPE].str.strip()
                pt_df = pt_df[pt_df[C_PUBTYPE].replace("",pd.NA).notna()]
                metric_col = C_REACH if st.session_state.metric_mode=="reach" else C_VALUE
                pt_cnt = pd.to_numeric(pt_df.groupby(C_PUBTYPE)[metric_col].sum(), errors="coerce").fillna(0).sort_values(ascending=False)
            if not pt_cnt.empty:
                _bar_or_pie(pt_cnt.values, pt_cnt.index.tolist(), C_PUBTYPE, "pubtype")
            else:
                st.info("אין נתונים בעמודה זו")
        with c6:
            st.subheader("נושא / קמפיין")
            tp = df[[C_TOPIC, C_REACH, C_VALUE]].copy()
            tp = tp[tp[C_TOPIC].replace("",pd.NA).notna()]
            tp = tp.assign(**{C_TOPIC: tp[C_TOPIC].str.split(",")}).explode(C_TOPIC)
            tp[C_TOPIC] = tp[C_TOPIC].str.strip()
            tp = tp[tp[C_TOPIC].replace("",pd.NA).notna()]
            if st.session_state.metric_mode == "count":
                tp_cnt = tp[C_TOPIC].value_counts()
            else:
                metric_col = C_REACH if st.session_state.metric_mode=="reach" else C_VALUE
                tp_cnt = pd.to_numeric(tp.groupby(C_TOPIC)[metric_col].sum(), errors="coerce").fillna(0).sort_values(ascending=False)
            if not tp_cnt.empty:
                _bar_or_pie(tp_cnt.values, tp_cnt.index.tolist(), C_TOPIC, "topic")
            else:
                st.info("אין נתונים בעמודה זו")

        # ── Row 5: top sources ───────────────────────────────────────────
        st.subheader("גופי תקשורת מובילים")
        _raw_src = st.slider("מספר גופים להצגה",5,30,20,key="n_src")
        n_sources = 35 - _raw_src
        src_agg = _agg_metric(df, C_SOURCE).head(n_sources).reset_index()
        src_agg.columns = ["גוף","ספירה"]
        chart_type_src = st.radio("סוג תצוגה",["עמודות","עוגה"],horizontal=True,
                                   key="ct_src",label_visibility="collapsed")
        if chart_type_src=="עוגה":
            fig=px.pie(values=src_agg["ספירה"],names=src_agg["גוף"],hole=0.3,color_discrete_sequence=ST_PALETTE)
            fig.update_traces(textinfo="percent+label",textposition="outside",pull=[0.03]*len(src_agg))
            fig.update_layout(showlegend=True,legend=dict(orientation="v",x=1.01,y=0.5,font=dict(size=11)),
                              margin=dict(t=30,b=10,l=10,r=160))
            _plot(fig, height=max(420, n_sources*22+80), key="src_pie")
        else:
            palette_src=(ST_PALETTE*((n_sources//len(ST_PALETTE))+1))[:n_sources]
            src_names = list(src_agg["גוף"])
            src_vals  = list(src_agg["ספירה"])
            fig = go.Figure(go.Bar(x=src_vals, y=src_names, orientation="h",
                                   text=src_vals, textposition="outside",
                                   texttemplate="%{x:,.0f}", cliponaxis=False,
                                   marker_color=palette_src))
            max_src_lbl=max((len(str(n)) for n in src_names),default=5)
            lmargin_src=max(140,min(max_src_lbl*9,280))
            fig.update_layout(showlegend=False,
                              xaxis_title=_MODE_YAXIS[st.session_state.metric_mode],
                              yaxis_title="",
                              margin=dict(t=30,b=50,l=lmargin_src,r=10),
                              yaxis=dict(automargin=True,tickfont=dict(size=12),categoryorder="total ascending"))
            _plot(fig, height=max(320,n_sources*30+60), key="src_bar")

        # ── Row 6: word frequency ────────────────────────────────────────
        st.subheader("מילים נפוצות")
        _raw_words = st.slider("מספר מילים להצגה", 5, 30, 20, key="n_words")
        n_words = 35 - _raw_words
        w1,w2 = st.columns(2)
        with w1:
            st.caption("בכותרות")
            wdf_t=_top_words(df[C_TITLE], n=n_words)
            if not wdf_t.empty:
                n_wt=len(wdf_t)
                palette_wt=(ST_PALETTE*((n_wt//len(ST_PALETTE))+1))[:n_wt]
                fig=go.Figure(go.Bar(x=list(wdf_t["ספירה"]),y=list(wdf_t["מילה"]),
                                     orientation="h",
                                     text=list(wdf_t["ספירה"]),textposition="outside",
                                     texttemplate="%{x:,.0f}", cliponaxis=False,
                                     marker_color=palette_wt))
                max_wt_lbl=max((len(str(w)) for w in wdf_t["מילה"].tolist()),default=4)
                wt_max=wdf_t["ספירה"].max() if not wdf_t.empty else 1
                fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="",
                                  margin=dict(t=30,b=40,l=max(80,min(max_wt_lbl*9,200)),r=10),
                                  xaxis=dict(range=[0, wt_max*1.3]),
                                  yaxis=dict(automargin=True,tickfont=dict(size=12),categoryorder="total ascending"))
                _plot(fig, height=max(400,n_wt*22+60), key="words_title")
        with w2:
            st.caption("בתוכן")
            wdf_c=_top_words(df[C_CONTENT], n=n_words)
            if not wdf_c.empty:
                n_wc=len(wdf_c)
                palette_wc=(ST_PALETTE*((n_wc//len(ST_PALETTE))+1))[:n_wc]
                fig=go.Figure(go.Bar(x=list(wdf_c["ספירה"]),y=list(wdf_c["מילה"]),
                                     orientation="h",
                                     text=list(wdf_c["ספירה"]),textposition="outside",
                                     texttemplate="%{x:,.0f}", cliponaxis=False,
                                     marker_color=palette_wc))
                max_wc_lbl=max((len(str(w)) for w in wdf_c["מילה"].tolist()),default=4)
                wc_max=wdf_c["ספירה"].max() if not wdf_c.empty else 1
                fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="",
                                  margin=dict(t=30,b=40,l=max(80,min(max_wc_lbl*9,200)),r=10),
                                  xaxis=dict(range=[0, wc_max*1.3]),
                                  yaxis=dict(automargin=True,tickfont=dict(size=12),categoryorder="total ascending"))
                _plot(fig, height=max(400,n_wc*22+60), key="words_content")

        # ── Word cloud ────────────────────────────────────────────────────
        st.subheader("ענן מילים")
        combined = df[C_TITLE].fillna("")+" "+df[C_CONTENT].fillna("")
        fig_wc = _wordcloud(combined)
        if fig_wc:
            st.pyplot(fig_wc)
        else:
            st.info("התקן `wordcloud` + `python-bidi`: `pip install wordcloud python-bidi`")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — SEARCH & BROWSE
# ─────────────────────────────────────────────────────────────────────────────
with tab_search:
    st.subheader("חיפוש וגלישה בכתבות")

    # ── שורת חיפוש + מיון ────────────────────────────────────────────────────
    sc1, sc2 = st.columns([3, 1])
    with sc1:
        query = st.text_input(
            "🔍 חיפוש חופשי — כותרת / תוכן / גוף תקשורת",
            placeholder='דוגמאות: גרין  |  "שלום" AND "פלסטיני"  |  אלון לי OR רולא')
        st.caption("תמיכה ב־ **AND** (שני מונחים חייבים להופיע) ו־ **OR** (אחד מהם מספיק). "
                   'ניתן לעטוף ביטוי מדויק במרכאות: `"קיצוני ימין"`')
    with sc2:
        sort_by = st.selectbox("מיין לפי",
                               ["תאריך (חדש→ישן)", "תאריך (ישן→חדש)", "חשיפה ↓", "ערך ↓"])

    # ── סינוני דמויות + סוג פרסום ────────────────────────────────────────────
    # נבנה מ-df_full (כל הנתונים) כדי שהרשימה תהיה שלמה ללא תלות בפילטר התאריכים
    def _split_multivals(series):
        """מחלץ ערכים בודדים מעמודה שמכילה ערכים מופרדים בפסיק"""
        return sorted({
            item.strip()
            for v in series.dropna()
            for item in str(v).split(",") if item.strip()
        })

    # רשימת הדמויות הקבועה — ממוינת לפי א-ב
    _KNOWN_CHARS = [
        "אורי וולטמן",
        "איתמר אבנרי",
        "אלון-לי גרין",
        "אליה לוין",
        "אמין אמארה",
        "גדיר האני",
        "יעל אגמון נכט",
        "מאיה פרץ",
        "מנאר קעדאן",
        "סאלי עבד",
        "עמרי גורן",
        "רביע אלעאסם",
        "רולא דאוד",
    ]

    sf1, sf2 = st.columns(2)
    with sf1:
        filt_chars_s = st.multiselect("👤 דמויות", _KNOWN_CHARS, placeholder="בחר")
    with sf2:
        _pubtype_vals = _split_multivals(df_full[C_PUBTYPE]) if C_PUBTYPE in df_full.columns else []
        filt_pubtype_s = st.multiselect("📄 סוג פרסום", _pubtype_vals, placeholder="בחר")

    # ── לוגיקת חיפוש טקסט ────────────────────────────────────────────────────
    def _hits(df_in, term):
        """Boolean Series: האם המונח מופיע ב-כותרת / תוכן / גוף?"""
        t = term.strip().strip('"')
        if not t:
            return pd.Series(True, index=df_in.index)
        p = re.escape(t)
        return (df_in[C_TITLE].str.contains(p, case=False, na=False) |
                df_in[C_CONTENT].str.contains(p, case=False, na=False) |
                df_in[C_SOURCE].str.contains(p, case=False, na=False))

    df_s = df.copy()
    q = query.strip()
    if q:
        if re.search(r'\bOR\b', q, re.IGNORECASE):
            # כל אחד מהמונחים מספיק (OR)
            terms = re.split(r'\bOR\b', q, flags=re.IGNORECASE)
            mask = pd.Series(False, index=df_s.index)
            for t in terms:
                mask |= _hits(df_s, t)
            df_s = df_s[mask]
        elif re.search(r'\bAND\b', q, re.IGNORECASE):
            # כל המונחים חייבים להופיע (AND)
            terms = re.split(r'\bAND\b', q, flags=re.IGNORECASE)
            mask = pd.Series(True, index=df_s.index)
            for t in terms:
                mask &= _hits(df_s, t)
            df_s = df_s[mask]
        else:
            df_s = df_s[_hits(df_s, q)]

    # ── סינון דמויות — חיפוש substring (מספיק שהשם מופיע בתוך תוכן התא) ─────────
    if filt_chars_s:
        wanted = set(filt_chars_s)
        def _has_char(val):
            if not val or pd.isna(val): return False
            cell = str(val)
            return any(name in cell for name in wanted)
        df_s = df_s[df_s[C_CHARS].apply(_has_char)]

    # ── סינון סוג פרסום (ערכים מופרדים בפסיק בתא, בדיוק כמו דמויות) ────────────
    if filt_pubtype_s:
        wanted_pt = set(filt_pubtype_s)
        def _has_pubtype(val):
            if not val or pd.isna(val): return False
            return bool({p.strip() for p in str(val).split(",")} & wanted_pt)
        df_s = df_s[df_s[C_PUBTYPE].apply(_has_pubtype)]

    # ── מיון ─────────────────────────────────────────────────────────────────
    sort_map = {"תאריך (חדש→ישן)": ("_date", False), "תאריך (ישן→חדש)": ("_date", True),
                "חשיפה ↓": (C_REACH, False), "ערך ↓": (C_VALUE, False)}
    scol, sasc = sort_map[sort_by]
    df_s = df_s.sort_values(scol, ascending=sasc, na_position="last")

    st.caption(f"נמצאו **{len(df_s):,}** כתבות")

    # ── טבלת תוצאות ──────────────────────────────────────────────────────────
    def _esc(v):
        return str(v or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")

    rows_html = []
    for _, row in df_s.iterrows():
        title = _esc(row.get(C_TITLE, ""))
        link  = str(row.get(C_LINK, "") or "").strip()
        title_cell = (f'<a href="{_esc(link)}" target="_blank" '
                      f'style="color:#B55BC8;text-decoration:none;">{title}</a>'
                      if link else title)
        rows_html.append(
            f"<tr>"
            f"<td>{_esc(row.get(C_DATE,''))}</td>"
            f"<td>{_esc(row.get(C_SOURCE,''))}</td>"
            f"<td>{_esc(row.get(C_MEDIA,''))}</td>"
            f"<td>{_esc(row.get(C_PUBTYPE,''))}</td>"
            f"<td>{_esc(row.get(C_LANG,''))}</td>"
            f"<td>{_esc(row.get(C_SENT,''))}</td>"
            f"<td style='max-width:180px;word-break:break-word;'>{_esc(row.get(C_CHARS,''))}</td>"
            f"<td style='min-width:280px;max-width:500px;word-break:break-word;'>{title_cell}</td>"
            f"</tr>"
        )
    tbl = (
        "<style>"
        ".srch-tbl{width:100%;border-collapse:collapse;font-size:13px;direction:ltr;}"
        ".srch-tbl th{background:#f0e6f6;color:#5C1070;padding:7px 10px;"
        "text-align:right;border-bottom:2px solid #8B1A9D;position:sticky;top:0;z-index:1;}"
        ".srch-tbl td{padding:6px 10px;border-bottom:1px solid #e2d4ee;"
        "vertical-align:top;color:#1a1a2e;text-align:right;}"
        ".srch-tbl tr:hover td{background:#f9f0ff;}"
        "</style>"
        '<div style="max-height:520px;overflow-y:auto;border:1px solid #d8c4e8;'
        'border-radius:6px;direction:ltr;">'
        '<table class="srch-tbl"><thead><tr>'
        "<th>תאריך</th><th>גוף תקשורת</th><th>מדיה</th><th>סוג פרסום</th>"
        "<th>שפה</th><th>סנטימנט</th><th>דמויות</th><th>כותרת</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table></div>"
    )
    st.markdown(tbl, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — SETTINGS
# ─────────────────────────────────────────────────────────────────────────────
with tab_settings:
    import requests as _req

    st.subheader("⚙️ הגדרות סיכום חדשות בוקרי")
    st.markdown("---")

    # ── קריאת GitHub Token מהקונפיג ───────────────────────────────────────
    _github_token = None
    _github_repo  = "Rozenzwaige/Daily-Slack-Summary"
    try:
        _github_token = st.secrets.get("github_token")
        _github_repo  = st.secrets.get("github_repo", _github_repo)
    except Exception:
        pass
    if not _github_token:
        _cfg = _local_cfg()
        _github_token = _cfg.get("github_token")
        _github_repo  = _cfg.get("github_repo", _github_repo)

    if not _github_token:
        st.warning(
            "לא נמצא GitHub Token.\n\n"
            "הוסף לקובץ `ifat_config.json`:\n"
            '```json\n"github_token": "ghp_...",\n"github_repo": "Rozenzwaige/Daily-Slack-Summary"\n```'
        )
    else:
        _HEADERS = {
            "Authorization": f"Bearer {_github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        _VAR_URL = (
            f"https://api.github.com/repos/{_github_repo}"
            "/actions/variables/SCHEDULE_HOUR_IDT"
        )

        # ── קריאת הערך הנוכחי מ-GitHub ────────────────────────────────────
        _current_hour = 6
        try:
            _r = _req.get(_VAR_URL, headers=_HEADERS, timeout=6)
            if _r.ok:
                _current_hour = int(_r.json()["value"])
        except Exception:
            pass

        # ── UI ─────────────────────────────────────────────────────────────
        st.markdown("**⏰ שעת שליחת הסיכום (שעון ישראל)**")

        _hour_options = list(range(5, 11))          # 05:00 – 10:00
        _hour_labels  = [f"{h:02d}:00" for h in _hour_options]
        _default_idx  = _hour_options.index(_current_hour) if _current_hour in _hour_options else 1

        _selected_label = st.selectbox(
            "שעה",
            options=_hour_labels,
            index=_default_idx,
            label_visibility="collapsed",
        )
        _selected_hour = int(_selected_label.split(":")[0])

        st.caption(f"שעה נוכחית מוגדרת: **{_current_hour:02d}:00**")

        if st.button("💾 שמור שעה", type="primary"):
            try:
                _payload = {"name": "SCHEDULE_HOUR_IDT", "value": str(_selected_hour)}
                _resp = _req.patch(_VAR_URL, json=_payload, headers=_HEADERS, timeout=6)
                if _resp.status_code == 404:
                    # משתנה לא קיים עדיין — יוצרים אותו
                    _create_url = (
                        f"https://api.github.com/repos/{_github_repo}"
                        "/actions/variables"
                    )
                    _resp = _req.post(_create_url, json=_payload, headers=_HEADERS, timeout=6)
                if _resp.ok or _resp.status_code == 204:
                    st.success(f"✅ שעת הסיכום עודכנה ל-{_selected_hour:02d}:00")
                    st.cache_data.clear()
                else:
                    st.error(f"שגיאה מ-GitHub: {_resp.status_code} — {_resp.text}")
            except Exception as _e:
                st.error(f"שגיאת חיבור: {_e}")
