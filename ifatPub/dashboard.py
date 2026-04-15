"""
dashboard.py — דשבורד ניטור מדיה עומדים ביחד
הרצה מקומית:   streamlit run dashboard.py
Streamlit Cloud: הגדר st.secrets (ראה .streamlit/secrets.toml.example)
"""
import os, json, re
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
SENT_COLORS = {"חיובי":"#B55BC8","ניטרלי":"#6B7280","שלילי":"#3D0950"}

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
  html,body,[class*="css"],.stApp{direction:rtl !important;}
  .stTabs [data-baseweb="tab-list"]{flex-direction:row-reverse !important;}
  section[data-testid="stSidebar"] *{direction:rtl !important;text-align:right !important;}
  .stDataFrame th{text-align:right !important;}
  h1,h2,h3,h4,h5,[data-testid="stHeading"],[data-testid="stSubheader"]{text-align:right !important;}
  div[data-testid="metric-container"]>div{text-align:center !important;}
  .block-container{padding-top:1.2rem !important;}
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

    # Resolve column names by position for columns that may have different names
    # K(10)=שפה, L(11)=מדיה, M(12)=סנטימנט, N(13)=סוג פרסום, O(14)=נושא
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
    # Rename columns to our standard names
    rename_map = {v: k for k,v in _remap.items() if v != k and v in df.columns}
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in ALL_COLS:
        if col not in df.columns:
            df[col] = ""

    # Parse date (DD/MM/YYYY primary, fallback dayfirst)
    parsed = pd.to_datetime(df[C_DATE], format="%d/%m/%Y", errors="coerce")
    bad = parsed.isna() & df[C_DATE].str.strip().astype(bool)
    if bad.any():
        parsed[bad] = pd.to_datetime(df.loc[bad,C_DATE], dayfirst=True, errors="coerce")
    df["_date"] = parsed

    df[C_REACH] = pd.to_numeric(df[C_REACH], errors="coerce")
    df[C_VALUE] = pd.to_numeric(df[C_VALUE], errors="coerce")
    df = df[df[C_TITLE].str.strip().astype(bool)|df[C_SOURCE].str.strip().astype(bool)]
    return df.reset_index(drop=True)


# ══ Chart helpers ════════════════════════════════════════════════════════════
def _copy_btn(fig, height=360):
    """Tiny iframe with a hidden off-screen Plotly chart → copy PNG to clipboard."""
    fig_json = fig.to_json()
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  body{{margin:0;padding:0;background:transparent;overflow:hidden;}}
  #h{{position:fixed;left:-9999px;top:0;width:1200px;height:{height}px;}}
  #btn{{background:#8B1A9D;color:#fff;border:none;padding:3px 12px;border-radius:4px;
        cursor:pointer;font-size:12px;font-family:Arial;}}
  #btn:hover{{background:#B55BC8;}}
</style></head><body>
<div id="h"></div>
<button id="btn" onclick="cp()">📋 העתק</button>
<script id="fd" type="application/json">{fig_json}</script>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<script>
var fig=JSON.parse(document.getElementById('fd').textContent);
fig.layout=fig.layout||{{}};
fig.layout.paper_bgcolor='#1e293b';fig.layout.plot_bgcolor='#0f172a';
fig.layout.font={{color:'#f1f5f9'}};
Plotly.newPlot('h',fig.data,fig.layout,{{staticPlot:true}});
async function cp(){{
  var b=document.getElementById('btn');b.textContent='⏳';
  try{{
    var img=await Plotly.toImage('h',{{format:'png',scale:2,width:1200,height:{height}}});
    var res=await fetch(img);var blob=await res.blob();
    if(navigator.clipboard&&window.ClipboardItem){{
      await navigator.clipboard.write([new ClipboardItem({{'image/png':blob}})]);
      b.textContent='✅ הועתק!';
    }}else{{
      var a=document.createElement('a');a.href=img;a.download='chart.png';a.click();
      b.textContent='⬇️ הורד';
    }}
  }}catch(e){{b.textContent='❌';}}
  setTimeout(()=>b.textContent='📋 העתק',2400);
}}
</script></body></html>"""
    components.html(html, height=34, scrolling=False)


def _plot(fig, height=360):
    """Render chart with dark theme via st.plotly_chart + clipboard button."""
    fig.update_layout(
        paper_bgcolor="#1e293b", plot_bgcolor="#0f172a", font_color="#f1f5f9",
        height=height,
    )
    st.plotly_chart(fig, use_container_width=True, config=_CHART_CFG)
    _copy_btn(fig, height)


def _bar_or_pie(values, names, label, key):
    """Toggle bar / pie. Bar = different color per category."""
    chart_type = st.radio("סוג תצוגה", ["עמודות","עוגה"], horizontal=True,
                          key=f"ct_{key}", label_visibility="collapsed")

    # left margin based on longest label (≈8 px per Hebrew char)
    max_lbl = max((len(str(n)) for n in names), default=5)
    lmargin = max(140, min(max_lbl * 9, 280))
    n_items = len(names)

    if chart_type == "עוגה":
        fig = px.pie(values=values, names=names, hole=0.35,
                     color_discrete_sequence=ST_PALETTE)
        fig.update_traces(
            textinfo="percent+label",
            textposition="outside",
            pull=[0.03]*n_items,
        )
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="v", x=1.01, y=0.5, font=dict(size=11)),
            margin=dict(t=30, b=10, l=10, r=160),
        )
        _plot(fig, height=max(360, n_items * 22 + 80))
    else:
        df_tmp = pd.DataFrame({"שם": names, "ספירה": values}).sort_values("ספירה", ascending=True)
        palette_cycle = (ST_PALETTE * ((n_items // len(ST_PALETTE)) + 1))[:n_items]
        fig = px.bar(df_tmp, x="ספירה", y="שם", orientation="h")
        fig.update_traces(marker_color=palette_cycle)
        fig.update_layout(
            showlegend=False,
            xaxis_title="", yaxis_title="",
            margin=dict(t=30, b=50, l=lmargin, r=10),
            yaxis=dict(automargin=True, tickfont=dict(size=12)),
        )
        _plot(fig, height=max(280, n_items * 30 + 60))

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
    # Fix Hebrew direction for Pillow rendering
    words_vis = [get_display(w) for w in words]
    font_candidates = [os.path.join(BASE_DIR,"fonts","hebrew.ttf"),
                       "C:/Windows/Fonts/arial.ttf","C:/Windows/Fonts/ARIALUNI.TTF",
                       "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    font_path = next((p for p in font_candidates if os.path.exists(p)), None)
    wc = WordCloud(width=1200,height=360,background_color="#1e293b",
                   font_path=font_path,prefer_horizontal=1.0,max_words=80,
                   colormap="Purples").generate(" ".join(words_vis))
    fig,ax = plt.subplots(figsize=(12,3.6))
    fig.patch.set_facecolor("#1e293b"); ax.set_facecolor("#1e293b")
    ax.imshow(wc,interpolation="bilinear"); ax.axis("off")
    plt.tight_layout(pad=0)
    return fig


# ══ SIDEBAR ══════════════════════════════════════════════════════════════════
_,SHEET_MAIN,SHEET_PEACE = _sheet_ids()

with st.sidebar:
    st.title("📰 ניטור מדיה")
    st.caption("עומדים ביחד")
    sheet_choice = st.radio("גיליון",[SHEET_MAIN,SHEET_PEACE],index=0)
    df_full = load_sheet(sheet_choice)
    if st.button("🔄 רענן נתונים"):
        st.cache_data.clear(); st.rerun()
    st.divider()
    st.subheader("סינונים")
    def _opts(col):
        return sorted(df_full[col].replace("",pd.NA).dropna().unique().tolist()) if col in df_full.columns else []
    filt_lang   = st.multiselect("שפה",        _opts(C_LANG))
    filt_media  = st.multiselect("מדיה",       _opts(C_MEDIA))
    filt_sent   = st.multiselect("סנטימנט",    _opts(C_SENT))
    filt_sector = st.multiselect("מגזר",       _opts(C_SECTOR))
    filt_source = st.multiselect("גוף תקשורת",_opts(C_SOURCE))


# ══ Date range ════════════════════════════════════════════════════════════════
valid_dates = df_full["_date"].dropna()
min_d = valid_dates.min().date() if not valid_dates.empty else date.today()-timedelta(days=90)
max_d = valid_dates.max().date() if not valid_dates.empty else date.today()

dr1,dr2,_ = st.columns([1,1,2])
with dr1: d_from = st.date_input("מתאריך",   value=min_d,min_value=min_d,max_value=max_d,format="DD/MM/YYYY")
with dr2: d_to   = st.date_input("עד תאריך", value=max_d,min_value=min_d,max_value=max_d,format="DD/MM/YYYY")


# ══ Apply filters ═════════════════════════════════════════════════════════════
df = df_full[(df_full["_date"].dt.date>=d_from)&(df_full["_date"].dt.date<=d_to)].copy()
if filt_lang:   df=df[df[C_LANG].isin(filt_lang)]
if filt_media:  df=df[df[C_MEDIA].isin(filt_media)]
if filt_sent:   df=df[df[C_SENT].isin(filt_sent)]
if filt_sector: df=df[df[C_SECTOR].isin(filt_sector)]
if filt_source: df=df[df[C_SOURCE].isin(filt_source)]


# ══ KPI row ═══════════════════════════════════════════════════════════════════
days = max((pd.Timestamp(d_to)-pd.Timestamp(d_from)).days,1)
k1,k2,k3,k4 = st.columns(4)
k1.metric("סה״כ פרסומים", f"{len(df):,}")
k2.metric("חשיפה כוללת",  f"{int(df[C_REACH].sum()):,}" if df[C_REACH].sum()>0 else "—")
k3.metric("ערך כולל (₪)", f"{int(df[C_VALUE].sum()):,}" if df[C_VALUE].sum()>0 else "—")
k4.metric("ממוצע יומי",   f"{len(df)/days:.1f}")
st.divider()


# ══ TABS ══════════════════════════════════════════════════════════════════════
tab_charts,tab_search = st.tabs(["📊  גרפים","🔍  חיפוש וגלישה"])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — CHARTS
# ─────────────────────────────────────────────────────────────────────────────
with tab_charts:
    if df.empty:
        st.info("אין נתונים לטווח התאריכים הנבחר.")
    else:
        st.caption("💡 ריחוף מעל גרף → סמל מצלמה → הורדת PNG")

        # ── Row 1: timeline ──────────────────────────────────────────────
        st.subheader("פרסומים לפי יום")
        show_trend = st.checkbox("הצג קו מגמה (ממוצע נע 7 ימים)")
        tl = df.groupby("_date").size().reset_index(name="ספירה")
        fig_tl = px.bar(tl, x="_date", y="ספירה",
                        labels={"_date":"תאריך","ספירה":"פרסומים"},
                        color_discrete_sequence=[ST_PRIMARY])
        if show_trend and len(tl)>=7:
            tl["מגמה"] = tl["ספירה"].rolling(7,min_periods=1).mean()
            fig_tl.add_trace(go.Scatter(x=tl["_date"],y=tl["מגמה"],mode="lines",
                                        name="ממוצע נע",
                                        line=dict(color="#E8C8EE",width=2,dash="dot")))
        fig_tl.update_xaxes(tickformat="%d/%m/%Y")
        fig_tl.update_layout(margin=dict(t=30,b=60,l=60,r=10))
        _plot(fig_tl, height=340)

        # ── Row 2: media + language ──────────────────────────────────────
        c1,c2 = st.columns(2)
        with c1:
            st.subheader("פילוג מדיה")
            mc = df[C_MEDIA].replace("",pd.NA).dropna().value_counts()
            _bar_or_pie(mc.values, mc.index.tolist(), C_MEDIA, "media")
        with c2:
            st.subheader("שפת פרסום")
            lc = df[C_LANG].replace("",pd.NA).dropna().value_counts()
            _bar_or_pie(lc.values, lc.index.tolist(), C_LANG, "lang")

        # ── Row 3: sentiment + sector ────────────────────────────────────
        c3,c4 = st.columns(2)
        with c3:
            st.subheader("סנטימנט")
            sc = df[C_SENT].replace("",pd.NA).dropna().value_counts()
            chart_type_sent = st.radio("סוג תצוגה",["עמודות","עוגה"],horizontal=True,
                                       key="ct_sent",label_visibility="collapsed")
            if chart_type_sent=="עוגה":
                fig=px.pie(values=sc.values,names=sc.index,hole=0.35,
                           color=sc.index,color_discrete_map=SENT_COLORS)
                fig.update_traces(textinfo="percent+label",textposition="outside",pull=[0.03]*len(sc))
                fig.update_layout(showlegend=True,legend=dict(orientation="v",x=1.01,y=0.5,font=dict(size=11)),
                                  margin=dict(t=30,b=10,l=10,r=160))
                _plot(fig, height=320)
            else:
                sc_df=sc.reset_index(); sc_df.columns=["סנטימנט","ספירה"]
                fig=px.bar(sc_df,x="סנטימנט",y="ספירה",color="סנטימנט",
                           color_discrete_map=SENT_COLORS)
                fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="",
                                  margin=dict(t=30,b=60,l=60,r=10))
                _plot(fig, height=320)
        with c4:
            st.subheader("פילוג מגזרים")
            sec=df[C_SECTOR].replace("",pd.NA).dropna().value_counts()
            _bar_or_pie(sec.values,sec.index.tolist(),C_SECTOR,"sector")

        # ── Row 4: pub type + topic ──────────────────────────────────────
        c5,c6 = st.columns(2)
        with c5:
            st.subheader("סוג פרסום")  # column N
            pt_raw = df[C_PUBTYPE].replace("",pd.NA).dropna()
            pt_exploded = pt_raw.str.split(",").explode().str.strip().replace("",pd.NA).dropna()
            pt_cnt = pt_exploded.value_counts()
            if not pt_cnt.empty:
                _bar_or_pie(pt_cnt.values,pt_cnt.index.tolist(),C_PUBTYPE,"pubtype")
            else:
                st.info("אין נתונים בעמודה זו")
        with c6:
            st.subheader("נושא / קמפיין")  # column O
            tp=df[C_TOPIC].replace("",pd.NA).dropna()
            # multi-value cells (comma-separated) — explode them
            tp_exploded = tp.str.split(",").explode().str.strip().replace("",pd.NA).dropna()
            tp_cnt=tp_exploded.value_counts()
            if not tp_cnt.empty:
                _bar_or_pie(tp_cnt.values,tp_cnt.index.tolist(),C_TOPIC,"topic")
            else:
                st.info("אין נתונים בעמודה זו")

        # ── Row 5: top sources ───────────────────────────────────────────
        st.subheader("גופי תקשורת מובילים")
        n_sources = st.slider("מספר גופים להצגה",5,30,15,key="n_src")
        src = df[C_SOURCE].replace("",pd.NA).dropna().value_counts().head(n_sources).reset_index()
        src.columns=["גוף","ספירה"]
        chart_type_src = st.radio("סוג תצוגה",["עמודות","עוגה"],horizontal=True,
                                   key="ct_src",label_visibility="collapsed")
        if chart_type_src=="עוגה":
            fig=px.pie(values=src["ספירה"],names=src["גוף"],hole=0.3,color_discrete_sequence=ST_PALETTE)
            fig.update_traces(textinfo="percent+label",textposition="outside",pull=[0.03]*len(src))
            fig.update_layout(showlegend=True,legend=dict(orientation="v",x=1.01,y=0.5,font=dict(size=11)),
                              margin=dict(t=30,b=10,l=10,r=160))
            _plot(fig, height=max(420, n_sources*22+80))
        else:
            palette_src=(ST_PALETTE*((n_sources//len(ST_PALETTE))+1))[:n_sources]
            fig=px.bar(src,x="ספירה",y="גוף",orientation="h")
            fig.update_traces(marker_color=palette_src)
            max_src_lbl=max((len(str(n)) for n in src["גוף"].tolist()),default=5)
            lmargin_src=max(140,min(max_src_lbl*9,280))
            fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="",
                              margin=dict(t=30,b=50,l=lmargin_src,r=10),
                              yaxis=dict(automargin=True,tickfont=dict(size=12),categoryorder="total ascending"))
            _plot(fig, height=max(320,n_sources*30+60))

        # ── Row 6: word frequency ────────────────────────────────────────
        st.subheader("מילים נפוצות")
        w1,w2 = st.columns(2)
        with w1:
            st.caption("בכותרות")
            wdf_t=_top_words(df[C_TITLE])
            if not wdf_t.empty:
                n_wt=len(wdf_t)
                palette_wt=(ST_PALETTE*((n_wt//len(ST_PALETTE))+1))[:n_wt]
                fig=px.bar(wdf_t,x="ספירה",y="מילה",orientation="h")
                fig.update_traces(marker_color=palette_wt)
                max_wt_lbl=max((len(str(w)) for w in wdf_t["מילה"].tolist()),default=4)
                fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="",
                                  margin=dict(t=30,b=40,l=max(80,min(max_wt_lbl*9,200)),r=10),
                                  yaxis=dict(automargin=True,tickfont=dict(size=12),categoryorder="total ascending"))
                _plot(fig, height=max(400,n_wt*22+60))
        with w2:
            st.caption("בתוכן")
            wdf_c=_top_words(df[C_CONTENT])
            if not wdf_c.empty:
                n_wc=len(wdf_c)
                palette_wc=(ST_PALETTE*((n_wc//len(ST_PALETTE))+1))[:n_wc]
                fig=px.bar(wdf_c,x="ספירה",y="מילה",orientation="h")
                fig.update_traces(marker_color=palette_wc)
                max_wc_lbl=max((len(str(w)) for w in wdf_c["מילה"].tolist()),default=4)
                fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="",
                                  margin=dict(t=30,b=40,l=max(80,min(max_wc_lbl*9,200)),r=10),
                                  yaxis=dict(automargin=True,tickfont=dict(size=12),categoryorder="total ascending"))
                _plot(fig, height=max(400,n_wc*22+60))

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
    sc1,sc2 = st.columns([3,1])
    with sc1:
        query = st.text_input("🔍 חיפוש חופשי — כותרת / תוכן / גוף תקשורת",
                              placeholder="לדוגמה: עומד, שלום, גרין...")
    with sc2:
        sort_by = st.selectbox("מיין לפי",["תאריך (חדש→ישן)","תאריך (ישן→חדש)","חשיפה ↓","ערך ↓"])

    df_s = df.copy()
    if query.strip():
        pat  = re.escape(query.strip())
        mask = (df_s[C_TITLE].str.contains(pat,case=False,na=False)|
                df_s[C_CONTENT].str.contains(pat,case=False,na=False)|
                df_s[C_SOURCE].str.contains(pat,case=False,na=False))
        df_s = df_s[mask]

    sort_map = {"תאריך (חדש→ישן)":("_date",False),"תאריך (ישן→חדש)":("_date",True),
                "חשיפה ↓":(C_REACH,False),"ערך ↓":(C_VALUE,False)}
    scol,sasc = sort_map[sort_by]
    df_s = df_s.sort_values(scol,ascending=sasc,na_position="last")

    st.caption(f"נמצאו **{len(df_s):,}** כתבות")

    # HTML table: title = blue hyperlink (no separate "פתח" column)
    def _esc(v):
        return str(v or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")

    rows_html = []
    for _, row in df_s.iterrows():
        title = _esc(row.get(C_TITLE,""))
        link  = str(row.get(C_LINK,"") or "").strip()
        title_cell = (f'<a href="{_esc(link)}" target="_blank" '
                      f'style="color:#B55BC8;text-decoration:none;">{title}</a>'
                      if link else title)
        rows_html.append(
            f"<tr>"
            f"<td>{_esc(row.get(C_DATE,''))}</td>"
            f"<td>{_esc(row.get(C_SOURCE,''))}</td>"
            f"<td>{_esc(row.get(C_MEDIA,''))}</td>"
            f"<td>{_esc(row.get(C_LANG,''))}</td>"
            f"<td>{_esc(row.get(C_SENT,''))}</td>"
            f"<td style='min-width:300px;max-width:560px;word-break:break-word;'>{title_cell}</td>"
            f"</tr>"
        )
    tbl = (
        "<style>"
        ".srch-tbl{width:100%;border-collapse:collapse;font-size:13px;direction:ltr;}"
        ".srch-tbl th{background:#1e293b;color:#D4A0DC;padding:7px 10px;"
        "text-align:right;border-bottom:2px solid #8B1A9D;position:sticky;top:0;z-index:1;}"
        ".srch-tbl td{padding:6px 10px;border-bottom:1px solid #2d3748;"
        "vertical-align:top;color:#f1f5f9;text-align:right;}"
        ".srch-tbl tr:hover td{background:#243247;}"
        "</style>"
        '<div style="max-height:500px;overflow-y:auto;border:1px solid #2d3748;border-radius:6px;direction:ltr;">'
        '<table class="srch-tbl">'
        "<thead><tr>"
        "<th>תאריך</th><th>גוף תקשורת</th><th>מדיה</th><th>שפה</th><th>סנטימנט</th><th>כותרת</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table></div>"
    )
    st.markdown(tbl, unsafe_allow_html=True)
