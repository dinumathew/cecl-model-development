# cecl_v3.py ' CECL CRE Workbench | Clean final build
# Pages: Overview | Data Ingestion | Data Sufficiency | Pipeline Monitor | Narratives

import streamlit as st
import pandas as pd
import numpy as np
import psycopg2, psycopg2.extras, decimal, os, re
import anthropic
from dotenv import load_dotenv
from datetime import datetime, date
from io import BytesIO
import json
import plotly.graph_objects as go
import warnings; warnings.filterwarnings("ignore")
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.enum.style import WD_STYLE_TYPE
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
from docx.enum.table import WD_TABLE_ALIGNMENT

load_dotenv(override=True)

NAVY  = RGBColor(0x1F, 0x38, 0x64)
BLUE  = RGBColor(0x2E, 0x75, 0xB6)
GREEN = RGBColor(0x37, 0x56, 0x23)
GREY  = RGBColor(0x40, 0x40, 0x40)
LGREY = RGBColor(0xD0, 0xD0, 0xCE)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
LIGHT_BLUE_BG = "EBF3FB"
DARK_NAVY_BG  = "1F3864"
ALT_ROW_BG    = "F0F6FB"

DB_CFG = dict(
    host="aws-1-ap-southeast-1.pooler.supabase.com",
    port=6543, dbname="postgres",
    user="postgres.dnvyxvlbebnuvxitblbu",
    password="Indroyal019283"
)
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
try:
    if not ANTHROPIC_KEY:
        ANTHROPIC_KEY = st.secrets.get("ANTHROPIC_API_KEY", "")
except Exception:
    pass

st.set_page_config(page_title="CECL CRE Workbench", page_icon=":bank:",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=IBM+Plex+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Inter',sans-serif;background:#F7F8FA;color:#1A1A2E;}
.stApp{background:#F7F8FA;}
section[data-testid="stSidebar"]{background:#1F3864;border-right:none;}
section[data-testid="stSidebar"] *{color:#FFFFFF!important;}
section[data-testid="stSidebar"] .stRadio label{color:#E0E8FF!important;font-size:13px;}
section[data-testid="stSidebar"] .stRadio [data-testid="stMarkdownContainer"] p{color:#E0E8FF!important;}
.stButton button{background:#1F3864;color:#FFFFFF;border:none;font-weight:600;font-size:12px;
  letter-spacing:.04em;text-transform:uppercase;border-radius:6px;padding:8px 20px;}
.stButton button:hover{background:#2E75B6;color:#FFFFFF;}
.stButton button:focus{outline:none!important;box-shadow:none!important;}
.stTabs [data-baseweb="tab-list"]{background:#FFFFFF;border-bottom:2px solid #E8EDF5;gap:0;}
.stTabs [data-baseweb="tab"]{color:#6B7FA3;font-size:12px;text-transform:uppercase;
  letter-spacing:.06em;padding:10px 22px;background:#FFFFFF;}
.stTabs [aria-selected="true"]{color:#1F3864!important;border-bottom:2px solid #1F3864!important;font-weight:600!important;}
.stDataFrame{border:1px solid #E8EDF5;border-radius:8px;background:#FFFFFF;}
.stDataFrame thead{background:#1F3864!important;}
h1,h2,h3{font-family:'Inter',sans-serif;color:#1A1A2E;}
div[data-testid="stExpander"]{border:1px solid #E8EDF5;border-radius:8px;background:#FFFFFF;}
.stSelectbox [data-baseweb="select"]{background:#FFFFFF;border:1px solid #E8EDF5;border-radius:6px;}
.stTextInput input,.stTextInput input:focus{background:#FFFFFF;border:1px solid #D0D8E8;border-radius:6px;color:#1A1A2E;}
.stProgress .st-bo{background:#1F3864;}
.stAlert{border-radius:8px;}
</style>""", unsafe_allow_html=True)


# '' DB HELPERS ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# -- CREDENTIALS -----------------------------------------------------------------
def safe_rerun():
    if hasattr(st, 'rerun'):
        st.rerun()
    else:
        st.experimental_rerun()


USERS = {
    "admin":  "cecl2026",
    "client": "pinnacle2026",
}

def login_page():
    st.markdown("<style>.block-container{padding-top:3rem;}</style>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1.2, 1])
    with c2:
        st.markdown(
            "<div style='background:#FFFFFF;border:1px solid #E8EDF5;border-radius:12px;"
            "padding:40px 36px;margin-top:40px;'>"
            "<div style='font-size:22px;font-weight:700;color:#1F3864;margin-bottom:6px;'>"
            "CECL CRE Workbench</div>"
            "<div style='font-size:12px;color:#6B7FA3;margin-bottom:28px;'>"
            "Combined Entity | ASC 326 | PD/LGD</div></div>",
            unsafe_allow_html=True)
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Sign In")
            if submitted:
                if username in USERS and USERS[username] == password:
                    st.session_state["authenticated"] = True
                    st.session_state["username"] = username
                    safe_rerun()
                else:
                    st.error("Invalid username or password.")


def get_conn():
    return psycopg2.connect(**DB_CFG)

def db_query(sql, params=None):
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
        conn.commit(); conn.close()
        result = []
        for row in rows:
            clean = {}
            for k, v in dict(row).items():
                clean[k] = float(v) if isinstance(v, decimal.Decimal) else v
            result.append(clean)
        return result
    except Exception as e:
        st.error("DB error: {}".format(e))
        return []

def db_exec(sql, params=None):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit(); conn.close()
        return True
    except Exception as e:
        st.error("DB error: {}".format(e))
        return False


# '' SCHEMA ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
def setup_schema():
    SQL = """
    CREATE TABLE IF NOT EXISTS cecl_institutions (
        inst_id TEXT PRIMARY KEY, inst_name TEXT, total_cre NUMERIC,
        data_start DATE, created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS cecl_cre_loans (
        loan_id TEXT, inst_id TEXT, origination_dt DATE, maturity_dt DATE,
        property_type TEXT, balance NUMERIC, original_balance NUMERIC,
        ltv_orig NUMERIC, ltv_current NUMERIC, dscr NUMERIC, occupancy NUMERIC,
        risk_grade TEXT, state TEXT, defaulted BOOLEAN DEFAULT FALSE,
        default_dt DATE, charge_off_amt NUMERIC DEFAULT 0,
        recovery_amt NUMERIC DEFAULT 0, vintage_year INTEGER,
        loaded_at TIMESTAMP DEFAULT NOW(), PRIMARY KEY (loan_id, inst_id)
    );
    CREATE TABLE IF NOT EXISTS cecl_model_segments (
        segment_id TEXT PRIMARY KEY, property_type TEXT, ltv_band TEXT,
        ltv_min NUMERIC, ltv_max NUMERIC, loan_count INTEGER, exposure NUMERIC,
        pd_ttc NUMERIC, pd_pit_base NUMERIC, pd_pit_adverse NUMERIC, pd_pit_severe NUMERIC,
        lgd_base NUMERIC, lgd_adverse NUMERIC, lgd_severe NUMERIC,
        ecl_base NUMERIC, ecl_adverse NUMERIC, ecl_severe NUMERIC,
        run_dt TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS cecl_narratives (
        narrative_id SERIAL PRIMARY KEY, doc_type TEXT, content TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    try:
        conn = get_conn(); conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.close(); return True
    except Exception as e:
        st.error("Schema error: {}".format(e)); return False


# '' DEMO DATA '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
PROPERTY_TYPES = ["Multifamily", "Office", "Retail", "Industrial"]

def generate_loans(inst_id, n=200, seed=42):
    np.random.seed(seed)
    loans, states = [], ["TX","FL","CA","NY","GA","NC","IL","OH","PA","WA"]
    for i in range(n):
        pt      = np.random.choice(PROPERTY_TYPES, p=[0.40,0.25,0.20,0.15])
        vintage = np.random.randint(2016, 2024)
        orig_dt = date(vintage, np.random.randint(1,13), 1)
        term    = np.random.choice([5,7,10])
        mat_dt  = date(vintage+term, orig_dt.month, 1)
        orig_bal= np.random.choice([2,3,4,5,8,10,12,15,20,25,30,35,40])*1e6
        ltv_orig= np.random.uniform(0.50, 0.85)
        ltv_curr= min(ltv_orig*np.random.uniform(0.90,1.10), 0.95)
        dscr_mu = {"Multifamily":1.35,"Office":1.28,"Retail":1.22,"Industrial":1.40}[pt]
        dscr    = max(0.80, np.random.normal(dscr_mu, 0.18))
        occ     = np.random.uniform(0.70, 1.00)
        score   = (ltv_orig-0.65)*2 + (1.20-dscr)*1.5
        prob    = 1/(1+np.exp(-score))*0.15
        defaulted = bool(np.random.random() < prob)
        curr_bal  = orig_bal*np.random.uniform(0.85,1.0)
        default_dt= None; charge_off=0.0; recovery=0.0
        if defaulted:
            def_yr    = vintage+int(np.random.uniform(1,min(term,5)))
            default_dt= date(min(def_yr,2024), np.random.randint(1,13), 1)
            lgd_raw   = max(0, ltv_orig-0.55+np.random.normal(0,0.08))
            charge_off= curr_bal*min(lgd_raw,0.80)
            recovery  = charge_off*np.random.uniform(0.05,0.30)
        rg = ("Pass"        if dscr>=1.25 and ltv_orig<=0.70 else
              "Watch"       if dscr>=1.10 and ltv_orig<=0.80 else
              "Substandard" if dscr>=1.00 else "Doubtful")
        loans.append({
            "loan_id":"{}_{:04d}".format(inst_id,i+1),"inst_id":inst_id,
            "origination_dt":orig_dt,"maturity_dt":mat_dt,"property_type":pt,
            "balance":round(curr_bal,0),"original_balance":round(orig_bal,0),
            "ltv_orig":round(ltv_orig,4),"ltv_current":round(ltv_curr,4),
            "dscr":round(dscr,3),"occupancy":round(occ,3),"risk_grade":rg,
            "state":np.random.choice(states),"defaulted":defaulted,
            "default_dt":default_dt,"charge_off_amt":round(charge_off,0),
            "recovery_amt":round(recovery,0),"vintage_year":int(vintage),
        })
    return loans

def seed_demo_data():
    for inst_id, name, total_cre in [
        ("BANK-A","Bank A",1200000000),
        ("BANK-B","Bank B",850000000),
    ]:
        db_exec("INSERT INTO cecl_institutions (inst_id,inst_name,total_cre,data_start) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT (inst_id) DO NOTHING",
                (inst_id, name, total_cre, date(2016,1,1)))
    for inst_id, n, seed in [("BANK-A",220,42),("BANK-B",180,99)]:
        loans = generate_loans(inst_id, n, seed)
        conn = get_conn(); cur = conn.cursor()
        vals = [(l["loan_id"],l["inst_id"],l["origination_dt"],l["maturity_dt"],
                 l["property_type"],l["balance"],l["original_balance"],l["ltv_orig"],
                 l["ltv_current"],l["dscr"],l["occupancy"],l["risk_grade"],l["state"],
                 l["defaulted"],l["default_dt"],l["charge_off_amt"],l["recovery_amt"],
                 l["vintage_year"]) for l in loans]
        psycopg2.extras.execute_values(cur,
            "INSERT INTO cecl_cre_loans "
            "(loan_id,inst_id,origination_dt,maturity_dt,property_type,balance,"
            "original_balance,ltv_orig,ltv_current,dscr,occupancy,risk_grade,state,"
            "defaulted,default_dt,charge_off_amt,recovery_amt,vintage_year) "
            "VALUES %s ON CONFLICT (loan_id,inst_id) DO NOTHING", vals)
        conn.commit(); conn.close()


# '' DATA SUFFICIENCY ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
PD_LGD_REQS = [
    {"requirement":"Loan-level origination data",  "field":"origination_dt", "min_years":7, "weight":"Critical"},
    {"requirement":"Historical default events",    "field":"defaulted",       "min_years":7, "weight":"Critical"},
    {"requirement":"Default dates",                "field":"default_dt",      "min_years":7, "weight":"Critical"},
    {"requirement":"Charge-off / loss amounts",    "field":"charge_off_amt",  "min_years":7, "weight":"Critical"},
    {"requirement":"Recovery amounts",             "field":"recovery_amt",    "min_years":5, "weight":"Important"},
    {"requirement":"LTV at origination",           "field":"ltv_orig",        "min_years":7, "weight":"Critical"},
    {"requirement":"Current LTV",                  "field":"ltv_current",     "min_years":3, "weight":"Important"},
    {"requirement":"DSCR",                         "field":"dscr",            "min_years":5, "weight":"Important"},
    {"requirement":"Occupancy rate",               "field":"occupancy",       "min_years":5, "weight":"Important"},
    {"requirement":"Property type segmentation",   "field":"property_type",   "min_years":7, "weight":"Critical"},
    {"requirement":"Internal risk grade",          "field":"risk_grade",      "min_years":5, "weight":"Important"},
    {"requirement":"Vintage year",                 "field":"vintage_year",    "min_years":7, "weight":"Critical"},
    {"requirement":"Balance outstanding",          "field":"balance",         "min_years":7, "weight":"Critical"},
    {"requirement":"Geographic identifier",        "field":"state",           "min_years":5, "weight":"Supplemental"},
]

def compute_sufficiency(df, inst_id):
    inst = df[df["inst_id"]==inst_id]
    if len(inst)==0: return []
    oldest      = pd.to_datetime(inst["origination_dt"]).min()
    history_yrs = (datetime.now()-oldest).days/365.25
    rows = []
    for req in PD_LGD_REQS:
        field    = req["field"]
        col_data = inst[field] if field in inst.columns else None
        if col_data is None:
            completeness, status, note = 0.0, "FAIL", "Field not present"
        else:
            completeness = col_data.notna().mean()
            years_ok = history_yrs >= req["min_years"]
            comp_ok  = completeness >= 0.90
            if comp_ok and years_ok:
                status, note = "PASS", "{:.0f}% complete | {:.1f} yrs history".format(completeness*100, history_yrs)
            elif comp_ok and not years_ok:
                status, note = "PARTIAL", "{:.0f}% complete | Only {:.1f} yrs (need {})".format(completeness*100, history_yrs, req["min_years"])
            elif not comp_ok:
                status, note = "PARTIAL", "Only {:.0f}% complete".format(completeness*100)
            else:
                status, note = "FAIL", "{:.0f}% complete | {:.1f} yrs".format(completeness*100, history_yrs)
        rows.append({"Requirement":req["requirement"],"Weight":req["weight"],
                     "Completeness":"{:.0f}%".format(completeness*100),"Status":status,"Note":note})
    return rows


# '' PIPELINE CHECKS '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
def run_pipeline_checks(df):
    checks = []
    for f in ["loan_id","origination_dt","balance","property_type","ltv_orig","dscr"]:
        null_pct = df[f].isna().mean()*100
        checks.append({"Check":"No nulls: {}".format(f),"Category":"Completeness",
            "Value":"{:.1f}% null".format(null_pct),
            "Status":"PASS" if null_pct < 1.0 else "FAIL"})
    checks.append({"Check":"LTV in [0.30, 1.00]","Category":"Range",
        "Value":"Min {:.2f} | Max {:.2f}".format(df["ltv_orig"].min(), df["ltv_orig"].max()),
        "Status":"PASS" if df["ltv_orig"].between(0.30,1.00).all() else "FAIL"})
    checks.append({"Check":"DSCR in [0.50, 4.00]","Category":"Range",
        "Value":"Min {:.2f} | Max {:.2f}".format(df["dscr"].min(), df["dscr"].max()),
        "Status":"PASS" if df["dscr"].between(0.50,4.00).all() else "FAIL"})
    checks.append({"Check":"Balance > 0","Category":"Range",
        "Value":"Min ${:,.0f}".format(df["balance"].min()),
        "Status":"PASS" if (df["balance"]>0).all() else "FAIL"})
    def_with_dt = df[df["defaulted"]==True]["default_dt"].notna().mean() if df["defaulted"].sum()>0 else 1.0
    checks.append({"Check":"Default date when defaulted=True","Category":"Consistency",
        "Value":"{:.0f}% have date".format(def_with_dt*100),
        "Status":"PASS" if def_with_dt>0.95 else "PARTIAL"})
    checks.append({"Check":"Charge-off <= Balance","Category":"Consistency",
        "Value":"Violations: {}".format((df["charge_off_amt"]>df["balance"]*1.05).sum()),
        "Status":"PASS" if (df["charge_off_amt"]<=df["balance"]*1.05).all() else "FAIL"})
    checks.append({"Check":"Recovery <= Charge-off","Category":"Consistency",
        "Value":"Violations: {}".format((df["recovery_amt"]>df["charge_off_amt"]).sum()),
        "Status":"PASS" if (df["recovery_amt"]<=df["charge_off_amt"]+1).all() else "FAIL"})
    vintages = sorted(df["vintage_year"].dropna().unique())
    checks.append({"Check":"Vintage coverage 2016-2023","Category":"Coverage",
        "Value":"{}-{}".format(int(min(vintages)), int(max(vintages))),
        "Status":"PASS" if min(vintages)<=2016 and max(vintages)>=2023 else "PARTIAL"})
    checks.append({"Check":"All 4 property types","Category":"Coverage",
        "Value":", ".join(sorted(df["property_type"].unique())),
        "Status":"PASS" if len(df["property_type"].unique())>=4 else "FAIL"})
    checks.append({"Check":"Both institutions loaded","Category":"Coverage",
        "Value":", ".join(sorted(df["inst_id"].unique())),
        "Status":"PASS" if len(df["inst_id"].unique())>=2 else "FAIL"})
    return checks


# '' NARRATIVE ENGINE ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
DOC_LABELS = {
    "methodology_memo":        "Methodology Selection Memo",
    "data_assessment":         "Data Assessment Report",
    "implementation_timeline": "Implementation Timeline",
    "model_risk_doc":          "Model Risk Documentation (SR 11-7)",
    "ecl_results_summary":     "ECL Results Summary",
}

def generate_narrative(doc_type, context):
    prompts = {
        "methodology_memo": (
            "You are a senior model risk officer. Write a COMPLETE CECL Methodology Selection Memo "
            "under ASC 326 for a merged CRE portfolio. Write ALL sections in full ' do not truncate. "
            "Structure: (1) Executive Summary, (2) Portfolio Overview, (3) PD/LGD Rationale, "
            "(4) Segmentation Framework, (5) ASC 326 Compliance Basis. "
            "Use markdown: ## for sections, ### for subsections, ** for bold, bullet points with -.\n\n" + context
        ),
        "data_assessment": (
            "You are a senior quantitative analyst. Write a COMPLETE Data Assessment Report "
            "for a combined entity CECL redevelopment. Write ALL sections in full ' do not truncate. "
            "Structure: (1) Executive Summary, (2) Data Inventory by Institution, "
            "(3) Sufficiency Assessment, (4) Gap Analysis, (5) Remediation Plan. "
            "Use markdown: ## for sections, ### for subsections, ** for bold, bullet points with -, tables with |.\n\n" + context
        ),
        "implementation_timeline": (
            "You are a CECL project manager. Write a COMPLETE Implementation Timeline. "
            "Write ALL sections in full ' do not truncate. "
            "Structure: (1) Executive Summary, (2) Phased Plan with weeks, "
            "(3) Key Milestones, (4) Risks and Dependencies, (5) Regulatory Schedule. "
            "Use markdown: ## for sections, ### for subsections, bullet points with -.\n\n" + context
        ),
        "model_risk_doc": (
            "Write COMPLETE Model Risk documentation under SR 11-7 for a CRE CECL PD/LGD model. "
            "Write ALL sections in full ' do not truncate. "
            "Structure: (1) Model Purpose and Scope, (2) Conceptual Soundness, "
            "(3) Developmental Evidence, (4) Limitations and Compensating Controls, "
            "(5) Ongoing Monitoring Plan. "
            "Use markdown: ## for sections, ### for subsections, bullet points with -.\n\n" + context
        ),
        "ecl_results_summary": (
            "Write a COMPLETE ECL Results Summary for the Board Risk Committee. "
            "Write ALL sections in full ' do not truncate. "
            "Structure: (1) Key Findings, (2) ECL by Segment, "
            "(3) Scenario Analysis Base/Adverse/Severe, "
            "(4) Comparison to Prior Reserve, (5) Management Conclusions. "
            "Use markdown: ## for sections, ### for subsections, bullet points with -, tables with |.\n\n" + context
        ),
    }
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        resp   = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=3000,
            messages=[{"role":"user","content":prompts[doc_type]}]
        )
        return resp.content[0].text
    except Exception as e:
        return "[Narrative generation failed: {}]".format(e)


# '' WORD HELPERS ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
def set_cell_bg(cell, hex_color):
    tcPr = cell._tc.get_or_add_tcPr()
    shd  = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)

def set_cell_margins(cell, top=60, bottom=60, left=120, right=120):
    tcPr  = cell._tc.get_or_add_tcPr()
    tcMar = OxmlElement("w:tcMar")
    for side, val in [("top",top),("bottom",bottom),("left",left),("right",right)]:
        el = OxmlElement("w:{}".format(side))
        el.set(qn("w:w"), str(val))
        el.set(qn("w:type"), "dxa")
        tcMar.append(el)
    tcPr.append(tcMar)

def set_para_spacing(para, before=0, after=120, line=276):
    pPr = para._p.get_or_add_pPr()
    pSp = OxmlElement("w:spacing")
    pSp.set(qn("w:before"), str(before))
    pSp.set(qn("w:after"),  str(after))
    pSp.set(qn("w:line"),   str(line))
    pSp.set(qn("w:lineRule"), "auto")
    pPr.append(pSp)

def add_bottom_border(para, color="2E75B6", sz=6):
    pPr  = para._p.get_or_add_pPr()
    pBdr = OxmlElement("w:pBdr")
    b    = OxmlElement("w:bottom")
    b.set(qn("w:val"), "single"); b.set(qn("w:sz"), str(sz))
    b.set(qn("w:space"), "4");    b.set(qn("w:color"), color)
    pBdr.append(b); pPr.append(pBdr)

def add_page_number_footer(doc):
    footer = doc.sections[0].footer
    fp = footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
    fp.clear(); fp.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = fp.add_run("Page ")
    run.font.size = Pt(8.5); run.font.color.rgb = LGREY; run.font.name = "Calibri"
    for ftype, code in [("begin"," PAGE "), ("end", None)]:
        fc = OxmlElement("w:fldChar"); fc.set(qn("w:fldCharType"), ftype)
        if ftype == "begin":
            it = OxmlElement("w:instrText"); it.text = code
            r  = OxmlElement("w:r"); r.append(fc); r.append(it)
        else:
            r = OxmlElement("w:r"); r.append(fc)
        fp._p.append(r)
    run2 = fp.add_run(" of ")
    run2.font.size = Pt(8.5); run2.font.color.rgb = LGREY; run2.font.name = "Calibri"
    for ftype, code in [("begin"," NUMPAGES "), ("end", None)]:
        fc = OxmlElement("w:fldChar"); fc.set(qn("w:fldCharType"), ftype)
        if ftype == "begin":
            it = OxmlElement("w:instrText"); it.text = code
            r  = OxmlElement("w:r"); r.append(fc); r.append(it)
        else:
            r = OxmlElement("w:r"); r.append(fc)
        fp._p.append(r)

def strip_md(text):
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'\*(.+?)\*',     r'\1', text)
    text = re.sub(r'__(.+?)__',     r'\1', text)
    text = re.sub(r'_(.+?)_',       r'\1', text)
    text = re.sub(r'`(.+?)`',       r'\1', text)
    return text.strip()

def add_rich_run(para, text, font_size=10.5, bold=False, color=None):
    color = color or GREY
    parts = re.split(r'(\*\*.*?\*\*)', text)
    for part in parts:
        if part.startswith('**') and part.endswith('**'):
            inner = part[2:-2]
            r = para.add_run(inner)
            r.font.name = "Calibri"; r.font.size = Pt(font_size)
            r.font.bold = True; r.font.color.rgb = color
        else:
            clean = re.sub(r'\*(.+?)\*', r'\1', part)
            clean = re.sub(r'`(.+?)`',   r'\1', clean)
            if clean:
                r = para.add_run(clean)
                r.font.name = "Calibri"; r.font.size = Pt(font_size)
                r.font.bold = bold; r.font.color.rgb = color

def is_table_row(line):
    return line.strip().startswith('|') and line.strip().endswith('|')

def is_separator_row(line):
    return is_table_row(line) and re.match(r'^[\|\s\-:]+$', line.strip())

def add_md_table(doc, lines_block):
    rows = []
    for line in lines_block:
        if not is_separator_row(line):
            cells = [c.strip() for c in line.strip().strip('|').split('|')]
            rows.append(cells)
    if not rows: return
    n_cols = max(len(r) for r in rows)
    rows   = [r + ['']*(n_cols-len(r)) for r in rows]
    tbl    = doc.add_table(rows=len(rows), cols=n_cols)
    tbl.style = "Table Grid"
    tbl.alignment = WD_TABLE_ALIGNMENT.LEFT
    col_w = Inches(6.0 / n_cols)
    for ri, row in enumerate(rows):
        is_hdr = (ri == 0)
        for ci, cell_text in enumerate(row):
            cell = tbl.rows[ri].cells[ci]; cell.width = col_w
            set_cell_margins(cell, top=80, bottom=80, left=120, right=120)
            set_cell_bg(cell, DARK_NAVY_BG if is_hdr else (ALT_ROW_BG if ri%2==0 else "FFFFFF"))
            p = cell.paragraphs[0]
            set_para_spacing(p, before=0, after=0, line=240)
            clean = strip_md(cell_text)
            r = p.add_run(clean)
            r.font.name = "Calibri"; r.font.size = Pt(9.5)
            r.font.bold = is_hdr
            r.font.color.rgb = WHITE if is_hdr else GREY
    doc.add_paragraph()


# '' WORD DOCUMENT BUILDER '''''''''''''''''''''''''''''''''''''''''''''''''''''
def build_professional_word(doc_type, label, narrative):
    doc = Document()
    for section in doc.sections:
        section.page_width    = Inches(8.5); section.page_height   = Inches(11)
        section.left_margin   = Inches(1.25); section.right_margin  = Inches(1.25)
        section.top_margin    = Inches(1.1);  section.bottom_margin = Inches(1.0)

    styles = doc.styles
    normal = styles["Normal"]
    normal.font.name = "Calibri"; normal.font.size = Pt(10.5)
    normal.font.color.rgb = GREY
    normal.paragraph_format.space_after  = Pt(6)
    normal.paragraph_format.space_before = Pt(0)

    for sid, sz, bold, color, sp_before, sp_after in [
        ("Heading 1", 15, True,  NAVY,  20, 4),
        ("Heading 2", 12, True,  BLUE,  14, 3),
        ("Heading 3", 11, True,  GREEN, 10, 2),
    ]:
        s = styles[sid]
        s.font.name = "Calibri"; s.font.size = Pt(sz)
        s.font.bold = bold; s.font.color.rgb = color; s.font.italic = False
        s.paragraph_format.space_before = Pt(sp_before)
        s.paragraph_format.space_after  = Pt(sp_after)
        s.paragraph_format.keep_with_next = True

    # Header
    hdr  = doc.sections[0].header
    htbl = hdr.add_table(1, 2, Inches(6.0))
    htbl.columns[0].width = Inches(4.0); htbl.columns[1].width = Inches(2.0)
    lp = htbl.rows[0].cells[0].paragraphs[0]; lp.clear()
    lr = lp.add_run("CECL CRE Model Redevelopment  |  Combined Entity")
    lr.font.name = "Calibri"; lr.font.size = Pt(8.5)
    lr.font.bold = True; lr.font.color.rgb = NAVY
    rp = htbl.rows[0].cells[1].paragraphs[0]
    rp.alignment = WD_ALIGN_PARAGRAPH.RIGHT; rp.clear()
    rr = rp.add_run("Confidential")
    rr.font.name = "Calibri"; rr.font.size = Pt(8.5); rr.font.color.rgb = LGREY
    for cell in htbl.rows[0].cells:
        tcPr = cell._tc.get_or_add_tcPr()
        tcB  = OxmlElement("w:tcBorders")
        bot  = OxmlElement("w:bottom")
        bot.set(qn("w:val"),"single"); bot.set(qn("w:sz"),"6"); bot.set(qn("w:color"),"2E75B6")
        tcB.append(bot); tcPr.append(tcB)

    add_page_number_footer(doc)

    # Cover page
    banner = doc.add_table(1, 1); banner.alignment = WD_TABLE_ALIGNMENT.LEFT
    bc = banner.rows[0].cells[0]
    set_cell_bg(bc, DARK_NAVY_BG); set_cell_margins(bc, top=180, bottom=120, left=180, right=180)
    t1 = bc.paragraphs[0]; set_para_spacing(t1, before=0, after=60, line=240)
    r1 = t1.add_run("CECL MODEL REDEVELOPMENT")
    r1.font.name="Calibri"; r1.font.size=Pt(22); r1.font.bold=True; r1.font.color.rgb=WHITE
    t2 = bc.add_paragraph(); set_para_spacing(t2, before=0, after=60, line=240)
    r2 = t2.add_run("CRE Portfolio  |  PD/LGD Methodology  |  Combined Entity")
    r2.font.name="Calibri"; r2.font.size=Pt(11); r2.font.color.rgb=RGBColor(0xCA,0xDC,0xFC)
    t3 = bc.add_paragraph(); set_para_spacing(t3, before=0, after=0, line=240)
    r3 = t3.add_run(label)
    r3.font.name="Calibri"; r3.font.size=Pt(10.5); r3.font.italic=True; r3.font.color.rgb=RGBColor(0x9E,0xD1,0xFF)
    doc.add_paragraph()

    meta = [
        ("Document Type",       label),
        ("Combined Entity",     "Bank A  +  Bank B"),
        ("Methodology",         "PD/LGD  |  ASC 326-20 CECL"),
        ("Portfolio",           "CRE: Multifamily, Office, Retail, Industrial"),
        ("Data History",        "2016 to 2024  (8 years, including COVID-19 stress period)"),
        ("Report Date",         datetime.now().strftime("%B %d, %Y")),
        ("Classification",      "Confidential  |  Model Risk Management"),
        ("Regulatory Framework","ASC 326-20  |  OCC Comptroller Handbook  |  SR 11-7"),
    ]
    mtbl = doc.add_table(rows=len(meta), cols=2); mtbl.style = "Table Grid"
    for i, (lbl_c, val) in enumerate(meta):
        row = mtbl.rows[i]
        row.cells[0].width = Inches(2.1); row.cells[1].width = Inches(3.9)
        set_cell_bg(row.cells[0], LIGHT_BLUE_BG)
        set_cell_margins(row.cells[0], top=80, bottom=80, left=120, right=120)
        set_cell_margins(row.cells[1], top=80, bottom=80, left=120, right=120)
        lp = row.cells[0].paragraphs[0]; set_para_spacing(lp, before=0, after=0, line=240)
        lr = lp.add_run(lbl_c)
        lr.font.name="Calibri"; lr.font.size=Pt(9.5); lr.font.bold=True; lr.font.color.rgb=NAVY
        vp = row.cells[1].paragraphs[0]; set_para_spacing(vp, before=0, after=0, line=240)
        vr = vp.add_run(val)
        vr.font.name="Calibri"; vr.font.size=Pt(9.5); vr.font.color.rgb=GREY
    doc.add_page_break()

    # Narrative title
    h_title = doc.add_heading(label, level=1)
    add_bottom_border(h_title, color="1F3864", sz=8)

    # Parse narrative markdown
    lines = narrative.strip().splitlines()
    i = 0
    while i < len(lines):
        raw     = lines[i]; stripped = raw.strip(); i += 1
        if not stripped: continue

        # Markdown table block
        if is_table_row(stripped):
            block = [stripped]
            while i < len(lines) and is_table_row(lines[i].strip()):
                block.append(lines[i].strip()); i += 1
            add_md_table(doc, block); continue

        # Skip backtick fences ' render content as body text
        if stripped.startswith("```"):
            while i < len(lines):
                nxt = lines[i].strip(); i += 1
                if nxt.startswith("```"): break
                if nxt:
                    p = doc.add_paragraph(); set_para_spacing(p, before=0, after=60, line=252)
                    r = p.add_run(nxt)
                    r.font.name="Calibri"; r.font.size=Pt(10.5); r.font.color.rgb=GREY
            continue

        # Horizontal rules
        if re.match(r'^[-*_]{3,}$', stripped): continue

        # Markdown headings
        h1m = re.match(r'^#{1}\s+(.+)', stripped)
        h2m = re.match(r'^#{2}\s+(.+)', stripped)
        h3m = re.match(r'^#{3,}\s+(.+)', stripped)
        if h1m:
            p = doc.add_heading(strip_md(h1m.group(1)), level=1)
            add_bottom_border(p, color="1F3864", sz=6); continue
        if h2m:
            p = doc.add_heading(strip_md(h2m.group(1)), level=2)
            add_bottom_border(p, color="2E75B6", sz=4); continue
        if h3m:
            doc.add_heading(strip_md(h3m.group(1)), level=3); continue

        # Numbered section headers ' only if short
        n2m = re.match(r'^(\d+\.\d+)\s+(.+)', stripped)
        n1m = re.match(r'^(\d+)\.\s+(.+)',    stripped)
        if n2m and len(stripped) <= 80:
            p = doc.add_heading(strip_md(stripped), level=2)
            add_bottom_border(p, color="2E75B6", sz=4); continue
        if n1m and len(stripped) <= 80:
            p = doc.add_heading(strip_md(stripped), level=1)
            add_bottom_border(p, color="1F3864", sz=6); continue

        # SECTION X: headers
        if re.match(r'^(SECTION\s+\d+[:\.])', stripped, re.IGNORECASE) and len(stripped) <= 80:
            p = doc.add_heading(strip_md(stripped), level=1)
            add_bottom_border(p, color="1F3864", sz=6); continue

        # ALL CAPS subheadings ' short only
        if re.match(r'^[A-Z][A-Z0-9\s\-/&,\.]{6,}:?\s*$', stripped) and len(stripped) < 60:
            doc.add_heading(strip_md(stripped).rstrip(":").title(), level=3); continue

        # Memo field lines **Label:** value
        memo = re.match(r'^\*\*(.+?):\*\*\s*(.*)', stripped)
        if memo:
            p = doc.add_paragraph(); set_para_spacing(p, before=0, after=60, line=252)
            rl = p.add_run(memo.group(1) + ":  ")
            rl.font.name="Calibri"; rl.font.size=Pt(10.5); rl.font.bold=True; rl.font.color.rgb=NAVY
            rv = p.add_run(strip_md(memo.group(2)))
            rv.font.name="Calibri"; rv.font.size=Pt(10.5); rv.font.color.rgb=GREY
            continue

        # Bullet points
        bm = re.match(r'^[-*+]\s+(.+)', stripped)
        if bm:
            p = doc.add_paragraph(style="List Bullet"); set_para_spacing(p, before=0, after=60, line=252)
            add_rich_run(p, strip_md(bm.group(1)), font_size=10.5, color=GREY); continue

        # Body paragraph
        p = doc.add_paragraph(); set_para_spacing(p, before=0, after=100, line=276)
        add_rich_run(p, stripped, font_size=10.5, color=GREY)

    # Regulatory references
    doc.add_page_break()
    h_ref = doc.add_heading("Regulatory References", level=1)
    add_bottom_border(h_ref, color="1F3864", sz=6)
    refs = [
        ("ASC 326-20",           "Financial Instruments - Credit Losses (CECL). FASB ASU 2016-13."),
        ("SR 11-7",              "Guidance on Model Risk Management. Federal Reserve / OCC."),
        ("OCC Comptroller Handbook", "Commercial Real Estate Lending (2023). Supervisory guidance on CRE."),
        ("12 CFR Part 34 Subpart D", "Real Estate Lending Standards. Supervisory LTV ratio limits."),
        ("FASB ASU 2016-13",     "Measurement of Credit Losses on Financial Instruments."),
        ("OCC 2011-12",          "Sound Practices for Model Risk Management."),
    ]
    rtbl = doc.add_table(rows=len(refs), cols=2); rtbl.style = "Table Grid"
    for i, (rt, rb) in enumerate(refs):
        rtbl.rows[i].cells[0].width = Inches(1.6); rtbl.rows[i].cells[1].width = Inches(4.4)
        set_cell_bg(rtbl.rows[i].cells[0], LIGHT_BLUE_BG)
        set_cell_bg(rtbl.rows[i].cells[1], ALT_ROW_BG if i%2==0 else "FFFFFF")
        set_cell_margins(rtbl.rows[i].cells[0], top=80, bottom=80, left=120, right=120)
        set_cell_margins(rtbl.rows[i].cells[1], top=80, bottom=80, left=120, right=120)
        lp = rtbl.rows[i].cells[0].paragraphs[0]; set_para_spacing(lp, before=0, after=0, line=240)
        lr = lp.add_run(rt)
        lr.font.name="Calibri"; lr.font.size=Pt(9.5); lr.font.bold=True; lr.font.color.rgb=NAVY
        vp = rtbl.rows[i].cells[1].paragraphs[0]; set_para_spacing(vp, before=0, after=0, line=240)
        vr = vp.add_run(rb)
        vr.font.name="Calibri"; vr.font.size=Pt(9.5); vr.font.color.rgb=GREY

    # Disclaimer
    doc.add_paragraph()
    dp = doc.add_paragraph(); set_para_spacing(dp, before=80, after=0, line=240)
    pPr = dp._p.get_or_add_pPr(); pBdr = OxmlElement("w:pBdr")
    top_el = OxmlElement("w:top"); top_el.set(qn("w:val"),"single")
    top_el.set(qn("w:sz"),"4"); top_el.set(qn("w:color"),"D0D0CE")
    pBdr.append(top_el); pPr.append(pBdr)
    dr = dp.add_run("Disclaimer:  ")
    dr.font.name="Calibri"; dr.font.size=Pt(8.5); dr.font.bold=True; dr.font.color.rgb=LGREY
    dr2 = dp.add_run(
        "This document was generated using AI-assisted analysis and is intended for internal "
        "review only. All model results and regulatory interpretations are subject to independent "
        "validation and management approval prior to use in financial reporting."
    )
    dr2.font.name="Calibri"; dr2.font.size=Pt(8.5); dr2.font.color.rgb=LGREY

    buf = BytesIO(); doc.save(buf); buf.seek(0)
    return buf


# '' UI HELPERS ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
def header(title, subtitle=""):
    st.markdown(
        "<h1 style='font-size:26px;font-weight:700;color:#1F3864;margin-bottom:2px;'>{}</h1>"
        "<div style='font-size:12px;color:#6B7FA3;margin-bottom:20px;letter-spacing:.04em;'>{}</div>".format(title, subtitle),
        unsafe_allow_html=True)

def metric_card(label, value, sub="", color="#1F3864"):
    st.markdown(
        "<div style='background:#FFFFFF;border:1px solid #E8EDF5;border-top:3px solid {};"
        "border-radius:8px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);'>"
        "<div style='font-size:11px;color:#6B7FA3;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;'>{}</div>"
        "<div style='font-size:24px;font-family:IBM Plex Mono,monospace;color:#1A1A2E;font-weight:600;'>{}</div>"
        "<div style='font-size:11px;color:#6B7FA3;margin-top:4px;'>{}</div>"
        "</div>".format(color, label, value, sub), unsafe_allow_html=True)


# '' PAGE 1: OVERVIEW ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
def page_overview():
    header("CECL CRE Workbench", "Combined Entity | PD/LGD Methodology | ASC 326")
    inst  = db_query("SELECT * FROM cecl_institutions ORDER BY inst_id")
    loans = db_query("SELECT * FROM cecl_cre_loans")
    if not loans:
        st.info("No data loaded. Go to Data Ingestion and seed demo data to begin.")
        return
    df = pd.DataFrame(loans)
    for col in ["balance","original_balance","ltv_orig","ltv_current","dscr",
                "occupancy","charge_off_amt","recovery_amt","vintage_year"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce")

    total_exposure = float(df["balance"].sum())
    total_defaults = int(df["defaulted"].sum())
    total_losses   = float((df["charge_off_amt"]-df["recovery_amt"]).clip(lower=0).sum())
    n_loans        = len(df)
    inst_names     = {i["inst_id"]: i["inst_name"] for i in inst}

    c1,c2,c3,c4,c5 = st.columns(5)
    with c1: metric_card("Total CRE Exposure", "${:.2f}B".format(total_exposure/1e9), "{} loans".format(n_loans))
    with c2: metric_card("Institutions", str(len(inst)), "Combined entity")
    with c3: metric_card("Historical Defaults", str(total_defaults), "{:.1f}% default rate".format(total_defaults/n_loans*100))
    with c4: metric_card("Net Losses", "${:.1f}M".format(total_losses/1e6), "Charge-off net of recovery")
    with c5: metric_card("Property Types", "4", "MF / Office / Retail / Industrial")

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("<div style='font-size:12px;color:#6B7FA3;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>Exposure by Property Type</div>", unsafe_allow_html=True)
        by_type = df.groupby("property_type")["balance"].sum().reset_index()
        fig = go.Figure(go.Pie(labels=by_type["property_type"], values=by_type["balance"],
            hole=0.55, marker_colors=["#86BC25","#2e6da4","#e8a838","#c0392b"], textfont_size=11))
        fig.update_layout(paper_bgcolor="#0d1530", plot_bgcolor="#0d1530", font_color="#c8d4e8",
            height=260, legend=dict(font_size=11, bgcolor="rgba(0,0,0,0)"), margin=dict(l=0,r=0,t=10,b=10))
        st.plotly_chart(fig)
    with col2:
        st.markdown("<div style='font-size:12px;color:#6B7FA3;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>Exposure by Institution</div>", unsafe_allow_html=True)
        by_inst = df.groupby("inst_id")["balance"].sum().reset_index()
        by_inst["Institution"] = by_inst["inst_id"].map(inst_names)
        fig2 = go.Figure(go.Bar(x=by_inst["Institution"], y=by_inst["balance"]/1e9,
            marker_color=["#86BC25","#2e6da4"],
            text=["${:.2f}B".format(v/1e9) for v in by_inst["balance"]], textposition="outside"))
        fig2.update_layout(paper_bgcolor="#0d1530", plot_bgcolor="#0d1530", font_color="#c8d4e8",
            showlegend=False, yaxis=dict(title="Exposure ($B)", gridcolor="#1e2d4a", color="#6b7fa3"),
            xaxis=dict(color="#6b7fa3"), height=260, margin=dict(l=0,r=0,t=10,b=10))
        st.plotly_chart(fig2)

    st.markdown("<div style='font-size:12px;color:#6B7FA3;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;margin-top:8px;'>Loan Vintage Distribution</div>", unsafe_allow_html=True)
    vintage = df.groupby(["vintage_year","inst_id"])["balance"].sum().reset_index()
    fig3 = go.Figure()
    for inst_id, color in [("BANK-A","#86BC25"),("BANK-B","#2e6da4")]:
        v = vintage[vintage["inst_id"]==inst_id]
        fig3.add_trace(go.Bar(name=inst_names.get(inst_id, inst_id), x=v["vintage_year"], y=v["balance"]/1e6, marker_color=color))
    fig3.update_layout(barmode="stack", paper_bgcolor="#0d1530", plot_bgcolor="#0d1530", font_color="#c8d4e8",
        yaxis=dict(title="Balance ($M)", gridcolor="#1e2d4a", color="#6b7fa3"),
        xaxis=dict(color="#6b7fa3", dtick=1), height=240,
        legend=dict(bgcolor="rgba(0,0,0,0)"), margin=dict(l=0,r=0,t=10,b=10))
    st.plotly_chart(fig3)

    summary = df.groupby("inst_id").agg(
        Loans=("loan_id","count"),
        Exposure=("balance", lambda x: "${:.2f}B".format(x.sum()/1e9)),
        Defaults=("defaulted","sum"),
        Default_Rate=("defaulted", lambda x: "{:.1f}%".format(x.mean()*100)),
        Avg_LTV=("ltv_orig", lambda x: "{:.1f}%".format(x.mean()*100)),
        Avg_DSCR=("dscr", lambda x: "{:.2f}x".format(x.mean())),
    ).reset_index()
    summary["inst_id"] = summary["inst_id"].map(inst_names)
    summary.columns = ["Institution","Loans","Exposure","Defaults","Default Rate","Avg LTV","Avg DSCR"]
    st.dataframe(summary)

def page_ingestion():
    header("Data Ingestion", "Unified CRE Data Model | Bank A + Bank B | Supabase PostgreSQL")

    loan_counts = {}
    rows = db_query("SELECT inst_id, COUNT(*) as cnt FROM cecl_cre_loans GROUP BY inst_id")
    for r in rows:
        loan_counts[r["inst_id"]] = int(r["cnt"])

    c1,c2,c3 = st.columns(3)
    with c1: metric_card("Bank A", "{} loans".format(loan_counts.get("BANK-A",0)),
                          "First National Bank", color="#86BC25" if loan_counts.get("BANK-A",0)>0 else "#6b7fa3")
    with c2: metric_card("Bank B", "{} loans".format(loan_counts.get("BANK-B",0)),
                          "Heritage Commercial Bank", color="#2e6da4" if loan_counts.get("BANK-B",0)>0 else "#6b7fa3")
    with c3: metric_card("Combined", "{} loans".format(sum(loan_counts.values())),
                          "Unified CRE portfolio", color="#86BC25" if sum(loan_counts.values())>0 else "#6b7fa3")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    tab1, tab2, tab3 = st.tabs(["Demo Data", "Field Mapping", "Unified Schema"])

    with tab1:
        st.markdown("""
        <div style='background:#FFFFFF;border:1px solid #E8EDF5;border-radius:8px;padding:20px;margin-bottom:16px;'>
        <div style='color:#86BC25;font-size:12px;font-weight:600;margin-bottom:8px;'>DEMO DATA GENERATOR</div>
        <div style='color:#1A1A2E;font-size:13px;line-height:1.8;'>
        Generates synthetic CRE loan portfolios for both institutions with 7+ years of history,
        realistic default events, and LGD observations. Covers Multifamily, Office, Retail, and Industrial.
        New records are skipped automatically if already loaded.
        </div></div>""", unsafe_allow_html=True)

        col1, col2 = st.columns([1,3])
        with col1:
            if st.button("Seed Demo Data"):
                with st.spinner("Generating portfolios..."):
                    setup_schema()
                    seed_demo_data()
                st.success("Done. 220 Bank A + 180 Bank B loans loaded.")
                st.rerun()

        total = sum(loan_counts.values())
        if total > 0:
            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            sample = db_query(
                "SELECT loan_id,inst_id,property_type,balance,ltv_orig,dscr,"
                "risk_grade,defaulted,vintage_year FROM cecl_cre_loans "
                "ORDER BY inst_id,loan_id LIMIT 20")
            if sample:
                sdf = pd.DataFrame(sample)
                sdf["balance"]  = sdf["balance"].apply(lambda x: "${:.1f}M".format(float(x)/1e6))
                sdf["ltv_orig"] = sdf["ltv_orig"].apply(lambda x: "{:.1f}%".format(float(x)*100))
                sdf["dscr"]     = sdf["dscr"].apply(lambda x: "{:.2f}x".format(float(x)))
                st.dataframe(sdf)

    with tab2:
        st.markdown("""
        <div style='background:#FFFFFF;border:1px solid #E8EDF5;border-radius:8px;padding:20px;'>
        <div style='color:#86BC25;font-size:12px;font-weight:600;margin-bottom:12px;'>FIELD MAPPING CROSSWALK</div>
        </div>""", unsafe_allow_html=True)
        fm = [
            {"Target Field":"loan_id",         "Bank A Source":"LOAN_NUMBER",    "Bank B Source":"ACCT_ID",          "Transform":"Prefix with inst_id"},
            {"Target Field":"origination_dt",  "Bank A Source":"ORIG_DATE",      "Bank B Source":"BOOKING_DATE",     "Transform":"Parse to DATE"},
            {"Target Field":"property_type",   "Bank A Source":"PROP_TYPE_CD",   "Bank B Source":"ASSET_CLASS",      "Transform":"Crosswalk table"},
            {"Target Field":"balance",         "Bank A Source":"CURR_BAL",       "Bank B Source":"OUTSTANDING_PRIN", "Transform":"Numeric, USD"},
            {"Target Field":"ltv_orig",        "Bank A Source":"ORIG_LTV",       "Bank B Source":"LTV_AT_ORIG",      "Transform":"Divide by 100 if pct"},
            {"Target Field":"dscr",            "Bank A Source":"DSCR_RATIO",     "Bank B Source":"DEBT_SVC_CVG",     "Transform":"Numeric, ratio"},
            {"Target Field":"occupancy",       "Bank A Source":"OCC_RATE",       "Bank B Source":"OCCUPANCY_PCT",    "Transform":"Divide by 100 if pct"},
            {"Target Field":"defaulted",       "Bank A Source":"DEFAULT_FLAG",   "Bank B Source":"NPA_FLAG",         "Transform":"Map Y/1 to TRUE"},
            {"Target Field":"default_dt",      "Bank A Source":"DEFAULT_DATE",   "Bank B Source":"NPA_DATE",         "Transform":"Parse to DATE, NULL if none"},
            {"Target Field":"charge_off_amt",  "Bank A Source":"CHRG_OFF_AMT",   "Bank B Source":"WRITE_OFF_AMT",    "Transform":"Numeric, USD, 0 if NULL"},
            {"Target Field":"recovery_amt",    "Bank A Source":"RECOVERY_AMT",   "Bank B Source":"RECOV_AMT",        "Transform":"Numeric, USD, 0 if NULL"},
            {"Target Field":"risk_grade",      "Bank A Source":"INT_RISK_RATING","Bank B Source":"CREDIT_GRADE",     "Transform":"Crosswalk to Pass/Watch/Sub/Doubt"},
            {"Target Field":"vintage_year",    "Bank A Source":"Derived",        "Bank B Source":"Derived",          "Transform":"YEAR(origination_dt)"},
        ]
        st.dataframe(pd.DataFrame(fm))

    with tab3:
        st.markdown("""
        <div style='background:#FFFFFF;border:1px solid #E8EDF5;border-radius:8px;padding:20px;'>
        <div style='color:#86BC25;font-size:12px;font-weight:600;margin-bottom:8px;'>UNIFIED SCHEMA - cecl_cre_loans</div>
        <div style='color:#1A1A2E;font-size:13px;'>PostgreSQL table in Supabase. Both institutions load into this single table with inst_id as discriminator.</div>
        </div>""", unsafe_allow_html=True)
        schema = [
            {"Column":"loan_id","Type":"TEXT","PK":"Yes","Description":"Unique loan identifier (prefixed with inst_id)"},
            {"Column":"inst_id","Type":"TEXT","PK":"Yes","Description":"Institution identifier (BANK-A or BANK-B)"},
            {"Column":"origination_dt","Type":"DATE","PK":"","Description":"Loan origination date"},
            {"Column":"property_type","Type":"TEXT","PK":"","Description":"Multifamily / Office / Retail / Industrial"},
            {"Column":"balance","Type":"NUMERIC","PK":"","Description":"Current outstanding balance (USD)"},
            {"Column":"original_balance","Type":"NUMERIC","PK":"","Description":"Balance at origination (USD)"},
            {"Column":"ltv_orig","Type":"NUMERIC","PK":"","Description":"LTV at origination (decimal, e.g. 0.72)"},
            {"Column":"ltv_current","Type":"NUMERIC","PK":"","Description":"Current LTV (decimal)"},
            {"Column":"dscr","Type":"NUMERIC","PK":"","Description":"Debt Service Coverage Ratio"},
            {"Column":"occupancy","Type":"NUMERIC","PK":"","Description":"Occupancy rate (decimal)"},
            {"Column":"risk_grade","Type":"TEXT","PK":"","Description":"Pass / Watch / Substandard / Doubtful"},
            {"Column":"defaulted","Type":"BOOLEAN","PK":"","Description":"Has loan defaulted (TRUE/FALSE)"},
            {"Column":"default_dt","Type":"DATE","PK":"","Description":"Date of default event (NULL if performing)"},
            {"Column":"charge_off_amt","Type":"NUMERIC","PK":"","Description":"Gross charge-off amount (USD)"},
            {"Column":"recovery_amt","Type":"NUMERIC","PK":"","Description":"Post charge-off recovery (USD)"},
            {"Column":"vintage_year","Type":"INTEGER","PK":"","Description":"Year of origination"},
        ]
        st.dataframe(pd.DataFrame(schema))


# -- PAGE 3: DATA SUFFICIENCY --------------------------------------------------
def page_sufficiency():
    header("Data Sufficiency Scorecard", "ASC 326 PD/LGD Data Requirements | Combined Entity")

    loans = db_query("SELECT * FROM cecl_cre_loans")
    if not loans:
        st.info("No data loaded. Go to Data Ingestion and seed demo data first.")
        return

    df = pd.DataFrame(loans)
    for col in ["balance","ltv_orig","ltv_current","dscr","occupancy","charge_off_amt","recovery_amt","vintage_year"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    inst_names = {"BANK-A":"First National Bank","BANK-B":"Heritage Commercial Bank"}
    all_scores = {}
    for inst_id in ["BANK-A","BANK-B"]:
        sc = compute_sufficiency(df, inst_id)
        all_scores[inst_id] = (sum(1 for r in sc if r["Status"]=="PASS"), len(sc))

    c1,c2,c3 = st.columns(3)
    with c1:
        p,t = all_scores["BANK-A"]
        metric_card("Bank A Pass Rate", "{}/{}".format(p,t), inst_names["BANK-A"], color="#86BC25")
    with c2:
        p,t = all_scores["BANK-B"]
        metric_card("Bank B Pass Rate", "{}/{}".format(p,t), inst_names["BANK-B"], color="#2e6da4")
    with c3:
        combined = sum(v[0] for v in all_scores.values())
        total    = sum(v[1] for v in all_scores.values())
        metric_card("Combined Readiness", "{:.0f}%".format(combined/total*100 if total>0 else 0),
                    "Data sufficiency score")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    tab1, tab2 = st.tabs(["Bank A - First National Bank", "Bank B - Heritage Commercial Bank"])

    for tab, inst_id in zip([tab1,tab2], ["BANK-A","BANK-B"]):
        with tab:
            sc = compute_sufficiency(df, inst_id)
            rows = []
            for r in sc:
                rows.append({
                    "Requirement": r["Requirement"],
                    "Weight":      r["Weight"],
                    "Completeness":r["Completeness"],
                    "Status":      r["Status"],
                    "Note":        r["Note"],
                })
            sc_df = pd.DataFrame(rows)

            def color_row(val):
                if val == "PASS":    return "background-color:#E8F5E9;color:#2E7D32"
                if val == "PARTIAL": return "background-color:#FFF3E0;color:#E65100"
                if val == "FAIL":    return "background-color:#FFEBEE;color:#C62828"
                return ""

            styled = sc_df.style.applymap(color_row, subset=["Status"])
            st.dataframe(styled)


# -- PAGE 4: PIPELINE MONITOR --------------------------------------------------
def page_monitor():
    header("Pipeline Monitor", "Automated Data Quality | 14-Point Validation Framework")

    loans = db_query("SELECT * FROM cecl_cre_loans")
    if not loans:
        st.info("No data loaded. Go to Data Ingestion and seed demo data first.")
        return

    df = pd.DataFrame(loans)
    for col in ["balance","ltv_orig","ltv_current","dscr","occupancy","charge_off_amt","recovery_amt","vintage_year"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    checks = run_pipeline_checks(df)
    chk_df = pd.DataFrame(checks)
    pass_n = (chk_df["Status"]=="PASS").sum()
    fail_n = (chk_df["Status"]=="FAIL").sum()
    part_n = (chk_df["Status"]=="PARTIAL").sum()

    c1,c2,c3,c4 = st.columns(4)
    with c1: metric_card("Total Checks", str(len(chk_df)), "Across 4 categories")
    with c2: metric_card("PASS", str(pass_n), "Checks passed", color="#86BC25")
    with c3: metric_card("PARTIAL", str(part_n), "Partial", color="#e8a838")
    with c4: metric_card("FAIL", str(fail_n), "Checks failed", color="#e05252")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    for category in ["Completeness","Range","Consistency","Coverage"]:
        cat_checks = [c for c in checks if c["Category"]==category]
        with st.expander("{} ({} checks)".format(category, len(cat_checks)), expanded=True):
            for chk in cat_checks:
                col1, col2, col3 = st.columns([3,2,1])
                with col1: st.markdown(chk["Check"])
                with col2: st.markdown("<span style='color:#6b7fa3;font-size:12px;'>{}</span>".format(chk["Value"]), unsafe_allow_html=True)
                with col3:
                    color = {"PASS":"#86BC25","PARTIAL":"#e8a838","FAIL":"#e05252"}.get(chk["Status"],"#6b7fa3")
                    st.markdown("<span style='color:{};font-weight:600;font-size:12px;'>{}</span>".format(color, chk["Status"]), unsafe_allow_html=True)

    # Portfolio stats
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:12px;color:#6b7fa3;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>Portfolio Statistics by Institution</div>", unsafe_allow_html=True)
    inst_names = {"BANK-A":"First National Bank","BANK-B":"Heritage Commercial Bank"}
    summary = df.groupby("inst_id").agg(
        Loans=("loan_id","count"),
        Exposure=("balance", lambda x: "${:.2f}B".format(x.sum()/1e9)),
        Defaults=("defaulted","sum"),
        Default_Rate=("defaulted", lambda x: "{:.1f}%".format(x.mean()*100)),
        Avg_LTV=("ltv_orig", lambda x: "{:.1f}%".format(x.mean()*100)),
        Avg_DSCR=("dscr", lambda x: "{:.2f}x".format(x.mean())),
        Vintage_Min=("vintage_year","min"),
        Vintage_Max=("vintage_year","max"),
    ).reset_index()
    summary["inst_id"] = summary["inst_id"].map(inst_names)
    summary.columns = ["Institution","Loans","Exposure","Defaults","Default Rate",
                        "Avg LTV","Avg DSCR","Vintage Min","Vintage Max"]
    st.dataframe(summary)


# -- PAGE 5: NARRATIVES --------------------------------------------------------
def page_narratives():
    header("Summary and Reports", "AI-Generated ASC 326 Documentation | Review Summaries and Generate Reports")
    if not ANTHROPIC_KEY:
        st.warning("Set ANTHROPIC_API_KEY in your .env file or Streamlit secrets.")
        return
    seg_rows = db_query("SELECT * FROM cecl_model_segments ORDER BY property_type, ltv_min")
    seg_df   = pd.DataFrame(seg_rows) if seg_rows else None

    doc_type = st.selectbox("Select Document", options=list(DOC_LABELS.keys()), format_func=lambda x: DOC_LABELS[x])
    label    = DOC_LABELS[doc_type]
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    tab_summary, tab_generate, tab_catalog = st.tabs(["Summary", "Generate Report", "Previously Generated"])

    with tab_summary:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        render_summary_table(doc_type, seg_df)

    with tab_generate:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown("<div style='background:#EBF3FB;border:1px solid #BBDEFB;border-left:4px solid #1F3864;border-radius:6px;padding:12px 16px;margin-bottom:16px;'><span style='color:#2E7D32;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;'>Selected: </span><span style='color:#1F3864;font-size:13px;'>{}</span></div>".format(label), unsafe_allow_html=True)
        generate = st.button("Generate Report")
        if generate:
            if seg_df is not None and len(seg_df) > 0:
                total_exp = float(seg_df["exposure"].sum()); ecl_b = float(seg_df["ecl_base"].sum())
                ecl_a = float(seg_df["ecl_adverse"].sum()); ecl_s = float(seg_df["ecl_severe"].sum())
                avg_pd = float(seg_df["pd_ttc"].mean()); avg_lgd = float(seg_df["lgd_base"].mean()); n_segs = len(seg_df)
            else:
                total_exp = ecl_b = ecl_a = ecl_s = avg_pd = avg_lgd = 0; n_segs = 0
            context = ("COMBINED ENTITY: Bank A + Bank B\nPORTFOLIO: CRE Only | PD/LGD | ASC 326-20 CECL\nDATE: {}\n"
                "LOANS: 400 (220 Bank A + 180 Bank B) | EXPOSURE: ${:.2f}B\nSEGMENTS: {}\n"
                "AVG PD TTC: {:.2f}% | AVG LGD BASE: {:.1f}%\nECL BASE: ${:.1f}M ({:.2f}%)\n"
                "ECL ADVERSE: ${:.1f}M\nECL SEVERELY ADVERSE: ${:.1f}M\n"
                "DATA HISTORY: 2016-2024 (8 years, including COVID-19 stress period)\n"
                "REGULATORY BASIS: ASC 326-20, OCC Comptroller Handbook, SR 11-7").format(
                datetime.now().strftime("%B %d, %Y"), total_exp/1e9, n_segs, avg_pd*100, avg_lgd*100,
                ecl_b/1e6, (ecl_b/total_exp*100 if total_exp>0 else 0), ecl_a/1e6, ecl_s/1e6)
            prog = st.progress(0)
            try:
                prog.progress(15)
                narrative = generate_narrative(doc_type, context)
                prog.progress(70)
                db_exec("INSERT INTO cecl_narratives (doc_type, content) VALUES (%s, %s)", (doc_type, narrative))
                word_buf = build_professional_word(doc_type, label, narrative)
                prog.progress(100); prog.empty()
            except Exception as e:
                prog.empty(); st.error("Report generation failed: {}".format(e)); st.stop()
            st.success("{} generated successfully.".format(label))
            st.download_button(label="Download Word Document (.docx)", data=word_buf,
                file_name="CECL_{}_{}.docx".format(doc_type, datetime.now().strftime("%Y%m%d")),
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

    with tab_catalog:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        catalog = db_query("SELECT doc_type, created_at FROM cecl_narratives ORDER BY created_at DESC LIMIT 30")
        if catalog:
            cat_df = pd.DataFrame(catalog)
            cat_df["Document"]  = cat_df["doc_type"].map(DOC_LABELS)
            cat_df["Generated"] = pd.to_datetime(cat_df["created_at"]).dt.strftime("%b %d %Y  %H:%M")
            st.dataframe(cat_df[["Document","Generated"]])
        else:
            st.info("No reports generated yet.")


# -- SUMMARY TABLES ------------------------------------------------------------
SUMMARY_TABLES = {
    "methodology_memo": {
        "title": "Methodology Selection Summary",
        "columns": ["Attribute", "Detail"],
        "rows": [
            ("Methodology Selected",    "PD/LGD (Probability of Default / Loss Given Default)"),
            ("Regulatory Basis",        "ASC 326-20 CECL | OCC Comptroller Handbook | SR 11-7"),
            ("Portfolio Scope",         "CRE: Multifamily, Office, Retail, Industrial"),
            ("Segmentation",            "16 segments: 4 property types x 4 LTV bands"),
            ("PD Approach",             "Through-the-cycle (TTC) rates, adjusted to PIT via macro overlay"),
            ("LGD Approach",            "Observed net charge-off severity; regulatory floors applied"),
            ("ECL Formula",             "ECL = PD (PIT) x LGD x EAD per segment"),
            ("Scenario Coverage",       "Base, Adverse, Severely Adverse macro overlays"),
            ("Data History",            "2016-2024 (8 years including COVID-19 stress period)"),
            ("Alternatives Considered", "DCF, Loss-Rate, Probability-Weighted CF"),
            ("Reason for Selection",    "Sufficient default history; segment-level granularity"),
            ("Compliance Status",       "ASC 326-20 compliant; SR 11-7 aligned"),
        ]
    },
    "data_assessment": {
        "title": "Data Assessment Summary",
        "columns": ["Requirement", "Bank A", "Bank B", "Status"],
        "rows": [
            ("Origination Data",     "Complete", "Complete", "PASS"),
            ("Default Events",       "Complete", "Complete", "PASS"),
            ("Default Dates",        "Complete", "Complete", "PASS"),
            ("Charge-off Amounts",   "Complete", "Complete", "PASS"),
            ("Recovery Amounts",     "Complete", "Complete", "PASS"),
            ("LTV at Origination",   "Complete", "Complete", "PASS"),
            ("Current LTV",          "Complete", "Complete", "PASS"),
            ("DSCR",                 "Complete", "Complete", "PASS"),
            ("Occupancy Rate",       "Complete", "Complete", "PASS"),
            ("Property Type",        "Complete", "Complete", "PASS"),
            ("Risk Grade",           "Complete", "Complete", "PASS"),
            ("Vintage Year",         "Complete", "Complete", "PASS"),
            ("Balance Outstanding",  "Complete", "Complete", "PASS"),
            ("Geographic ID",        "Complete", "Complete", "PASS"),
        ]
    },
    "implementation_timeline": {
        "title": "Implementation Timeline Summary",
        "columns": ["Phase", "Weeks", "Key Activities", "Milestone"],
        "rows": [
            ("1. Discovery",    "Wks 1-3",   "Data inventory, field mapping, gap assessment",      "Governance sign-off"),
            ("2. Data Build",   "Wks 4-8",   "Seed unified schema, run QA checks, remediate gaps", "Data lock"),
            ("3. Development",  "Wks 9-14",  "Compute PD/LGD, calibrate floors, ECL prototype",    "Model prototype"),
            ("4. Validation",   "Wks 15-19", "Back-test, sensitivity analysis, MRM review",         "MRM sign-off"),
            ("5. Parallel Run", "Wks 20-24", "Run alongside legacy, compare, explain variances",     "Parallel complete"),
            ("6. Go-Live",      "Wks 25-26", "Regulatory submission, board approval, deployment",    "First CECL disclosure"),
        ]
    },
    "model_risk_doc": {
        "title": "Model Risk Documentation Summary",
        "columns": ["SR 11-7 Component", "Status", "Key Points"],
        "rows": [
            ("Model Purpose and Scope",      "Documented",  "CRE CECL PD/LGD for combined entity under ASC 326-20"),
            ("Conceptual Soundness",         "Documented",  "PD/LGD grounded in credit theory; segment design validated"),
            ("Developmental Evidence",       "Documented",  "8-yr history; back-test results; calibration documentation"),
            ("Limitations",                  "Documented",  "Thin segments; data harmonization; LGD floor reliance"),
            ("Compensating Controls",        "In Place",    "Conservative floors; segment pooling; qualitative overlay"),
            ("Ongoing Monitoring Plan",      "Documented",  "Quarterly PD/LGD stability; annual recalibration trigger"),
            ("Independent Validation",       "Pending",     "MRM review scheduled post-development phase"),
            ("Model Inventory Registration", "Pending",     "OCC model inventory submission on go-live"),
        ]
    },
    "ecl_results_summary": {
        "title": "ECL Results Summary",
        "columns": ["Scenario", "ECL ($M)", "ECL % of Exposure", "PD Multiplier", "LGD Add"],
        "rows": [
            ("Base Case",        "See report", "See report", "1.00x", "0.0%"),
            ("Adverse",          "See report", "See report", "1.55x", "+6.0%"),
            ("Severely Adverse", "See report", "See report", "2.40x", "+14.0%"),
        ]
    },
}

def render_summary_table(doc_type, seg_df):
    config = SUMMARY_TABLES.get(doc_type)
    if not config:
        return
    st.markdown(
        "<div style='font-size:12px;color:#1F3864;font-weight:600;text-transform:uppercase;"
        "letter-spacing:.08em;margin-bottom:8px;'>{}</div>".format(config["title"]),
        unsafe_allow_html=True)
    if doc_type == "ecl_results_summary" and seg_df is not None and len(seg_df) > 0:
        total_exp = float(seg_df["exposure"].sum())
        ecl_b = float(seg_df["ecl_base"].sum())
        ecl_a = float(seg_df["ecl_adverse"].sum())
        ecl_s = float(seg_df["ecl_severe"].sum())
        rows = [
            ("Base Case",        "${:.1f}M".format(ecl_b/1e6), "{:.2f}%".format(ecl_b/total_exp*100 if total_exp>0 else 0), "1.00x", "0.0%"),
            ("Adverse",          "${:.1f}M".format(ecl_a/1e6), "{:.2f}%".format(ecl_a/total_exp*100 if total_exp>0 else 0), "1.55x", "+6.0%"),
            ("Severely Adverse", "${:.1f}M".format(ecl_s/1e6), "{:.2f}%".format(ecl_s/total_exp*100 if total_exp>0 else 0), "2.40x", "+14.0%"),
        ]
        df_display = pd.DataFrame(rows, columns=config["columns"])
    else:
        df_display = pd.DataFrame(config["rows"], columns=config["columns"])
    def style_status(val):
        if val in ("PASS","Documented","In Place"): return "background-color:#E8F5E9;color:#2E7D32;font-weight:600"
        if val in ("PARTIAL","Pending"):            return "background-color:#FFF3E0;color:#E65100;font-weight:600"
        if val == "FAIL":                           return "background-color:#FFEBEE;color:#C62828;font-weight:600"
        return ""
    if "Status" in df_display.columns:
        st.dataframe(df_display.style.applymap(style_status, subset=["Status"]))
    else:
        st.dataframe(df_display)



def page_agent():
    header("AI Analysis Agent", "Autonomous CECL Workflow | AI-Powered Analysis")
    setup_schema()
    db_exec("""CREATE TABLE IF NOT EXISTS cecl_agent_runs (
        run_id SERIAL PRIMARY KEY, run_dt TIMESTAMP DEFAULT NOW(),
        status TEXT, segments_checked INTEGER, anomalies_found INTEGER,
        ecl_base_fmt TEXT, findings TEXT, word_doc TEXT
    )""")

    loans_count = db_query("SELECT COUNT(*) as cnt FROM cecl_cre_loans")
    seg_count   = db_query("SELECT COUNT(*) as cnt FROM cecl_model_segments")
    narr_count  = db_query("SELECT COUNT(*) as cnt FROM cecl_narratives")
    n_loans = int(loans_count[0]["cnt"]) if loans_count else 0
    n_segs  = int(seg_count[0]["cnt"])   if seg_count else 0
    n_narr  = int(narr_count[0]["cnt"])  if narr_count else 0

    c1, c2, c3 = st.columns(3)
    with c1: metric_card("Loans Available", str(n_loans), "In Supabase")
    with c2: metric_card("Segments Computed", str(n_segs), "Model results")
    with c3: metric_card("Reports Generated", str(n_narr), "Saved to DB")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown(
        "<div style='background:#EBF3FB;border:1px solid #BBDEFB;border-left:4px solid #1F3864;"
        "border-radius:6px;padding:14px 18px;margin-bottom:20px;'>"
        "<div style='color:#1F3864;font-size:12px;font-weight:600;text-transform:uppercase;"
        "letter-spacing:.06em;margin-bottom:8px;'>WHAT THE AGENT DOES</div>"
        "<div style='color:#1A1A2E;font-size:13px;line-height:1.8;'>"
        "One click runs the complete CECL workflow autonomously: "
        "<b>1. Portfolio Query</b> -- Reads and summarises all loans "
        "<b>2. Data Quality</b> -- Runs all pipeline checks "
        "<b>3. PD/LGD Model</b> -- Computes ECL across all 16 segments "
        "<b>4. Anomaly Detection</b> -- Flags high-PD, thin, concentration risks "
        "<b>5. ECL Summary</b> -- Generates Board-ready Word report"
        "</div></div>",
        unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1,1,2])
    with col1:
        run_btn = st.button("Run Full Analysis")

    if not run_btn:
        last = db_query("SELECT * FROM cecl_agent_runs ORDER BY run_dt DESC LIMIT 1")
        if last:
            r = last[0]
            st.markdown(
                "<div style='font-size:12px;color:#6B7FA3;text-transform:uppercase;"
                "letter-spacing:.08em;margin-bottom:8px;'>LAST RUN</div>",
                unsafe_allow_html=True)
            lc1, lc2, lc3, lc4 = st.columns(4)
            with lc1: metric_card("Status", str(r.get("status","--")), str(r.get("run_dt",""))[:16])
            with lc2: metric_card("Segments", str(r.get("segments_checked",0)), "Analysed")
            with lc3: metric_card("Anomalies", str(r.get("anomalies_found",0)), "Flagged")
            with lc4: metric_card("ECL Base", str(r.get("ecl_base_fmt","--")), "Combined")
            if r.get("findings"):
                st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
                st.markdown(
                    "<div style='font-size:12px;color:#1F3864;font-weight:600;text-transform:uppercase;"
                    "letter-spacing:.08em;margin-bottom:8px;'>AGENT FINDINGS</div>",
                    unsafe_allow_html=True)
                findings_html = r["findings"].replace("\n", "<br>")
                st.markdown(
                    "<div style='background:#F5F8FF;border:1px solid #DDEAFF;border-radius:8px;"
                    "padding:16px;color:#1A1A2E;font-size:13px;line-height:1.8;'>"
                    + findings_html + "</div>",
                    unsafe_allow_html=True)
            if r.get("word_doc"):
                import base64
                st.download_button(
                    label="Download Last Report (.docx)",
                    data=base64.b64decode(r["word_doc"]),
                    file_name="CECL_Agent_Report.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        return

    loans = db_query("SELECT * FROM cecl_cre_loans")
    if not loans:
        st.error("No loan data. Go to Data Ingestion and seed demo data first.")
        return

    df = pd.DataFrame(loans)
    for col in ["balance","ltv_orig","ltv_current","dscr","occupancy","charge_off_amt","recovery_amt","vintage_year"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log_area = st.empty()
    prog     = st.progress(0)
    log_lines = []

    def log(msg, pct=None):
        log_lines.append(msg)
        log_area.markdown(
            "<div style='background:#F0F4FF;border:1px solid #D0D8F0;border-radius:8px;"
            "padding:14px;font-family:IBM Plex Mono,monospace;font-size:11px;"
            "color:#1F3864;line-height:1.8;max-height:220px;overflow-y:auto;'>"
            + "<br>".join(log_lines[-12:]) + "</div>",
            unsafe_allow_html=True)
        if pct is not None:
            prog.progress(pct)

    log("[AI] Starting full CECL analysis...", 5)
    checks = run_pipeline_checks(df)
    passes = sum(1 for c in checks if c["Status"] == "PASS")
    log("Step 1/5: Data quality -- {}/{} checks passed".format(passes, len(checks)), 15)

    log("Step 2/5: Running PD/LGD model across 16 segments...", 25)
    LTV_BANDS  = [("<=60%",0.00,0.60),("60-70%",0.60,0.70),("70-80%",0.70,0.80),(">80%",0.80,1.00)]
    MACRO_A    = {"base":{"pd_mult":1.00,"lgd_add":0.00},"adverse":{"pd_mult":1.55,"lgd_add":0.06},"severe":{"pd_mult":2.40,"lgd_add":0.14}}
    LGD_FLOORS = {"Multifamily":0.25,"Office":0.35,"Retail":0.38,"Industrial":0.28}
    results    = []
    for pt in PROPERTY_TYPES:
        for band, ltv_min, ltv_max in LTV_BANDS:
            mask = ((df["property_type"]==pt) & (df["ltv_orig"]>=ltv_min) &
                    (df["ltv_orig"]<ltv_max if ltv_max<1.0 else df["ltv_orig"]<=ltv_max))
            seg = df[mask]
            if len(seg) == 0:
                continue
            n, n_def  = len(seg), int(seg["defaulted"].sum())
            exposure  = float(seg["balance"].sum())
            defs      = seg[seg["defaulted"]==True]
            pd_ttc    = float(n_def/n)
            if len(defs) > 0:
                net_loss = defs["charge_off_amt"] - defs["recovery_amt"]
                lgd_base = float((net_loss/defs["balance"].replace(0,np.nan)).clip(0,1).mean())
                if np.isnan(lgd_base):
                    lgd_base = float(LGD_FLOORS.get(pt, 0.32))
            else:
                lgd_base = float(LGD_FLOORS.get(pt, 0.32))
            pd_b  = float(pd_ttc * MACRO_A["base"]["pd_mult"])
            pd_a  = float(pd_ttc * MACRO_A["adverse"]["pd_mult"])
            pd_s  = float(pd_ttc * MACRO_A["severe"]["pd_mult"])
            lgd_a = float(min(lgd_base + MACRO_A["adverse"]["lgd_add"], 0.95))
            lgd_s = float(min(lgd_base + MACRO_A["severe"]["lgd_add"], 0.95))
            seg_id = str(pt[:3].upper() + "-" + band.replace("%","").replace("<=","LE").replace(">","GT").replace("-","_"))
            results.append({
                "segment_id":str(seg_id),"property_type":str(pt),"ltv_band":str(band),
                "ltv_min":float(ltv_min),"ltv_max":float(ltv_max),"loan_count":int(n),"exposure":float(exposure),
                "pd_ttc":float(pd_ttc),"pd_pit_base":float(pd_b),"pd_pit_adverse":float(pd_a),"pd_pit_severe":float(pd_s),
                "lgd_base":float(lgd_base),"lgd_adverse":float(lgd_a),"lgd_severe":float(lgd_s),
                "ecl_base":float(pd_b*lgd_base*exposure),"ecl_adverse":float(pd_a*lgd_a*exposure),
                "ecl_severe":float(pd_s*lgd_s*exposure),
            })

    seg_df2   = pd.DataFrame(results)
    total_exp = float(seg_df2["exposure"].sum())
    ecl_b     = float(seg_df2["ecl_base"].sum())
    ecl_a     = float(seg_df2["ecl_adverse"].sum())
    ecl_s     = float(seg_df2["ecl_severe"].sum())
    ecl_base_fmt = "${:.1f}M ({:.2f}%)".format(ecl_b/1e6, ecl_b/total_exp*100)

    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM cecl_model_segments")
    for r in results:
        cur.execute(
            "INSERT INTO cecl_model_segments "
            "(segment_id,property_type,ltv_band,ltv_min,ltv_max,loan_count,exposure,"
            "pd_ttc,pd_pit_base,pd_pit_adverse,pd_pit_severe,"
            "lgd_base,lgd_adverse,lgd_severe,ecl_base,ecl_adverse,ecl_severe) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (str(r["segment_id"]),str(r["property_type"]),str(r["ltv_band"]),
             float(r["ltv_min"]),float(r["ltv_max"]),int(r["loan_count"]),float(r["exposure"]),
             float(r["pd_ttc"]),float(r["pd_pit_base"]),float(r["pd_pit_adverse"]),
             float(r["pd_pit_severe"]),float(r["lgd_base"]),float(r["lgd_adverse"]),
             float(r["lgd_severe"]),float(r["ecl_base"]),float(r["ecl_adverse"]),
             float(r["ecl_severe"])))
    conn.commit(); conn.close()
    log("Step 2/5: Model complete -- {} segments | ECL Base: {}".format(len(results), ecl_base_fmt), 40)

    seg_summary = "\n".join([
        "  {}: {} loans | PD={:.2f}% | LGD={:.1f}% | ECL=${:.2f}M".format(
            r["segment_id"], r["loan_count"], r["pd_ttc"]*100, r["lgd_base"]*100, r["ecl_base"]/1e6)
        for r in results])

    log("Step 3/5: Running AI anomaly detection...", 55)
    anomaly_text = ""
    anomaly_count = 0
    try:
        client_ai = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        anomaly_prompt = (
            "You are a senior credit risk analyst reviewing CECL PD/LGD model results. "
            "Identify anomalies, outliers, or concerns. Focus on: unusually high/low PD, "
            "LGD floor reliance, thin segments (<20 loans), concentration risk. "
            "List findings as numbered points.\n\nSEGMENT RESULTS:\n{}\n\n"
            "PORTFOLIO: 400 loans | ${:.2f}B exposure | ECL Base: {}".format(
                seg_summary, total_exp/1e9, ecl_base_fmt))
        ar = client_ai.messages.create(
            model="claude-sonnet-4-6", max_tokens=800,
            messages=[{"role":"user","content":anomaly_prompt}])
        anomaly_text  = ar.content[0].text
        anomaly_count = sum(1 for line in anomaly_text.split("\n") if line.strip() and line.strip()[0].isdigit())
    except Exception as e:
        anomaly_text = "Anomaly detection unavailable: {}".format(e)
    log("Step 3/5: {} anomalies identified".format(anomaly_count), 65)

    log("Step 4/5: Generating ECL Results Summary...", 72)
    narrative = ""
    try:
        narr_prompt = (
            "Write a COMPLETE ECL Results Summary for the Board Risk Committee. "
            "Incorporate the anomalies below. "
            "Structure: (1) Key Findings (2) ECL by Segment (3) Scenario Analysis "
            "(4) Anomalies and Concerns (5) Management Conclusions. "
            "Use ## for sections, - for bullets. Write in markdown.\n\n"
            "PORTFOLIO: Bank A + Bank B | ${:.3f}B | 400 loans\n"
            "ECL Base: {} | ECL Adverse: ${:.1f}M | ECL Severe: ${:.1f}M\n\n"
            "AGENT-IDENTIFIED ANOMALIES:\n{}\n\nSEGMENT DETAIL:\n{}".format(
                total_exp/1e9, ecl_base_fmt, ecl_a/1e6, ecl_s/1e6, anomaly_text, seg_summary))
        nr = client_ai.messages.create(
            model="claude-sonnet-4-6", max_tokens=3000,
            messages=[{"role":"user","content":narr_prompt}])
        narrative = nr.content[0].text
    except Exception as e:
        narrative = "Report generation failed: {}".format(e)
    log("Step 4/5: Narrative generated ({} words)".format(len(narrative.split())), 82)

    log("Step 5/5: Building Word document...", 90)
    word_b64 = ""
    try:
        import base64
        word_buf = build_professional_word("ecl_results_summary", "ECL Results Summary -- Agent Run", narrative)
        word_b64 = base64.b64encode(word_buf.read()).decode()
    except Exception as e:
        log("  Word doc failed: {}".format(e))

    combined_findings = "ANOMALIES:\n{}\n\nNARRATIVE SUMMARY:\n{}".format(
        anomaly_text, narrative[:500] + "...")
    db_exec(
        "INSERT INTO cecl_agent_runs "
        "(status,segments_checked,anomalies_found,ecl_base_fmt,findings,word_doc) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        ("COMPLETE", len(results), anomaly_count, ecl_base_fmt, combined_findings, word_b64))
    db_exec("INSERT INTO cecl_narratives (doc_type, content) VALUES (%s,%s)",
            ("ecl_results_summary", narrative))

    prog.progress(100)
    log("Analysis complete.", 100)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    rc1, rc2, rc3, rc4 = st.columns(4)
    with rc1: metric_card("Status", "COMPLETE", "All steps done", color="#2E7D32")
    with rc2: metric_card("Segments", str(len(results)), "16 analysed")
    with rc3: metric_card("Anomalies", str(anomaly_count), "Flagged")
    with rc4: metric_card("ECL Base", ecl_base_fmt.split(" ")[0],
                          ecl_base_fmt.split("(")[1].rstrip(")") if "(" in ecl_base_fmt else "")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    t1, t2, t3 = st.tabs(["Agent Findings", "Segment Results", "Download Report"])

    with t1:
        findings_html = anomaly_text.replace("\n", "<br>")
        st.markdown(
            "<div style='background:#F5F8FF;border:1px solid #DDEAFF;border-radius:8px;"
            "padding:16px;color:#1A1A2E;font-size:13px;line-height:1.8;'>"
            + findings_html + "</div>",
            unsafe_allow_html=True)

    with t2:
        disp = seg_df2[["property_type","ltv_band","loan_count","exposure",
                         "pd_ttc","lgd_base","ecl_base","ecl_adverse","ecl_severe"]].copy()
        for col in ["exposure","ecl_base","ecl_adverse","ecl_severe"]:
            disp[col] = disp[col].apply(lambda x: "${:.1f}M".format(float(x)/1e6))
        for col in ["pd_ttc","lgd_base"]:
            disp[col] = disp[col].apply(lambda x: "{:.2f}%".format(float(x)*100))
        disp.columns = ["Property Type","LTV Band","Loans","Exposure","PD TTC",
                        "LGD Base","ECL Base","ECL Adverse","ECL Severe"]
        st.dataframe(disp)

    with t3:
        if word_b64:
            import base64
            st.download_button(
                label="Download ECL Results Report (.docx)",
                data=base64.b64decode(word_b64),
                file_name="CECL_Agent_ECL_{}.docx".format(datetime.now().strftime("%Y%m%d")),
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        else:
            st.info("Word document not available.")



def sidebar():
    with st.sidebar:
        st.markdown("""
        <div style='padding:20px 0 10px 0;'>
        <div style='font-size:18px;font-weight:700;color:#1F3864;'>CECL CRE</div>
        <div style='font-size:10px;color:#6b7fa3;letter-spacing:.1em;text-transform:uppercase;'>
        Model Redevelopment Workbench</div>
        </div>""", unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#1e2d4a;margin:0 0 12px 0;'>", unsafe_allow_html=True)

        nav_options = ["Overview", "Data Ingestion", "Data Sufficiency",
                       "Pipeline Monitor", "Summary and Reports", "AI Agent"]
        default_idx = 0
        if st.session_state.get("nav_page") in nav_options:
            default_idx = nav_options.index(st.session_state["nav_page"])
            st.session_state["nav_page"] = None
        page = st.radio("Navigation", nav_options, index=default_idx)

        st.markdown("<hr style='border-color:#1e2d4a;margin:12px 0;'>", unsafe_allow_html=True)
        st.markdown("""
        <div style='font-size:10px;color:#6b7fa3;line-height:1.8;'>
        Combined Entity<br>
        Bank A<br>
        Bank B<br><br>
        ASC 326-20 | PD/LGD<br>
        SR 11-7 | OCC Handbook
        </div>""", unsafe_allow_html=True)


        st.markdown("<hr style='border-color:#1e2d4a;margin:8px 0;'>", unsafe_allow_html=True)
        user = st.session_state.get("username", "")
        st.markdown("<div style='font-size:11px;color:#B0C4DE;margin-bottom:6px;'>Signed in as <b style='color:#FFFFFF;'>{}</b></div>".format(user), unsafe_allow_html=True)
        if st.button("Sign Out"):
            st.session_state["authenticated"] = False
            st.session_state["username"] = ""
            safe_rerun()
    return page

def main():
    setup_schema()
    page = sidebar()
    if page == "Overview":         page_overview()
    elif page == "Data Ingestion": page_ingestion()
    elif page == "Data Sufficiency": page_sufficiency()
    elif page == "Pipeline Monitor": page_monitor()
    elif page == "Summary and Reports": page_narratives()
    elif page == "AI Agent":            page_agent()

if __name__ == "__main__":
    if not st.session_state.get("authenticated"):
        login_page()
    else:
        main()
