# cecl_v3.py ' CECL CRE Workbench | Clean final build
# Pages: Overview | Data Ingestion | Data Sufficiency | Data Quality Monitor | Narratives

import streamlit as st
import pandas as pd
import numpy as np
import psycopg2, psycopg2.extras, decimal, os, re
import anthropic
from dotenv import load_dotenv
from datetime import datetime, date
from io import BytesIO, StringIO
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
# Direct connection (port 5432) - bypasses PgBouncer, supports bulk inserts
DB_CFG_DIRECT = dict(
    host="db.dnvyxvlbebnuvxitblbu.supabase.co",
    port=5432, dbname="postgres",
    user="postgres",
    password="Indroyal019283",
    connect_timeout=10,
)
def get_direct_conn():
    try:
        return psycopg2.connect(**DB_CFG_DIRECT)
    except Exception:
        return psycopg2.connect(**DB_CFG)  # fallback to pooler
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
VOYAGE_KEY    = os.getenv("VOYAGE_API_KEY", "")
try:
    if not ANTHROPIC_KEY:
        ANTHROPIC_KEY = st.secrets.get("ANTHROPIC_API_KEY", "")
        if not VOYAGE_KEY: VOYAGE_KEY = st.secrets.get("VOYAGE_API_KEY", "")
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
.stButton > button, .stDownloadButton > button {
    background:#1F3864 !important;
    color:#FFFFFF !important;
    border:2px solid #1F3864 !important;
    font-weight:600 !important;
    font-size:13px !important;
    letter-spacing:.01em !important;
    text-transform:none !important;
    border-radius:6px !important;
    padding:9px 22px !important;
    box-shadow:0 2px 6px rgba(31,56,100,0.25) !important;
}
.stButton > button:hover, .stDownloadButton > button:hover {
    background:#2E75B6 !important;
    border-color:#2E75B6 !important;
    color:#FFFFFF !important;
}
.stButton > button p, .stButton > button span, .stButton > button div,
.stDownloadButton > button p, .stDownloadButton > button span, .stDownloadButton > button div {
    color:#FFFFFF !important;
    text-transform:none !important;
}
[data-testid="stSidebar"] .stButton > button {
    background:transparent !important;
    border:none !important;
    box-shadow:none !important;
    color:#A8C4E0 !important;
}
[data-testid="stSidebar"] .stButton > button p,
[data-testid="stSidebar"] .stButton > button span {
    color:#A8C4E0 !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background:rgba(255,255,255,0.10) !important;
    color:#FFFFFF !important;
}
[data-testid="stSidebar"] .stButton > button:hover p,
[data-testid="stSidebar"] .stButton > button:hover span {
    color:#FFFFFF !important;
}
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
/* Force light background on tab panels and expander content */
div[data-baseweb="tab-panel"] { background:#FFFFFF !important; color:#1A1A2E !important; }
div[data-testid="stExpander"] details summary { background:#FFFFFF !important; color:#1A1A2E !important; }
div[data-testid="stExpander"] details { background:#FFFFFF !important; }
div[data-testid="stExpanderDetails"] { background:#FFFFFF !important; color:#1A1A2E !important; }
.streamlit-expanderContent { background:#FFFFFF !important; color:#1A1A2E !important; }
.streamlit-expanderContent * { color:#1A1A2E !important; }
/* Ensure step card HTML text shows correctly */
.element-container div[style*="background:#1F3864"] * { color:#FFFFFF !important; }
.element-container div[style*="background:#2E7D32"] * { color:#FFFFFF !important; }
.element-container div[style*="background:#C62828"] * { color:#FFFFFF !important; }
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
                    # Clear upload counts so page starts at 0
                    for _k in ["session_n_a","session_n_b","session_n_combined"]:
                        if _k in st.session_state: del st.session_state[_k]
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
        ("BANK-B","Bank B",850000000)]:
        db_exec("INSERT INTO cecl_institutions (inst_id,inst_name,total_cre,data_start) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT (inst_id) DO NOTHING",
                (inst_id, name, total_cre, date(2016,1,1)))
    for inst_id, n, seed in [("BANK-A",220,42),("BANK-B",180,99)]:
        loans = generate_loans(inst_id, n, seed)
        conn = get_conn(); cur = conn.cursor()
        for l in loans:
            cur.execute(
                "INSERT INTO cecl_cre_loans "
                "(loan_id,inst_id,origination_dt,maturity_dt,property_type,balance,"
                "original_balance,ltv_orig,ltv_current,dscr,occupancy,risk_grade,state,"
                "defaulted,default_dt,charge_off_amt,recovery_amt,vintage_year) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (loan_id,inst_id) DO NOTHING",
                (str(l["loan_id"]),str(l["inst_id"]),l["origination_dt"],l["maturity_dt"],
                 str(l["property_type"]),float(l["balance"]),float(l["original_balance"]),
                 float(l["ltv_orig"]),float(l["ltv_current"]),float(l["dscr"]),float(l["occupancy"]),
                 str(l["risk_grade"]),str(l["state"]),bool(l["defaulted"]),l["default_dt"],
                 float(l["charge_off_amt"]),float(l["recovery_amt"]),int(l["vintage_year"])))
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
    {"requirement":"Geographic identifier",        "field":"state",           "min_years":5, "weight":"Supplemental"}]

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

def retrieve_regulatory_context(query_text, match_count=4):
    """Embed query with Voyage AI and retrieve top matching regulatory chunks from Supabase."""
    if not VOYAGE_KEY:
        return "__NO_KEY__"
    try:
        import requests as _req
        headers = {
            "Authorization": "Bearer " + VOYAGE_KEY,
            "Content-Type": "application/json",
        }
        body = {"model": "voyage-3", "input": [query_text], "input_type": "query"}
        r = _req.post("https://api.voyageai.com/v1/embeddings",
                      headers=headers, json=body, timeout=30)
        if r.status_code != 200:
            return ""
        emb = r.json()["data"][0]["embedding"]
        emb_str = "[" + ",".join(str(x) for x in emb) + "]"
        # Use direct connection to run vector similarity query
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT doc_name, doc_title, chunk_text, "
            "1 - (embedding <=> %s::vector) AS similarity "
            "FROM cecl_reg_embeddings "
            "ORDER BY embedding <=> %s::vector "
            "LIMIT %s",
            (emb_str, emb_str, match_count)
        )
        rows = cur.fetchall()
        cur.close(); conn.close()
        if not rows:
            return ""
        context_parts = []
        for row in rows:
            # row is a tuple: (doc_name, doc_title, chunk_text, similarity)
            context_parts.append("[{}]\n{}".format(row[1], row[2]))
        return "\n\n---\n\n".join(context_parts)
    except Exception as _e:
        return "__ERROR__: " + str(_e)


def generate_narrative(doc_type, context):
    # Augment context with relevant regulatory passages
    reg_ctx = retrieve_regulatory_context(context[:400])
    if reg_ctx:
        context = context + "\n\nRELEVANT REGULATORY GUIDANCE (cite specific sections):\n" + reg_ctx
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
        ("Heading 3", 11, True,  GREEN, 10, 2)]:
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
        ("Regulatory Framework","ASC 326-20  |  OCC Comptroller Handbook  |  SR 11-7")]
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
        ("OCC 2011-12",          "Sound Practices for Model Risk Management.")]
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
        "<div style='font-size:11px;color:#6B7FA3;letter-spacing:.02em;margin-bottom:6px;'>{}</div>"
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

    st.warning(f"{violations} monotonicity violation(s) ' acceptable for small portfolio. Document in Model Card.")

    # '' TAB 3: SEGMENT CALIBRATION ''''''''''''''''''''''''''''''''''''''''''
    with tab3:
        st.markdown("### Segment-Level Calibration Check")
        st.markdown("<div style='font-size:12px;color:#555;margin-bottom:10px;'>Predicted PD vs observed default rate for all 16 segments. Gaps >20% feed into the Q-Factor Register.</div>", unsafe_allow_html=True)
        calib_rows = []
        for pt in PROP_TYPES:
            for band, lmin, lmax in LTV_BANDS:
                mask = (df["property_type"]==pt) & (df["ltv_orig"].fillna(0)>=lmin) & (df["ltv_orig"].fillna(0)<lmax)
                seg  = df[mask]
                if len(seg)==0: continue
                n_c  = len(seg); n_d = int(seg["defaulted"].sum())
                obs  = float(n_d/n_c)
                pred = pred_map.get(pt, port_pd)
                gap  = obs - pred
                gap_pct = (gap/max(pred,0.001))*100
                tl_v = "GREEN" if abs(gap_pct)<=20 else "AMBER" if abs(gap_pct)<=50 else "RED"
                qbps = round(abs(gap)*100*2) if abs(gap_pct)>20 else 0
                calib_rows.append({"Segment":"{} {}".format(pt[:3],band),"Loans":n_c,
                    "Observed PD":"{:.2f}%".format(obs*100),
                    "Predicted PD":"{:.2f}%".format(pred*100),
                    "Gap":"{:+.2f}pp".format(gap*100),
                    "Gap %":"{:+.1f}%".format(gap_pct),
                    "Status":tl_v,
                    "Q-Factor Suggestion":"{} bps".format(qbps) if qbps>0 else "None",
                    "_tl":tl_v,"_qbps":qbps})
        if calib_rows:
            red_n   = sum(1 for r in calib_rows if r["_tl"]=="RED")
            amb_n   = sum(1 for r in calib_rows if r["_tl"]=="AMBER")
            total_q = sum(r["_qbps"] for r in calib_rows)
            c1,c2,c3,c4 = st.columns(4)
            with c1: metric_card("Segments Tested", str(len(calib_rows)), "Of 16 total")
            with c2: metric_card("Red",  str(red_n), "Gap >50%",  color="#C62828" if red_n else "#2E7D32")
            with c3: metric_card("Amber",str(amb_n), "Gap 20-50%",color="#E65100" if amb_n else "#2E7D32")
            with c4: metric_card("Total Q Suggestion","~{} bps".format(total_q), "Sum of red/amber gaps")
            disp_cols = ["Segment","Loans","Observed PD","Predicted PD","Gap","Gap %","Status","Q-Factor Suggestion"]
            st.dataframe(pd.DataFrame(calib_rows)[disp_cols], use_container_width=True, hide_index=True)
            if st.button("Export Calibration Gaps to Assumption Log", type="primary"):
                for r in calib_rows:
                    if r["_qbps"]>0:
                        db_exec("INSERT INTO cecl_assumption_log (phase,category,assumption,decision_adopted,rationale,owner) VALUES (%s,%s,%s,%s,%s,%s)",
                                ("Phase 5","Backtesting Calibration","Q-Factor: {} calibration gap".format(r["Segment"]),
                                 "{} bps".format(r["_qbps"]),
                                 "Model predicted {} vs observed {} -- gap of {}".format(r["Predicted PD"],r["Observed PD"],r["Gap"]),
                                 st.session_state.get("username","MRM")))
                db_exec("INSERT INTO cecl_audit_trail (username,category,assumption,old_value,new_value,justification) VALUES (%s,%s,%s,%s,%s,%s)",
                        (st.session_state.get("username","user"),"Model Backtesting","Calibration gaps exported",
                         "","{} segs with gap >20%".format(red_n+amb_n),"Exported to Assumption Log"))
                st.success("Calibration gaps exported to Assumption Log.")

    # '' TAB 4: PSI ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    with tab4:
        st.markdown("### Population Stability Index (PSI)")
        st.markdown("<div style='font-size:12px;color:#555;margin-bottom:10px;'>PSI < 0.10 = stable. 0.10-0.25 = moderate shift. > 0.25 = significant shift.</div>", unsafe_allow_html=True)

        def psi(a, b, bins=10):
            a = pd.to_numeric(a, errors="coerce").dropna()
            b = pd.to_numeric(b, errors="coerce").dropna()
            if len(a)<5 or len(b)<5: return float("nan")
            combined = pd.concat([a,b])
            try:
                edges = pd.qcut(combined, q=bins, duplicates="drop", retbins=True)[1]
            except Exception:
                edges = np.linspace(combined.min(), combined.max()+1e-9, bins+1)
            def pct(s):
                counts = pd.cut(s, bins=edges, include_lowest=True).value_counts().sort_index()
                return (counts/len(s)).clip(lower=0.001)
            pa = pct(a); pb = pct(b)
            idx = pa.index.intersection(pb.index)
            return float(((pa[idx]-pb[idx])*np.log(pa[idx]/pb[idx])).sum()) if len(idx)>0 else float("nan")

        if "institution_id" in df.columns:
            pop_a = df[df["institution_id"].str.contains("A|a", na=False)]
            pop_b = df[df["institution_id"].str.contains("B|b", na=False)]
            split = "Bank A vs Bank B"
        else:
            df["yr2"] = df["vintage_year"].fillna(df["origination_dt"].dt.year.fillna(2019))
            med = df["yr2"].median()
            pop_a = df[df["yr2"]<=med]; pop_b = df[df["yr2"]>med]
            split = "Pre-{:.0f} vs Post-{:.0f} originations".format(med,med)

        st.caption("Split: {} | N(A)={} | N(B)={}".format(split,len(pop_a),len(pop_b)))
        psi_results = []
        for fname, fcol in [("LTV at Origination","ltv_orig"),("DSCR","dscr")]:
            if fcol in df.columns:
                v = psi(pop_a[fcol], pop_b[fcol])
                if not (v!=v):  # not nan
                    status = "Stable" if v<0.10 else ("Moderate Shift" if v<0.25 else "Significant Shift")
                    color  = "#2E7D32" if v<0.10 else ("#E65100" if v<0.25 else "#C62828")
                    psi_results.append({"Feature":fname,"PSI":round(v,4),"Status":status,"_color":color})
        # Property type PSI
        pa_pt = pop_a["property_type"].value_counts(normalize=True).clip(lower=0.001)
        pb_pt = pop_b["property_type"].value_counts(normalize=True).clip(lower=0.001)
        common = pa_pt.index.intersection(pb_pt.index)
        if len(common)>0:
            pt_psi = float(((pa_pt[common]-pb_pt[common])*np.log(pa_pt[common]/pb_pt[common])).sum())
            status = "Stable" if pt_psi<0.10 else ("Moderate Shift" if pt_psi<0.25 else "Significant Shift")
            color  = "#2E7D32" if pt_psi<0.10 else ("#E65100" if pt_psi<0.25 else "#C62828")
            psi_results.append({"Feature":"Property Type Distribution","PSI":round(pt_psi,4),"Status":status,"_color":color})

        for r in psi_results:
            st.markdown("<div style='background:#FFFFFF;border:1px solid #E8EDF5;border-left:5px solid {};border-radius:6px;padding:10px 16px;margin-bottom:5px;display:flex;justify-content:space-between;align-items:center;'>"
                "<span style='font-size:13px;font-weight:700;color:#1F3864;'>{}</span>"
                "<span style='font-size:14px;font-weight:800;color:{};'>{} ' {}</span>"
                "</div>".format(r["_color"],r["Feature"],r["_color"],r["PSI"],r["Status"]),
                unsafe_allow_html=True)
        if not psi_results:
            st.info("PSI requires institution_id or vintage_year fields in the uploaded loan data.")




if __name__ == "__main__":
    if not st.session_state.get("authenticated"):
        login_page()
    else:
        main()


# '' CCAR-CECL BRIDGE '''''''''''''''''''''''''''''''''''''''''''''''''''''''''

# '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# PHASE 4 ' MODEL BUILD SUITE
# '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

