import streamlit as st

st.set_page_config(page_title="Model Approval Workflow", layout="wide")

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet" />
<style>
  :root {
    --navy: #0d2b55;
    --navy-mid: #163c6e;
    --navy-light: #1e4f8c;
    --green-bg: #eaf4ee;
    --green-badge: #d4edda;
    --green-text: #1a6b3a;
    --white: #ffffff;
    --text-muted: #6c757d;
    --border: #d9e8d9;
    --bg: #f4f8f4;
    --pending-bg: #f0f4ff;
    --pending-border: #c0d0f0;
    --pending-text: #3355aa;
    --rejected-bg: #fff0f0;
    --rejected-text: #cc2222;
  }

  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif !important; }
  #MainMenu, footer, header { visibility: hidden; }
  .block-container { padding: 2rem 2.5rem !important; max-width: 1200px; }

  h2.workflow-title {
    font-size: 1.1rem; font-weight: 700;
    letter-spacing: 0.04em; text-transform: uppercase;
    color: var(--navy); margin-bottom: 20px;
  }

  .section-divider {
    display: flex; align-items: center; gap: 12px; margin: 28px 0 16px;
  }
  .section-divider span {
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.1em; color: var(--navy); white-space: nowrap;
  }
  .section-divider::before, .section-divider::after {
    content: ''; flex: 1; height: 1px; background: var(--border);
  }

  .steps-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 16px; margin-bottom: 24px;
  }
  .step-card {
    background: var(--green-bg); border: 1.5px solid var(--border);
    border-radius: 14px; padding: 28px 20px 22px; text-align: center;
    position: relative; transition: transform 0.15s, box-shadow 0.15s;
  }
  .step-card:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(13,43,85,0.1); }
  .step-card.pending { background: var(--pending-bg); border-color: var(--pending-border); }
  .step-card.rejected { background: var(--rejected-bg); border-color: #f0c0c0; }

  .step-info-icon {
    position: absolute; top: 12px; left: 50%; transform: translateX(-50%);
    width: 20px; height: 20px; border-radius: 50%; border: 1.5px solid currentColor;
    font-size: 11px; font-weight: 700; display: flex;
    align-items: center; justify-content: center; color: var(--green-text); cursor: pointer;
  }
  .step-card.pending .step-info-icon { color: var(--pending-text); }

  .step-number {
    font-size: 2.2rem; font-weight: 700; color: var(--green-text);
    margin: 16px 0 4px; font-family: 'DM Mono', monospace;
  }
  .step-card.pending .step-number  { color: var(--pending-text); }
  .step-card.rejected .step-number { color: var(--rejected-text); }

  .step-status {
    font-size: 0.7rem; font-weight: 700; letter-spacing: 0.12em;
    text-transform: uppercase; color: var(--green-text); margin-bottom: 6px;
  }
  .step-card.pending .step-status  { color: var(--pending-text); }
  .step-card.rejected .step-status { color: var(--rejected-text); }
  .step-label { font-size: 0.82rem; color: var(--text-muted); font-weight: 500; }

  /* Override Streamlit buttons */
  div[data-testid="stButton"] > button {
    background-color: #0d2b55 !important;
    color: white !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 10px 28px !important;
    font-weight: 600 !important;
    font-size: 0.95rem !important;
    box-shadow: 0 4px 14px rgba(13,43,85,0.25) !important;
    width: 100% !important;
  }
  div[data-testid="stButton"] > button:hover {
    background-color: #1e4f8c !important;
    box-shadow: 0 6px 20px rgba(13,43,85,0.35) !important;
  }

  .timeline-section {
    background: var(--white); border-radius: 16px; overflow: hidden;
    box-shadow: 0 2px 16px rgba(13,43,85,0.08); margin-bottom: 32px;
  }
  .timeline-header {
    background: var(--navy); color: var(--white); padding: 16px 24px;
    font-size: 0.85rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
  }
  table { width: 100%; border-collapse: collapse; }
  thead tr { background: var(--navy-mid); }
  thead th {
    color: var(--white); font-size: 0.78rem; font-weight: 600;
    letter-spacing: 0.06em; text-transform: uppercase; padding: 14px 20px; text-align: left;
  }
  tbody tr { border-bottom: 1px solid #e8eef8; transition: background 0.12s; }
  tbody tr:nth-child(even) { background: #f7f9fd; }
  tbody tr:nth-child(odd)  { background: var(--white); }
  tbody tr:hover           { background: #eef2fb; }
  tbody td { padding: 13px 20px; font-size: 0.85rem; color: #2a3a4a; font-weight: 400; }

  .badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
  }
  .badge-approved { background: var(--green-badge); color: var(--green-text); }
  .badge-pending  { background: #dce8ff; color: #2244aa; }
  .badge-rejected { background: #ffe0e0; color: #bb2222; }
</style>
""", unsafe_allow_html=True)

# ── TITLE ──
st.markdown('<h2 class="workflow-title">Model Validation Workflow</h2>', unsafe_allow_html=True)

# ── STEP GROUP 1 ──
st.markdown("""
<div class="section-divider"><span>Step 1 — Initial Review</span></div>
<div class="steps-grid">
  <div class="step-card">
    <div class="step-info-icon">i</div>
    <div class="step-number">1</div>
    <div class="step-status">Approved</div>
    <div class="step-label">Data Quality</div>
  </div>
  <div class="step-card">
    <div class="step-info-icon">i</div>
    <div class="step-number">2</div>
    <div class="step-status">Approved</div>
    <div class="step-label">PD/LGD Model</div>
  </div>
  <div class="step-card">
    <div class="step-info-icon">i</div>
    <div class="step-number">3</div>
    <div class="step-status">Approved</div>
    <div class="step-label">Anomaly Detection</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── STEP GROUP 2 ──
st.markdown("""
<div class="section-divider"><span>Step 2 — Technical Validation</span></div>
<div class="steps-grid">
  <div class="step-card pending">
    <div class="step-info-icon">i</div>
    <div class="step-number">4</div>
    <div class="step-status">Pending</div>
    <div class="step-label">Backtesting</div>
  </div>
  <div class="step-card pending">
    <div class="step-info-icon">i</div>
    <div class="step-number">5</div>
    <div class="step-status">Pending</div>
    <div class="step-label">Stress Testing</div>
  </div>
  <div class="step-card pending">
    <div class="step-info-icon">i</div>
    <div class="step-number">6</div>
    <div class="step-status">Pending</div>
    <div class="step-label">Model Governance</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── BUTTONS ──
col1, col2, col3 = st.columns([1, 1, 6])
with col1:
    st.button("Restart", key="restart")
with col2:
    st.button("Stop Process", key="stop")

# ── TIMELINE ──
st.markdown("""
<div class="timeline-section">
  <div class="timeline-header">Model Timeline</div>
  <table>
    <thead>
      <tr>
        <th>Date</th><th>Model</th><th>Version</th>
        <th>Stage</th><th>Reviewer</th><th>Status</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>2024-01-15</td><td>PD/LGD Model</td><td>v2.3.1</td>
        <td>Data Quality</td><td>A. Kumar</td>
        <td><span class="badge badge-approved">Approved</span></td>
      </tr>
      <tr>
        <td>2024-01-18</td><td>Anomaly Detection</td><td>v1.0.4</td>
        <td>PD/LGD Model</td><td>S. Patel</td>
        <td><span class="badge badge-approved">Approved</span></td>
      </tr>
      <tr>
        <td>2024-01-22</td><td>Credit Risk Model</td><td>v3.1.0</td>
        <td>Anomaly Detection</td><td>R. Nair</td>
        <td><span class="badge badge-approved">Approved</span></td>
      </tr>
      <tr>
        <td>2024-02-01</td><td>PD/LGD Model</td><td>v2.4.0</td>
        <td>Backtesting</td><td>A. Kumar</td>
        <td><span class="badge badge-pending">Pending</span></td>
      </tr>
      <tr>
        <td>2024-02-05</td><td>Anomaly Detection</td><td>v1.1.0</td>
        <td>Stress Testing</td><td>M. Iyer</td>
        <td><span class="badge badge-pending">Pending</span></td>
      </tr>
      <tr>
        <td>2024-02-10</td><td>Market Risk Model</td><td>v4.0.2</td>
        <td>Data Quality</td><td>V. Menon</td>
        <td><span class="badge badge-rejected">Rejected</span></td>
      </tr>
    </tbody>
  </table>
</div>
""", unsafe_allow_html=True)
