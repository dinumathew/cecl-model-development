"""
patch_fixes.py
==============
Run this LOCALLY on your Windows machine using Anaconda Prompt.

It downloads the current app.py from GitHub, applies the 3 targeted fixes,
and pushes it back. This avoids replacing the entire file.

Usage:
    cd C:\Users\dinum\Downloads
    pip install requests
    python patch_fixes.py

You need your GitHub Personal Access Token (ghp_...).
"""

import requests, base64, re, sys

GITHUB_TOKEN = "ghp_YOUR_TOKEN_HERE"   # <-- paste your token
REPO_OWNER   = "dinumathew"
REPO_NAME    = "cecl-model-development"
FILE_PATH    = "app.py"
BRANCH       = "main"

if GITHUB_TOKEN == "ghp_YOUR_TOKEN_HERE":
    print("ERROR: Set your GITHUB_TOKEN at the top of this script.")
    sys.exit(1)

api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ── 1. Download current file from GitHub ─────────────────────────────────────
print("Downloading current app.py from GitHub...")
r = requests.get(api_url, headers=headers, params={"ref": BRANCH})
if r.status_code != 200:
    print(f"ERROR: {r.status_code} — {r.text[:200]}")
    sys.exit(1)

file_data = r.json()
sha       = file_data["sha"]
content   = base64.b64decode(file_data["content"]).decode("utf-8", errors="replace")
lines_before = content.count("\n")
print(f"Downloaded: {len(content):,} bytes | {lines_before:,} lines | SHA: {sha[:12]}")

if lines_before < 5000:
    print("WARNING: File appears truncated (fewer than 5000 lines). Aborting to be safe.")
    print("The file on GitHub may already be broken. Check github.com manually first.")
    sys.exit(1)

# ── 2. Apply the three targeted fixes ────────────────────────────────────────
original = content
fixes_applied = []

# FIX a) Monotonicity warning - yellow on yellow background
old_a = 'st.warning("{} monotonicity violation(s). Acceptable for small portfolio. Document in Model Card.".format(violations))'
new_a = ('st.markdown("<div style=\'background:#FFF3E0;border-left:4px solid #E65100;'
         'border-radius:6px;padding:8px 14px;font-size:12px;color:#5C2D00;\'>"'
         '"{} monotonicity violation(s). Acceptable for small portfolio — document in Model Card.'
         '</div>".format(violations), unsafe_allow_html=True)')
if old_a in content:
    content = content.replace(old_a, new_a)
    fixes_applied.append("a) Monotonicity warning colour fixed")
else:
    fixes_applied.append("a) Already fixed or not found - skipped")

# FIX b) np.trapz -> compatible with NumPy 2.0
old_b = "auc_val = float(np.trapz(tpr_pts, fpr_pts))"
new_b = "auc_val = float(np.trapz(tpr_pts, fpr_pts) if hasattr(np,'trapz') else np.trapezoid(tpr_pts, fpr_pts))"
if old_b in content:
    content = content.replace(old_b, new_b)
    fixes_applied.append("b) np.trapz fixed for NumPy 2.0")
else:
    fixes_applied.append("b) np.trapz already fixed or not found - skipped")

# FIX c) Add L1 logistic + SVM to PD model workshop
old_c = '''    MODELS = {
        "Logistic Regression (L2)": LogisticRegression(C=0.1, max_iter=1000, class_weight="balanced", random_state=42),
        "Decision Tree":            DecisionTreeClassifier(max_depth=4, class_weight="balanced", random_state=42),
        "Random Forest":            RandomForestClassifier(n_estimators=100, max_depth=5, class_weight="balanced", random_state=42, oob_score=True),
        "Gradient Boosting":        GradientBoostingClassifier(n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42),
    }'''
new_c = '''    MODELS = {
        "Logistic Regression (L2)":   LogisticRegression(C=0.1, max_iter=1000, class_weight="balanced", random_state=42),
        "Logistic Regression (L1)":   LogisticRegression(C=0.1, penalty="l1", solver="liblinear", class_weight="balanced", random_state=42),
        "Decision Tree":              DecisionTreeClassifier(max_depth=4, class_weight="balanced", random_state=42),
        "Random Forest":              RandomForestClassifier(n_estimators=100, max_depth=5, class_weight="balanced", random_state=42, oob_score=True),
        "Gradient Boosting":          GradientBoostingClassifier(n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42),
    }
    try:
        from sklearn.svm import SVC as _SVC
        MODELS["SVM (RBF kernel)"] = _SVC(kernel="rbf", class_weight="balanced", probability=True, random_state=42)
    except Exception:
        pass'''
if old_c in content:
    content = content.replace(old_c, new_c)
    fixes_applied.append("c) 6 models (added L1 + SVM)")
else:
    fixes_applied.append("c) Model dict not found in expected form - skipped")

# ── 3. Report ─────────────────────────────────────────────────────────────────
print("\nFixes applied:")
for f in fixes_applied:
    print(" ", f)

lines_after = content.count("\n")
print(f"\nBefore: {lines_before:,} lines | After: {lines_after:,} lines")

if content == original:
    print("\nNo changes made. File is already up to date.")
    sys.exit(0)

# ── 4. Push back to GitHub ────────────────────────────────────────────────────
print("\nPushing patched file to GitHub...")
payload = {
    "message": "Apply 3 targeted fixes: warning colour, trapz, 6 models",
    "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
    "sha":     sha,
    "branch":  BRANCH,
}
r2 = requests.put(api_url, headers=headers, json=payload)
if r2.status_code in (200, 201):
    new_sha = r2.json()["content"]["sha"]
    print(f"\nSUCCESS — pushed to GitHub")
    print(f"New SHA: {new_sha[:12]}")
    print(f"Lines:   {lines_after:,}")
    print("\nStreamlit will redeploy in ~60 seconds.")
    print("Then reboot via Manage app > Reboot app.")
else:
    print(f"ERROR: {r2.status_code}")
    print(r2.text[:400])
