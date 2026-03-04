import json
import time
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh
API = "http://127.0.0.1:8001"  # change if different

st.set_page_config(page_title="Data Analysis Assistant", layout="wide")

st.title("Data Analysis Assistant")

# -------------------------
# Helpers
# -------------------------
def api_post(path, **kwargs):
    r = requests.post(f"{API}{path}", **kwargs)
    if r.status_code >= 400:
        st.error(f"API error {r.status_code}: {r.text}")
        return None
    return r.json()

def api_get(path):
    r = requests.get(f"{API}{path}")
    # Result doesn't exist until workflow runs -> don't treat as error
    if r.status_code == 404 and path.endswith("/result"):
        return None
    if r.status_code >= 400:
        st.error(f"API error {r.status_code}: {r.text}")
        return None
    return r.json()

def render_steps(steps):
    # simple status badge
    for s in steps:
        status = s["status"]
        icon = "⏳"
        if status in ("completed", "success"):
            icon = "✅"
        elif status in ("failed", "error"):
            icon = "❌"
        elif status in ("started", "running"):
            icon = "🔄"

        with st.container(border=True):
            st.write(f"{icon} **{s['name']}**  — `{status}`")
            if s.get("error_text"):
                st.error(s["error_text"])
            # optional: show output json collapsed
            if s.get("output_json"):
                with st.expander("Output"):
                    st.json(s["output_json"])

def should_show_approval(result_res: dict) -> bool:
    if not result_res:
        return False
    # show approval only when pending AND we have suggestions to approve
    return (
        result_res.get("approval_status") == "pending"
        and result_res.get("cleaning_suggestions") is not None
    )

# -------------------------
# Sidebar: create + upload
# -------------------------
st.sidebar.header("Run Controls")

if "run_id" not in st.session_state:
    st.session_state.run_id = None

if st.sidebar.button("➕ Create new run"):
    res = api_post("/runs")
    if res:
        st.session_state.run_id = res["run_id"]

run_id = st.sidebar.text_input("Run ID", value=st.session_state.run_id or "")
if run_id:
    st.session_state.run_id = run_id

uploaded = st.sidebar.file_uploader("Upload CSV/XLSX", type=["csv", "xlsx", "xls", "xlsm"])

if st.sidebar.button("⬆️ Upload to this run"):
    if not st.session_state.run_id:
        st.warning("Create/select a run first.")
    elif not uploaded:
        st.warning("Choose a file.")
    else:
        files = {"file": (uploaded.name, uploaded.getvalue())}
        res = api_post(f"/runs/{st.session_state.run_id}/upload", files=files)
        if res:
            st.sidebar.success("Uploaded!")

colA, colB, colC = st.columns([1.2, 1.2, 2])

with colA:
    st.subheader("Start / Resume")
    if st.button("▶️ Start workflow"):
        if not st.session_state.run_id:
            st.warning("Create/select a run first.")
        else:
            res = api_post(f"/runs/{st.session_state.run_id}/start")
            if res:
                st.success(f"Started: {res.get('status')}")

with colB:
    st.subheader("Live Steps")
    auto_refresh = st.toggle("Auto refresh (2s)", value=True)

with colC:
    st.subheader("Run Status + Results")

# -------------------------
# Main: show run details
# -------------------------
if not st.session_state.run_id:
    st.info("Create a run from the left sidebar to begin.")
    st.stop()

# Polling
if auto_refresh:
    time.sleep(0.4)  # small delay to reduce spam

steps_res = api_get(f"/runs/{st.session_state.run_id}/steps")
result_res = api_get(f"/runs/{st.session_state.run_id}/result")

def is_finished(result_res):
    if not result_res:
        return False

    approval = result_res.get("approval_status")
    stop_reason = result_res.get("stop_reason")
    report_path = result_res.get("report_path")
    cleaned_path = result_res.get("cleaned_path")
    charts = result_res.get("charts") or []
    errors = result_res.get("errors") or []

    # If waiting for approval, keep polling
    # Stop refreshing if pending but LLM quota error
    if approval == "pending" and errors:
        return True

    if approval == "pending":
        return False

    # Consider done if report exists OR stop_reason exists (and not pending) OR outputs exist
    if report_path:
        return True
    if stop_reason and approval != "pending":
        return True
    if cleaned_path or charts:
        return True

    # If errors exist and not pending, stop polling
    if errors and approval != "pending":
        return True

    return False


finished = is_finished(result_res)

# ✅ Auto refresh every 2 sec, but only while not finished
if auto_refresh and not finished:
    st_autorefresh(interval=2000, key="poller")

if steps_res:
    render_steps(steps_res["steps"])

if result_res:
    # Top summary
    st.write("### Workflow Result")
    if result_res.get("stop_reason"):
        st.info(f"Stop reason: **{result_res['stop_reason']}**")
    if result_res.get("errors"):
        st.error("Errors:")
        for e in result_res["errors"]:
            st.write(f"- {e}")

    # Profile + Issues
    left, right = st.columns(2)
    with left:
        st.write("#### Dataset Profile")
        st.json(result_res.get("df_profile") or {})
    with right:
        st.write("#### Quality Issues")
        st.json(result_res.get("quality_issues") or [])

    # -------------------------
    # Approval
    # -------------------------
    st.write("### Approval")

    approval = result_res.get("approval_status")
    suggestions = result_res.get("cleaning_suggestions")
    errors = result_res.get("errors") or []

    st.write(f"Approval status: **{approval}**")

    # Case 1: pending but suggestions missing (LLM failed / quota exceeded)
    if approval == "pending" and not suggestions:
        st.warning("Cleaning suggestions unavailable (LLM failed or quota exceeded).")

        if errors:
            with st.expander("See error"):
                for e in errors:
                    st.write(f"- {e}")

        st.info("Wait for quota reset or restart workflow later.")

    # Case 2: normal approval flow
    elif approval == "pending" and suggestions:
        st.write("#### Cleaning Suggestions (editable)")

        edited = st.text_area(
            "Approved config (JSON)",
            value=json.dumps(suggestions, indent=2),
            height=260,
            key="approved_config_editor"
        )

        approve_col, reject_col = st.columns(2)

        with approve_col:
            if st.button("✅ Approve & Resume", key="approve_btn"):
                try:
                    payload = {"status": "approved", "approved_config": json.loads(edited)}
                except Exception:
                    st.error("Invalid JSON.")
                    payload = None

                if payload:
                    res = api_post(f"/runs/{st.session_state.run_id}/approve", json=payload)
                    if res:
                        st.success("Workflow resumed.")
                        st.rerun()

        with reject_col:
            if st.button("❌ Reject", key="reject_btn"):
                payload = {"status": "rejected"}
                res = api_post(f"/runs/{st.session_state.run_id}/approve", json=payload)
                if res:
                    st.warning("Rejected.")
                    st.rerun()

    else:
        st.success("No approval action needed.")

    # Charts + Report
    st.write("### Outputs")
    charts = result_res.get("charts") or []
    if charts:
        for ch in charts:
            st.write(f"- {ch.get('name')}")
            path = ch.get("path")
            if path:
                # If paths are local, easiest is to serve them via FastAPI /files endpoint.
                st.code(path)
    else:
        st.write("No charts yet.")

    if result_res.get("report_path"):
        st.write("Report path:")
        st.code(result_res["report_path"])


