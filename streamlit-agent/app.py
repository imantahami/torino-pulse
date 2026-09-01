import os
import re
import json
import requests
import streamlit as st
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv("/home/ubuntu/streamlit-agent/.env")

# ── Config ──────────────────────────────────────────────
METABASE_URL = os.getenv("METABASE_URL")
USERNAME     = os.getenv("METABASE_USERNAME")
PASSWORD     = os.getenv("METABASE_PASSWORD")
DATABASE_ID  = int(os.getenv("METABASE_DATABASE_ID", "2"))
NVIDIA_KEY   = os.getenv("NVIDIA_API_KEY")

# ── Metabase helpers ─────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def get_token() -> str:
    r = requests.post(f"{METABASE_URL}/api/session",
                      json={"username": USERNAME, "password": PASSWORD},
                      timeout=10)
    r.raise_for_status()
    return r.json()["id"]

def mb_headers() -> dict:
    return {"X-Metabase-Session": get_token(),
            "Content-Type": "application/json"}

@st.cache_data(ttl=3600, show_spinner=False)
def get_schema() -> str:
    r = requests.get(f"{METABASE_URL}/api/database/{DATABASE_ID}/metadata",
                     headers=mb_headers(), timeout=15)
    r.raise_for_status()
    meta = r.json()

    USEFUL_TABLES = {
        'fct_route_performance', 'fct_hourly_service', 'fct_alert_impact',
        'stg_vehicle_positions', 'stg_trip_updates', 'stg_alerts',
        'stg_weather', 'stg_routes', 'stg_stops',
    }

    lines = ["Available tables and columns:\n"]
    for t in meta.get("tables", []):
        if t.get("name") in USEFUL_TABLES:
            cols = [f["name"] for f in t.get("fields", [])]
            if cols:
                lines.append(f"  {t['name']}: {', '.join(cols)}")
    return "\n".join(lines)

def get_all_cards():
    r = requests.get(f"{METABASE_URL}/api/card",
                     headers=mb_headers(), timeout=15)
    r.raise_for_status()
    return [{"id": c["id"], "name": c["name"]} for c in r.json()]

def create_chart(name: str, sql: str, display: str):
    payload = {
        "name": name,
        "dataset_query": {
            "database": DATABASE_ID,
            "native": {"query": sql},
            "type": "native"
        },
        "display": display,
        "visualization_settings": {}
    }
    r = requests.post(f"{METABASE_URL}/api/card",
                      headers=mb_headers(), json=payload, timeout=15)
    if r.status_code == 200:
        return True, r.json()["id"], None
    return False, None, r.text

def create_dashboard(name: str, card_ids: list[int]) -> tuple[bool, str | None]:
    # 1. ساخت داشبورد
    r = requests.post(f"{METABASE_URL}/api/dashboard",
                      headers=mb_headers(),
                      json={"name": name}, timeout=15)
    if r.status_code != 200:
        return False, r.text
    dashboard_id = r.json()["id"]

    # 2. اضافه کردن نمودارها با layout خودکار
    cards_payload = []
    col = 0
    row = 0
    for i, card_id in enumerate(card_ids):
        cards_payload.append({
            "id": -(i + 1),  # temp id
            "card_id": card_id,
            "row": row,
            "col": col,
            "size_x": 9,
            "size_y": 6,
            "visualization_settings": {},
            "parameter_mappings": []
        })
        col += 9
        if col >= 18:
            col = 0
            row += 6

    r = requests.put(
        f"{METABASE_URL}/api/dashboard/{dashboard_id}/cards",
        headers=mb_headers(),
        json={"cards": cards_payload},
        timeout=15
    )
    if r.status_code != 200:
        return False, r.text

    return True, f"{METABASE_URL}/dashboard/{dashboard_id}"

# ── AI ───────────────────────────────────────────────────
SYSTEM_PROMPT = """\
You are an expert PostgreSQL analyst for Turin's public transit system (GTT).
Given the database schema below, generate a Metabase-compatible SQL query.

{schema}

Key facts:
- negative arrival_delay_seconds = EARLY, positive = LATE
- use fetched_at_local for Turin local time
- fct_route_performance is pre-aggregated: use columns directly, no AVG/SUM
- fct_hourly_service has fleet size + delay + weather per hour
- fct_alert_impact has one row per (route, alert)

Rules:
- Use ONLY exact table and column names from the schema.
- Output ONLY raw JSON — no markdown, no extra text.
- JSON: {{"chart_name": "...", "sql_query": "...", "display_type": "bar|line|area|pie|scatter|table|scalar|row"}}
- For fct_route_performance: SELECT route_short_name, median_delay_seconds FROM fct_route_performance (no re-aggregation)
"""

def ask_ai(user_request: str, schema: str) -> dict:
    client = OpenAI(
        base_url="https://integrate.api.nvidia.com/v1",
        api_key=NVIDIA_KEY,
        timeout=120.0
    )
    resp = client.chat.completions.create(
        model="meta/llama-3.1-8b-instruct",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT.format(schema=schema)},
            {"role": "user",   "content": user_request}
        ],
        temperature=0.1,
        max_tokens=1024,
    )
    raw = resp.choices[0].message.content.strip()
    clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    return json.loads(clean)

# ── UI ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Torino Pulse · Chart Builder",
    page_icon="🚌",
    layout="centered"
)

st.title("🚌 Torino Pulse — AI Chart Builder")
st.caption("Describe what you want to see. The AI writes the SQL and creates the chart in Metabase.")

# Sidebar
with st.sidebar:
    st.header("📋 Database schema")
    try:
        schema = get_schema()
        st.code(schema, language="text")
        st.success("Connected ✓")
    except Exception as e:
        st.error(f"Could not load schema: {e}")
        schema = None

# Tabs
tab1, tab2 = st.tabs(["✨ Create Chart", "📊 Create Dashboard"])

# ── Tab 1: Chart Builder ──────────────────────────────────
with tab1:
    with st.expander("💡 Example prompts"):
        st.markdown("""
- Top 10 routes with highest median delay
- How does fleet size change throughout the day?
- Which routes have the most active alerts?
- Show delay vs percentage late for each route
- Tram vs bus: compare average delay and punctuality
- Routes with more than 20% late arrivals
        """)

    request = st.text_area(
        "What would you like to visualise?",
        height=120,
        placeholder="e.g. Show the 10 routes with the worst median delay"
    )

    if st.button("✨ Generate & create chart", type="primary", use_container_width=True):
        if not request.strip():
            st.warning("Please describe the chart you want.")
            st.stop()
        if schema is None:
            st.error("Cannot reach the database. Check the sidebar.")
            st.stop()

        with st.spinner("Thinking…"):
            try:
                result = ask_ai(request, schema)
            except json.JSONDecodeError:
                st.error("The AI returned an unexpected format. Try rephrasing.")
                st.stop()
            except Exception as e:
                st.error(f"AI error: {e}")
                st.stop()

        st.subheader(result["chart_name"])
        col1, col2 = st.columns(2)
        col1.metric("Chart type", result["display_type"])

        with st.expander("Generated SQL"):
            st.code(result["sql_query"], language="sql")

        with st.spinner("Creating chart in Metabase…"):
            ok, card_id, err = create_chart(
                result["chart_name"],
                result["sql_query"],
                result["display_type"]
            )

        if ok:
            st.success("Chart created!")
            st.link_button("Open in Metabase →", f"{METABASE_URL}/question/{card_id}")
        else:
            st.error(f"Metabase error: {err}")

# ── Tab 2: Dashboard Builder ──────────────────────────────
with tab2:
    st.subheader("📊 Create Dashboard from Existing Charts")
    st.caption("Select charts and give your dashboard a name.")

    dashboard_name = st.text_input(
        "Dashboard name",
        value="Torino Pulse — Transit Analytics"
    )

    with st.spinner("Loading charts…"):
        try:
            all_cards = get_all_cards()
        except Exception as e:
            st.error(f"Could not load charts: {e}")
            all_cards = []

    if all_cards:
        card_options = {c["name"]: c["id"] for c in all_cards}
        selected = st.multiselect(
            "Select charts to include:",
            options=list(card_options.keys()),
            default=list(card_options.keys())[:5] if len(all_cards) >= 5 else list(card_options.keys())
        )

        if st.button("📊 Create Dashboard", type="primary", use_container_width=True):
            if not selected:
                st.warning("Please select at least one chart.")
                st.stop()
            if not dashboard_name.strip():
                st.warning("Please enter a dashboard name.")
                st.stop()

            selected_ids = [card_options[name] for name in selected]

            with st.spinner("Creating dashboard…"):
                ok, result = create_dashboard(dashboard_name, selected_ids)

            if ok:
                st.success("Dashboard created!")
                st.link_button("Open Dashboard →", result)
            else:
                st.error(f"Error: {result}")
    else:
        st.info("No charts found. Create some charts first in the 'Create Chart' tab.")
