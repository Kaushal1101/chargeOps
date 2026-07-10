"""LogiShield operations dashboard — read-only fleet state from the Operational API.

Run with:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pydeck as pdk
import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
REFRESH_INTERVAL_SECONDS = 5
MAX_FLEET_SIZE = 8877
ALERT_FEED_LIMIT = 50

TIER_SORT_ORDER = {"RED": 0, "YELLOW": 1}
CHARGER_COLUMNS = [
    "charger_id",
    "tier",
    "connector_type",
    "user_tier",
    "session_state",
    "session_progress",
    "estimated_completion_minutes",
    "session_buffer",
    "avg_temperature",
    "reason",
    "last_update",
]

st.set_page_config(page_title="LogiShield Operations", layout="wide")

# --- Initialise session state ---
if "alert_feed" not in st.session_state:
    st.session_state.alert_feed: list[dict] = []
if "prev_tiers" not in st.session_state:
    st.session_state.prev_tiers: dict[str, str] = {}

# --- Sidebar: Controls ---
with st.sidebar:
    st.header("Fleet Controls")

    try:
        config_resp = requests.get(f"{API_BASE_URL}/config", timeout=3)
        current_size = config_resp.json().get("network_size") if config_resp.ok else None
    except requests.RequestException:
        current_size = None

    new_size = st.number_input(
        "Fleet size",
        min_value=1,
        max_value=MAX_FLEET_SIZE,
        value=current_size or 20,
        step=10,
    )
    if st.button("Apply", use_container_width=True):
        try:
            resp = requests.post(
                f"{API_BASE_URL}/config/fleet-size",
                json={"size": new_size},
                timeout=3,
            )
            if resp.ok:
                st.success(f"Fleet size set to {new_size}. Simulator reloads within 5s.")
            else:
                st.error("Failed to update fleet size.")
        except requests.RequestException as e:
            st.error(f"API error: {e}")

    st.divider()
    st.header("Live Alerts")

    feed_placeholder = st.empty()

# --- Fetch data ---
try:
    _health_resp = requests.get(f"{API_BASE_URL}/health", timeout=3)
    _health_resp.raise_for_status()
except requests.RequestException as exc:
    st.error(f"API not reachable at {API_BASE_URL}: {exc}")
    st.stop()

health_data = _health_resp.json()
summary_data = requests.get(f"{API_BASE_URL}/summary").json()
chargers_data = requests.get(f"{API_BASE_URL}/chargers").json()
network_stats = requests.get(f"{API_BASE_URL}/stats/network").json()
region_stats = requests.get(f"{API_BASE_URL}/stats/regions").json()
connector_stats = requests.get(f"{API_BASE_URL}/stats/connectors").json()

# --- Update alert feed from tier transitions ---
now_ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
current_tiers = {rec["charger_id"]: rec["tier"] for rec in chargers_data if rec}
new_alerts = []
for charger_id, tier in current_tiers.items():
    prev = st.session_state.prev_tiers.get(charger_id)
    if prev != tier:
        color = "🔴" if tier == "RED" else "🟡"
        new_alerts.append({"ts": now_ts, "label": f"{color} {charger_id} → {tier}"})
st.session_state.alert_feed = (new_alerts + st.session_state.alert_feed)[:ALERT_FEED_LIMIT]
st.session_state.prev_tiers = current_tiers

# --- Render alert feed ---
with feed_placeholder:
    if not st.session_state.alert_feed:
        st.caption("Waiting for alerts...")
    else:
        for entry in st.session_state.alert_feed:
            st.caption(f"`{entry['ts']}` {entry['label']}")

# --- Section 1: System Health ---
st.header("System Health")

st.write("API: Online")

if health_data["pipeline_active"]:
    st.write("Pipeline: Active")
    st.write(f"Last update: {health_data['last_update_ts']}")
else:
    st.write(
        f"Pipeline: Stalled — last update: {health_data['last_update_ts'] or 'never'}"
    )

st.caption(
    "Kafka and Spark health are inferred from data freshness. "
    "A stale timestamp indicates the upstream pipeline may have stopped."
)

# --- Section 1.5: Network Statistics ---
st.header("Network Statistics")

if not network_stats:
    st.caption("No statistics available yet — waiting for Spark stats sink.")
else:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active Sessions", network_stats.get("total_active_sessions", "—"))
    col2.metric("Avg Temperature", f"{network_stats.get('avg_temperature', '—')} °C")
    col3.metric("Avg Session Buffer", f"{network_stats.get('avg_session_buffer', '—')} min")
    col4.metric("Avg Power Output", f"{network_stats.get('avg_power_kw', '—')} kW")

    region_rows = []
    for rec in sorted(region_stats, key=lambda r: r.get("region", "")):
        region_rows.append({
            "region": rec.get("region", ""),
            "active_sessions": rec.get("active_sessions", ""),
            "avg_temperature": rec.get("avg_temperature", ""),
            "avg_session_progress": f"{round(float(rec.get('avg_session_progress', 0)) * 100)}%" if rec.get("avg_session_progress") else "",
            "avg_session_buffer": rec.get("avg_session_buffer", ""),
            "avg_power_kw": rec.get("avg_power_kw", ""),
        })

    if region_rows:
        st.subheader("By Region")
        st.dataframe(region_rows, use_container_width=True)

    connector_rows = []
    for rec in sorted(connector_stats, key=lambda r: r.get("connector_type", "")):
        connector_rows.append({
            "connector_type": rec.get("connector_type", ""),
            "active_sessions": rec.get("active_sessions", ""),
            "avg_power_kw": rec.get("avg_power_kw", ""),
            "avg_rated_power_kw": rec.get("avg_rated_power_kw", ""),
            "avg_utilization_pct": f"{rec.get('avg_utilization_pct', '')}%",
            "avg_energy_kwh": rec.get("avg_energy_kwh", ""),
        })

    if connector_rows:
        st.subheader("By Connector Type")
        st.dataframe(connector_rows, use_container_width=True)

# --- Section 2: Network Summary ---
st.header("Network Summary")

red_count = summary_data["red_alerts"]
yellow_count = summary_data["yellow_alerts"]

col_red, col_yellow = st.columns(2)
col_red.metric("RED Alerts", red_count)
col_yellow.metric("YELLOW Alerts", yellow_count)

# --- Section 2.5: Alert Map ---
st.header("Alert Map")

map_rows = []
for record in chargers_data:
    if not record:
        continue
    try:
        lat = float(record.get("charger_lat", 0))
        lng = float(record.get("charger_lng", 0))
    except (ValueError, TypeError):
        continue
    if lat == 0 or lng == 0:
        continue
    tier = record.get("tier", "")
    if tier not in ("RED", "YELLOW"):
        continue
    color = [220, 38, 38] if tier == "RED" else [234, 179, 8]
    map_rows.append({
        "lat": lat,
        "lng": lng,
        "tier": tier,
        "charger_id": record.get("charger_id", ""),
        "site_id": record.get("site_id", ""),
        "reason": record.get("reason", ""),
        "color": color,
    })

if not map_rows:
    st.caption("No active alerts to display on map.")
else:
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_rows,
        get_position="[lng, lat]",
        get_fill_color="color",
        get_radius=300,
        pickable=True,
    )
    view_state = pdk.ViewState(latitude=1.352, longitude=103.820, zoom=11)
    tooltip = {
        "html": "<b>{charger_id}</b><br/>{site_id}<br/>Tier: {tier}<br/>{reason}",
        "style": {"backgroundColor": "#1e1e1e", "color": "white", "fontSize": "12px"},
    }
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))

# --- Section 3: Active Chargers ---
st.header("Active Chargers")


def _fmt_session_progress(value: str) -> str:
    try:
        return f"{float(value) * 100:.0f}%"
    except (ValueError, TypeError):
        return value


trucks: list[dict[str, str]] = []
for record in chargers_data:
    if record:
        row = {col: record.get(col, "") for col in CHARGER_COLUMNS}
        row["session_progress"] = _fmt_session_progress(row["session_progress"])
        trucks.append(row)

if not trucks:
    st.write("No active alerts.")
else:
    trucks.sort(
        key=lambda row: (
            TIER_SORT_ORDER.get(row.get("tier", ""), 99),
            row.get("charger_id", ""),
        )
    )
    st.dataframe(trucks, column_order=CHARGER_COLUMNS, use_container_width=True)

# --- Section 4: Charger Lookup ---
st.header("Charger Lookup")

charger_id = st.text_input("Charger ID")
if charger_id:
    charger_id = charger_id.strip().upper()
    resp = requests.get(f"{API_BASE_URL}/chargers/{charger_id}")
    data = resp.json()
    tier = data.get("tier", "")
    if tier == "GREEN":
        st.success(f"{charger_id} — GREEN ({data.get('session_state', '')})")
    elif tier == "INACTIVE":
        st.info(f"{charger_id} — INACTIVE (not in current fleet or between sessions)")
    else:
        st.json(data)

time.sleep(REFRESH_INTERVAL_SECONDS)
st.rerun()
