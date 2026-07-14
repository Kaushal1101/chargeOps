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


def _fmt_ts(iso_str: str | None, mode: str = "relative") -> str:
    if not iso_str:
        return "never"
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if mode == "relative":
            delta = int((datetime.now(timezone.utc) - dt).total_seconds())
            if delta < 60:
                return f"{delta}s ago"
            if delta < 3600:
                return f"{delta // 60}m {delta % 60}s ago"
            return f"{delta // 3600}h ago"
        return dt.strftime("%H:%M:%S UTC")
    except Exception:
        return iso_str


st.set_page_config(page_title="LogiShield Operations", layout="wide")

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

# --- Section 1: System Health ---
st.header("System Health")
st.divider()

if health_data["pipeline_active"]:
    st.success(
        f"API Online  |  Pipeline Active  |  Updated "
        f"{_fmt_ts(health_data['last_update_ts'])}"
    )
else:
    st.error(
        f"Pipeline Stalled — last update: {_fmt_ts(health_data['last_update_ts'])}"
    )

st.caption(
    "Kafka and Spark health are inferred from data freshness. "
    "A stale timestamp indicates the upstream pipeline may have stopped."
)

# --- Section 1.5: Network Statistics ---
st.header("Network Statistics")
st.divider()

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
st.divider()

red_count = summary_data["red_alerts"]
yellow_count = summary_data["yellow_alerts"]

col_red, col_yellow = st.columns(2)
col_red.metric("RED Alerts", red_count)
col_yellow.metric("YELLOW Alerts", yellow_count)

# --- Section 2.5: Alert Map ---
st.header("Alert Map")
st.divider()

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
st.divider()


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
        row["last_update"] = _fmt_ts(row.get("last_update"), mode="relative")
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
    st.dataframe(
        trucks,
        column_order=CHARGER_COLUMNS,
        use_container_width=True,
        column_config={
            "charger_id": st.column_config.TextColumn("Charger ID", width="small"),
            "tier": st.column_config.TextColumn("Tier", width="small"),
            "connector_type": st.column_config.TextColumn("Connector", width="small"),
            "user_tier": st.column_config.TextColumn("User Tier", width="small"),
            "session_state": st.column_config.TextColumn("State", width="small"),
            "session_progress": st.column_config.TextColumn("Progress", width="small"),
            "estimated_completion_minutes": st.column_config.TextColumn(
                "ETA (min)", width="small"
            ),
            "session_buffer": st.column_config.TextColumn("Buffer", width="small"),
            "avg_temperature": st.column_config.TextColumn("Temp (°C)", width="small"),
            "reason": st.column_config.TextColumn("Reason", width="medium"),
            "last_update": st.column_config.TextColumn("Last Seen", width="small"),
        },
    )

# --- Section 4: Charger Lookup ---
st.header("Charger Lookup")
st.divider()

charger_id = st.text_input("Charger ID")
if charger_id:
    charger_id = charger_id.strip().upper()
    resp = requests.get(f"{API_BASE_URL}/chargers/{charger_id}")
    if resp.status_code == 200:
        st.json(resp.json())
    else:
        st.write(f"No data found for {charger_id}.")

time.sleep(REFRESH_INTERVAL_SECONDS)
st.rerun()
