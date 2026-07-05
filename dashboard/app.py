"""LogiShield operations dashboard — read-only fleet state from Redis.

Run with:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

from datetime import datetime, timezone

import pydeck as pdk
import redis
import streamlit as st
import time

REDIS_HOST = "localhost"
REDIS_PORT = 6379
STALENESS_THRESHOLD_SECONDS = 600  # 10 minutes — 2× window duration
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


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


st.set_page_config(page_title="LogiShield Operations", layout="wide")

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
except redis.RedisError as exc:
    st.error(f"Redis not reachable at {REDIS_HOST}:{REDIS_PORT}: {exc}")
    st.stop()

# --- Section 1: System Health ---
st.header("System Health")

st.write("Redis: Online")

last_update = r.hgetall("network:last_update")
ts = last_update.get("ts")
parsed_ts = _parse_ts(ts)

if parsed_ts is not None:
    staleness = (datetime.now(timezone.utc) - parsed_ts).total_seconds()
else:
    staleness = STALENESS_THRESHOLD_SECONDS

if parsed_ts is not None and staleness < STALENESS_THRESHOLD_SECONDS:
    st.write("Pipeline: Active")
    st.write(f"Last update: {ts}")
else:
    st.write(f"Pipeline: Stalled — last update: {ts or 'never'}")

st.caption(
    "Kafka and Spark health are inferred from data freshness. "
    "A stale timestamp indicates the upstream pipeline may have stopped."
)

# --- Section 1.5: Network Statistics ---
st.header("Network Statistics")

network_stats = r.hgetall("stats:network")

if not network_stats:
    st.caption("No statistics available yet — waiting for Spark stats sink.")
else:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active Sessions", network_stats.get("total_active_sessions", "—"))
    col2.metric("Avg Temperature", f"{network_stats.get('avg_temperature', '—')} °C")
    col3.metric("Avg Session Buffer", f"{network_stats.get('avg_session_buffer', '—')} min")
    col4.metric("Avg Power Output", f"{network_stats.get('avg_power_kw', '—')} kW")

    # Regional breakdown
    region_rows = []
    for key in sorted(r.scan_iter("stats:region:*")):
        rec = r.hgetall(key)
        region = key.split("stats:region:")[-1]
        region_rows.append({
            "region": region,
            "active_sessions": rec.get("active_sessions", ""),
            "avg_temperature": rec.get("avg_temperature", ""),
            "avg_session_progress": f"{round(float(rec.get('avg_session_progress', 0)) * 100)}%" if rec.get("avg_session_progress") else "",
            "avg_session_buffer": rec.get("avg_session_buffer", ""),
            "avg_power_kw": rec.get("avg_power_kw", ""),
        })

    if region_rows:
        st.subheader("By Region")
        st.dataframe(region_rows, use_container_width=True)

    # Connector breakdown
    connector_rows = []
    for key in sorted(r.scan_iter("stats:connector:*")):
        rec = r.hgetall(key)
        connector = key.split("stats:connector:")[-1]
        connector_rows.append({
            "connector_type": connector,
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

counts = r.hgetall("network:counts")
red_count = int(counts.get("RED") or 0)
yellow_count = int(counts.get("YELLOW") or 0)

col_red, col_yellow = st.columns(2)
col_red.metric("RED Alerts", red_count)
col_yellow.metric("YELLOW Alerts", yellow_count)

# --- Section 2.5: Alert Map ---
st.header("Alert Map")

map_rows = []
for key in r.scan_iter("charger:*"):
    record = r.hgetall(key)
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
for key in r.scan_iter("charger:*"):
    record = r.hgetall(key)
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
    truck = r.hgetall(f"charger:{charger_id}")
    if truck:
        st.json(truck)
    else:
        st.write(f"No active alerts for {charger_id}.")

time.sleep(REFRESH_INTERVAL_SECONDS)
st.rerun()
