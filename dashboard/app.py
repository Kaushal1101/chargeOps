"""LogiShield operations dashboard — read-only fleet state from Redis.

Run with:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

from datetime import datetime, timezone

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

# --- Section 2: Network Summary ---
st.header("Network Summary")

counts = r.hgetall("network:counts")
red_count = int(counts.get("RED") or 0)
yellow_count = int(counts.get("YELLOW") or 0)

col_red, col_yellow = st.columns(2)
col_red.metric("RED Alerts", red_count)
col_yellow.metric("YELLOW Alerts", yellow_count)

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
