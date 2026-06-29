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
TRUCK_COLUMNS = [
    "vehicle_id",
    "tier",
    "cargo_type",
    "customer_priority",
    "delivery_buffer",
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

last_update = r.hgetall("fleet:last_update")
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

# --- Section 2: Fleet Summary ---
st.header("Fleet Summary")

counts = r.hgetall("fleet:counts")
red_count = int(counts.get("RED") or 0)
yellow_count = int(counts.get("YELLOW") or 0)

col_red, col_yellow = st.columns(2)
col_red.metric("RED Alerts", red_count)
col_yellow.metric("YELLOW Alerts", yellow_count)

# --- Section 3: Active Trucks ---
st.header("Active Trucks")

trucks: list[dict[str, str]] = []
for key in r.scan_iter("truck:*"):
    record = r.hgetall(key)
    if record:
        trucks.append({col: record.get(col, "") for col in TRUCK_COLUMNS})

if not trucks:
    st.write("No active alerts.")
else:
    trucks.sort(
        key=lambda row: (
            TIER_SORT_ORDER.get(row.get("tier", ""), 99),
            row.get("vehicle_id", ""),
        )
    )
    st.dataframe(trucks, column_order=TRUCK_COLUMNS, use_container_width=True)

# --- Section 4: Truck Lookup ---
st.header("Truck Lookup")

vehicle_id = st.text_input("Vehicle ID")
if vehicle_id:
    vehicle_id = vehicle_id.strip().upper()
    truck = r.hgetall(f"truck:{vehicle_id}")
    if truck:
        st.json(truck)
    else:
        st.write(f"No active alerts for {vehicle_id}.")

time.sleep(REFRESH_INTERVAL_SECONDS)
st.rerun()
