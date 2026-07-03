Phase 9C — Redis Consumer and Dashboard

Objective

Update the Redis consumer and Streamlit dashboard to reflect the EV charging schema produced by Phases 9A and 9B. The Redis TTL pattern, hash storage model, fleet counter logic, and dashboard auto-refresh are all structurally unchanged. This phase is field renames, key prefix updates, and label changes only.

⸻

Scope

This phase touches:
- redis_consumer/state_consumer.py — key prefixes, field names, consumer group name
- dashboard/app.py — column list, section headers, key scan pattern, lookup logic

This phase does not touch:
- Redis connection configuration
- Kafka consumer configuration
- TTL duration
- Hash storage pattern
- Dashboard refresh logic
- System health staleness check

⸻

Redis Consumer Changes (state_consumer.py)

Key prefix renames:
  truck:{vehicle_id}   →  charger:{charger_id}
  fleet:counts         →  network:counts
  fleet:last_update    →  network:last_update

Consumer group:
  logishield-redis-state  (can remain unchanged or rename to logishield-network-state)

_recount_fleet() function:
- Rename to _recount_network()
- Update scan pattern: "truck:*"  →  "charger:*"
- Update delete/hset target: "fleet:counts"  →  "network:counts"
- Print statement: "fleet:counts rebuilt"  →  "network:counts rebuilt"

run() function:
- truck_key = f"truck:{vehicle_id}"  →  charger_key = f"charger:{charger_id}"
- Update r.hget(truck_key, "tier")  →  r.hget(charger_key, "tier")
- Update r.hset(truck_key, ...) →  r.hset(charger_key, ...)
- Update r.expire(truck_key, ...) →  r.expire(charger_key, ...)
- Update fleet:counts references to network:counts
- Update fleet:last_update hset to network:last_update

hset mapping field renames:
  vehicle_id              → charger_id
  delivery_buffer         → session_buffer
  trip_state              → session_state
  trip_id                 → session_id
  cargo_type              → connector_type
  cargo_value             → energy_requested_kwh
  customer_priority       → user_tier
  service_level           → charging_speed
  destination_region      → site_region
  route_progress          → session_progress
  estimated_arrival_minutes → estimated_completion_minutes
  remaining_stops         → power_output_kw
  driver_hours_remaining  → energy_delivered_kwh

New fields to store in hset mapping:
  charger_lat             str(alert.get("charger_lat", ""))
  charger_lng             str(alert.get("charger_lng", ""))
  rated_power_kw          str(alert.get("rated_power_kw", ""))
  site_id                 alert.get("site_id", "")

Log line update:
  f"[state_consumer] {vehicle_id} → {tier} (prev: {prev_tier or 'new'})"
  becomes:
  f"[state_consumer] {charger_id} → {tier} (prev: {prev_tier or 'new'})"

⸻

Dashboard Changes (app.py)

Page title:
  "LogiShield Operations"  →  "ChargeSight Operations"
  (or retain LogiShield branding — either is acceptable)

CHARGER_COLUMNS (replaces TRUCK_COLUMNS):
  "charger_id"
  "tier"
  "connector_type"
  "user_tier"
  "session_state"
  "session_progress"
  "estimated_completion_minutes"
  "session_buffer"
  "avg_temperature"
  "reason"
  "last_update"

Redis scan pattern:
  r.scan_iter("truck:*")  →  r.scan_iter("charger:*")

fleet:last_update reads:
  r.hgetall("fleet:last_update")  →  r.hgetall("network:last_update")

fleet:counts reads:
  r.hgetall("fleet:counts")  →  r.hgetall("network:counts")

Section header renames:
  "Fleet Summary"   →  "Network Summary"
  "Active Trucks"   →  "Active Chargers"
  "Truck Lookup"    →  "Charger Lookup"

Metric label renames:
  "RED Alerts"      →  "RED Alerts"       (unchanged)
  "YELLOW Alerts"   →  "YELLOW Alerts"    (unchanged)

_fmt_route_progress() function:
  Rename to _fmt_session_progress()
  Applied to "session_progress" column instead of "route_progress"
  Logic is unchanged (float → percentage string)

Charger lookup:
  vehicle_id input label  →  "Charger ID"
  r.hgetall(f"truck:{vehicle_id}")  →  r.hgetall(f"charger:{charger_id}")
  "No active alerts for {vehicle_id}."  →  "No active alerts for {charger_id}."

No active alerts message:
  "No active alerts."  (unchanged)

⸻

Validation Checklist

- Redis consumer reads risk-alerts and writes to charger: keys successfully
- charger:{charger_id} hashes contain all new fields including charger_lat, charger_lng, site_id
- network:counts increments and decrements correctly on tier transitions
- network:last_update is written on every processed alert
- TTL is applied correctly to charger: keys
- Dashboard scans charger:* and displays rows in CHARGER_COLUMNS order
- session_progress is formatted as a percentage
- Network Summary metrics show correct RED and YELLOW counts
- Charger Lookup returns full enriched JSON for a given charger_id
- System health staleness check still works (reads network:last_update)

⸻

Exit Criteria

Phase 9C is complete when:
- The full pipeline flows end-to-end with the EV charging schema:
  Charger Simulator → charger-telemetry → Spark → risk-alerts → Redis → Dashboard
- The dashboard presents a network operations view: charger health, session state, site context, temperature, session buffer
- All distributed systems properties are preserved: watermarking, windowed aggregation, transition deduplication, TTL-based eviction
- Phase 9 is complete
