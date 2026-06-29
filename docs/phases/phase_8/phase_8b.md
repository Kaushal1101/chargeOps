Phase 8B — Dynamic Operational Fields

Objective

Add dynamic, lifecycle-aware operational fields to LogiShield so each trip carries richer time-varying context while preserving the existing risk pipeline.

Phase 8A introduced trip lifecycle state and static trip metadata. Phase 8B builds on that by adding fields that change as the truck moves through the trip.

The goal is not to change Spark's core risk logic.
The goal is to make the telemetry more realistic and to provide richer operational context for later Redis, dashboard, and AI phases.

⸻

Scope

This phase focuses exclusively on:

* Time-varying trip fields with fixed simulation formulas
* Spark schema expansion for the new dynamic fields
* Spark aggregation strategy for time-varying values
* Preserving the existing IN_TRANSIT filter and risk logic

This phase does not include:

* New trip lifecycle states
* New Spark risk logic
* Redis consumer changes (deferred to Phase 8C)
* Dashboard changes (deferred to Phase 8C)
* AI remediation
* Historical analytics

⸻

Architecture Being Updated

Simulator
    ↓
Kafka
    ↓
Spark
    ↓
risk-alerts

No new infrastructure is added.
Only the trip payload becomes more dynamic during the IN_TRANSIT portion of the lifecycle.

Redis consumer and dashboard column updates are explicitly deferred to Phase 8C.

⸻

Design Principle

These fields are simulated approximations, not a real GIS or routing engine.

The simulator should behave like a logistics system with believable movement over time, but it does not need to model real roads or spatial coordinates.

Each dynamic field must be computed from a fixed, deterministic formula so Cursor does not invent its own logic.

⸻

Dynamic Fields and Simulation Formulas

route_progress

route_progress = (elapsed_in_transit_seconds / trip_duration_seconds) clamped to [0.0, 1.0]

* 0.0 at the start of IN_TRANSIT
* 1.0 at the moment the trip duration elapses
* Computed on each event generation tick

⸻

estimated_arrival_minutes

estimated_arrival_minutes = round((1.0 - route_progress) * trip_duration_minutes)

* An integer representing minutes remaining until delivery
* Decreases toward 0 as route_progress approaches 1.0
* Always non-negative

⸻

remaining_stops

* Assigned once at trip start: random integer between 1 and 5
* Decremented by 1 at evenly-spaced route_progress thresholds
* If remaining_stops = 3, decrements occur at progress 0.33, 0.67
* If remaining_stops = 1, decrements at progress 0.5
* Never goes below 0

Formula for threshold check:

threshold_interval = 1.0 / remaining_stops_initial
stops_completed = int(route_progress / threshold_interval)
remaining_stops = max(0, remaining_stops_initial - stops_completed)

⸻

driver_hours_remaining

* Assigned once at trip start: random float between 8.0 and 11.0 hours
* Decreases proportionally to elapsed IN_TRANSIT seconds:
  driver_hours_remaining = shift_hours - (elapsed_in_transit_seconds / 3600.0)
* Clamped to [0.0, shift_hours]
* Resets to a new random value on the next trip assignment

This is a simplified approximation. It does not model rest periods or real HOS compliance.

⸻

Field Assignment Summary

Assigned once at trip start (alongside existing static fields):

* remaining_stops_initial (int, 1–5)
* shift_hours (float, 8.0–11.0)
* trip_start_time (float, time.time() at the moment IN_TRANSIT begins)

Computed on every IN_TRANSIT event:

* route_progress
* estimated_arrival_minutes
* remaining_stops
* driver_hours_remaining

During IDLE, LOADING, DELIVERY_COMPLETE: emit 0.0 / 0 for all dynamic fields.

⸻

Spark Changes

Two changes are required.

1. Expand schema

Add the following fields to TELEMETRY_SCHEMA and the structured select:

route_progress        — DoubleType
estimated_arrival_minutes — IntegerType
remaining_stops       — IntegerType
driver_hours_remaining — DoubleType

2. Aggregate with max() and min()

Dynamic fields change during the window, so first() would give a stale start-of-window snapshot.
Use aggregations that reflect the current-ish state of the truck at window close:

* max(route_progress) — furthest point reached in the window
* min(remaining_stops) — fewest stops remaining in the window
* min(driver_hours_remaining) — most depleted value in the window
* min(estimated_arrival_minutes) — shortest remaining time in the window

Add these to the existing .agg(...) block alongside the current first() calls.

Add the four aggregated columns to the alert_records select so they propagate to risk-alerts.

Do not change the risk tier thresholds or write_on_transition logic.

⸻

Validation Checklist

* route_progress advances from 0.0 toward 1.0 during IN_TRANSIT
* estimated_arrival_minutes decreases as route_progress increases
* remaining_stops decrements at the correct thresholds
* driver_hours_remaining decreases during transit and resets on new trip
* Dynamic fields are 0 / 0.0 during non-IN_TRANSIT states
* Spark schema includes all four new fields
* Spark aggregates dynamic fields with max() / min() rather than first()
* New fields appear in risk-alerts output
* Existing risk logic and IN_TRANSIT filter are unchanged

⸻

Exit Criteria

Phase 8B is complete when:

* Trips have time-varying operational fields computed from fixed formulas
* The simulator produces believable in-transit progression
* The enriched fields flow through Spark into risk-alerts
* Redis consumer and dashboard updates are ready to be tackled in Phase 8C

⸻

Phase 8B Outcome

Phase 8B is complete.

The telemetry stream now carries route_progress, estimated_arrival_minutes, remaining_stops, and driver_hours_remaining on every IN_TRANSIT event — computed from fixed formulas and visible in the risk-alerts output.

A bug was discovered and fixed during validation: write_on_transition was keyed on vehicle_id alone, causing alerts to be silently suppressed when a truck returned to the same tier on a new trip. The fix keys on (vehicle_id, trip_id) so each trip is treated as a fresh alert context. See ARCHITECTURE_DECISIONS.md Decision 17.

Redis consumer and dashboard column updates are deferred to Phase 8C.
