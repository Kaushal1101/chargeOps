Phase 8 — Logistics Lifecycle and Operational Context

Objective

Transform the simulator from a simple telemetry generator into a realistic logistics lifecycle simulator.

Up to this point, each truck continuously emits telemetry with a repeating GREEN → YELLOW → RED cycle.

Phase 8 introduces deliveries as first-class entities. Trucks now perform complete delivery lifecycles, finish routes, become idle, receive new assignments, and continue operating.

The goal is to make the telemetry stream resemble a real logistics fleet rather than an endlessly repeating simulation.

⸻

Scope

This phase focuses exclusively on:

* Trip lifecycle simulation with a state machine
* Static trip context assigned at trip start
* Dynamic operational fields that evolve during a trip
* Spark filtering so risk classification only applies to IN_TRANSIT trucks
* Schema propagation through the full pipeline

This phase does not include:

* AI remediation
* Dashboard redesign
* New Kafka topics
* Spatial or GIS routing
* Benchmarking

⸻

Revised Sub-Phase Structure

Phase 8A — Trip Lifecycle and Static Trip Metadata ✓ Complete

Merge lifecycle assignment and trip metadata into one implementation step.
A truck cannot be assigned a trip without also receiving its trip context.

Phase 8B — Dynamic Operational Fields

Introduce fields that evolve naturally over the course of a trip.

Phase 8C — Schema Propagation

Propagate all new fields through Kafka, Spark, Redis, and the dashboard.

⸻

Phase 8A — Trip Lifecycle and Static Trip Metadata

Goal

Give each truck a complete, repeating delivery lifecycle driven by a state machine with fixed durations and small random jitter.

⸻

Lifecycle State Machine

Idle
  ↓
Loading
  ↓
In Transit
  ↓
Delivery Complete
  ↓
Idle

Each truck cycles through these states continuously.

⸻

State Durations

Loading
Duration: 1–3 minutes with random jitter.
The truck is being loaded at a depot or warehouse.

In Transit
Duration: until route_progress reaches 1.0.
The trip duration is assigned at trip start. The truck progresses toward 1.0 over that duration.

Delivery Complete
Duration: immediate — transitions to Idle on the next tick.
The delivery is finished. The truck is briefly marked complete before becoming available again.

Idle
Duration: until a new assignment is generated.
The truck is available but not yet assigned. A short idle window (configurable) before the next trip begins.

⸻

Static Trip Context

Assign at trip start. These fields are immutable for the duration of the trip.

trip_id
A new UUID assigned at the start of each trip.

cargo_type
One of: Pharmaceuticals, Fresh Food, Frozen Goods, Electronics, General Freight.

cargo_value
Approximate monetary value of the shipment.

customer_priority
One of: Standard, Priority, Critical.

service_level
One of: Standard, Express, Same-Day.

destination_region
One of: North, South, East, West.

These fields remain constant from Loading through Delivery Complete.

⸻

Phase 8B — Dynamic Operational Fields

Goal

Introduce fields that change naturally as the trip progresses.

These are simulated approximations, not real spatial tracking. LogiShield is not building a GIS route engine.

⸻

Dynamic Fields

route_progress
Advances from 0.0 to 1.0 over the assigned trip duration.
Represents how far through the delivery the truck currently is.

estimated_arrival
Derived from the remaining simulated trip time.
Decrements as route_progress increases.

remaining_stops
Decrements as route_progress increases.
Assigned at trip start and counts down to 0 by Delivery Complete.

driver_hours_remaining
Decreases as simulated time passes during IN_TRANSIT.
Resets to a full value when a new trip is assigned.
This is a regulatory approximation, not real HOS tracking.

⸻

trip_state Field

Every telemetry event should carry a trip_state field with one of:

IDLE
LOADING
IN_TRANSIT
DELIVERY_COMPLETE

This field is the key used downstream to decide whether risk scoring applies.

⸻

Phase 8C — Schema Propagation

Goal

Propagate all new fields through the existing pipeline without changing risk logic.

⸻

Spark Changes

Add all new fields to the Spark telemetry schema:

trip_state, trip_id, cargo_type, cargo_value, customer_priority, service_level,
destination_region, route_progress, estimated_arrival, remaining_stops,
driver_hours_remaining

Risk classification filter:

Only compute delivery_buffer, avg_temperature, and risk_tier for events where
trip_state == IN_TRANSIT.

Non-IN_TRANSIT events (Idle, Loading, Delivery Complete) are dropped before
windowed aggregation. This prevents spurious YELLOW or RED alerts from trucks
that are not on an active delivery leg.

Static trip context fields (cargo_type, customer_priority, etc.) should be
forwarded to the risk-alerts payload so downstream components have full
operational context.

⸻

Redis Changes

Update state_consumer.py to store the new fields in each truck hash.

The trip_state field should be included so the dashboard can surface it.

⸻

Dashboard Changes

Add trip_state and cargo_type to the active truck list columns.

No other dashboard changes are required.

⸻

Architecture

No new components are added in Phase 8.

The change is entirely within the data flowing through the existing pipeline.

Simulator
    ↓ (enriched telemetry with lifecycle state and trip context)
Kafka
    ↓
Spark (risk logic filtered to IN_TRANSIT only)
    ↓
risk-alerts (enriched with operational context)
    ↓
Redis State Consumer
    ↓
Redis
    ↓
Dashboard

⸻

Design Principles

Simulated approximations are acceptable

route_progress, estimated_arrival, and driver_hours_remaining are approximations
that produce believable operational behavior. They do not require real spatial
data or HOS compliance logic.

Risk logic must not change

The delivery_buffer and cargo_temperature thresholds remain unchanged.
Phase 8 only changes which events are fed into the risk computation.

Static context is assigned once per trip

trip_id and all static fields are generated at the moment a trip begins.
They do not change mid-trip.

⸻

Validation Checklist

* Trucks cycle through the full lifecycle repeatedly
* Each trip receives a unique trip_id and consistent static metadata
* Dynamic fields evolve naturally during IN_TRANSIT
* route_progress reaches 1.0 and triggers Delivery Complete correctly
* driver_hours_remaining decrements and resets correctly
* Non-IN_TRANSIT events produce no risk alerts in Spark
* IN_TRANSIT events continue producing YELLOW and RED alerts as expected
* New fields appear in Kafka messages
* Spark forwards new fields to risk-alerts
* Redis stores enriched truck state including trip_state
* Dashboard active truck list shows trip_state and cargo_type

⸻

Exit Criteria

Phase 8 is complete when:

* Trucks behave as realistic logistics assets moving through delivery lifecycles
* Risk classification is scoped exclusively to IN_TRANSIT trucks
* The enriched schema flows end-to-end from simulator to dashboard
* The pipeline produces clean, operationally meaningful alerts
