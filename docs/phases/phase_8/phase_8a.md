Phase 8A — Trip Lifecycle and Static Trip Metadata

Objective

Extend the simulator so LogiShield models full delivery trips rather than a single endless truck loop.

Phase 8A introduces a four-state trip lifecycle and assigns static trip context at the start of each delivery. This makes the stream more realistic and gives future phases a stronger operational foundation.

The goal is not to make Spark smarter yet.
The goal is to produce richer lifecycle-aware telemetry while keeping the current risk logic stable.

⸻

Scope

This phase focuses exclusively on:

* Four-state trip lifecycle simulation
* Static trip metadata assignment at trip start
* Emitting trip_state on every event
* Filtering non-IN_TRANSIT events in Spark before windowed aggregation
* Forwarding static trip metadata through Spark into risk-alerts

This phase does not include:

* Dynamic route progress fields (Phase 8B)
* Dynamic driver hours modeling (Phase 8B)
* Dashboard changes
* AI remediation
* New risk logic
* New alert logic

⸻

Architecture Being Updated

Simulator
    ↓
Kafka
    ↓
Spark
    ↓
risk-alerts

The architecture does not change.
Only the simulator data model becomes trip-aware, and Spark is updated to filter and propagate accordingly.

⸻

Trip Lifecycle Model

Each truck moves through a four-state delivery lifecycle:

Idle
  ↓
Loading
  ↓
In Transit
  ↓
Delivery Complete
  ↓
Idle

State Definitions

Idle
The truck is waiting for a new trip assignment.
No active delivery context. No risk scoring.

Loading
A trip has been assigned. The truck is being prepared at the depot.
Static trip metadata is assigned at the moment Loading begins.
No risk scoring.

In Transit
The truck is on an active delivery leg.
Normal telemetry and windowed risk classification apply here.
This is the only state eligible for risk scoring.

Delivery Complete
The trip has ended. One brief terminal tick before returning to Idle.
No risk scoring.

⸻

State Durations

These are concrete durations for the simulator state machine.
Do not invent timing logic — use these values.

Idle
Duration: 30–90 seconds, uniformly random.
The truck waits before a new assignment is generated.

Loading
Duration: 1–3 minutes, uniformly random.
The truck is being loaded at a depot or staging area.

In Transit
Duration: configurable trip duration, 5–15 minutes, uniformly random.
The truck progresses toward delivery completion over this window.

Delivery Complete
Duration: 1 tick (one telemetry emission), then immediately return to Idle.

⸻

Trip State as Control Signal

Every telemetry event must carry a trip_state field.

Valid values: IDLE, LOADING, IN_TRANSIT, DELIVERY_COMPLETE

This field is the control signal for downstream processing.

Spark uses trip_state to decide which events are eligible for risk classification.

⸻

Static Trip Metadata

Assign static metadata at the moment Loading begins.
These fields are immutable for the duration of the trip.
They must be present on every event from Loading through Delivery Complete.

Fields:

trip_id       — UUID, new per trip
cargo_type    — one of: Pharmaceuticals, Fresh Food, Frozen Goods, Electronics, General Freight
cargo_value   — approximate monetary value (float)
customer_priority — one of: Standard, Priority, Critical
service_level — one of: Standard, Express, Same-Day
destination_region — one of: North, South, East, West

During Idle, these fields should be emitted as empty strings or null equivalents.

⸻

Spark Changes

Two changes are required. Both are necessary.

1. Filter

Only IN_TRANSIT events should enter the windowed aggregation path.

Filter condition: trip_state == IN_TRANSIT

Apply this filter before the windowed aggregation. Non-IN_TRANSIT events should be dropped at this stage and never reach the risk classification logic.

This prevents idle, loading, and delivery-complete trucks from generating spurious alerts or wasting aggregation resources.

2. Propagate

Static trip metadata fields must be included in the risk-alerts output payload.

Add the following fields to the Spark output schema and carry them through to risk-alerts:

trip_id, cargo_type, cargo_value, customer_priority, service_level, destination_region

These fields survive the Spark boundary so Redis and the dashboard can display full operational context alongside the alert.

If Spark only filters but does not propagate, the new fields will be silently dropped and unavailable to all downstream components.

⸻

Deliverables

Trip Lifecycle State Machine

Update the simulator so trucks progress through the four delivery lifecycle states with the specified durations.

Success Criteria

Trucks complete delivery trips, return to idle, and receive new assignments automatically.

⸻

Static Trip Metadata Assignment

Assign immutable trip context at the moment Loading begins.

Success Criteria

Each trip has a stable identity and operational context that persists from Loading through Delivery Complete.

⸻

trip_state Emission

Emit trip_state on every telemetry event.

Success Criteria

Spark and all downstream consumers can determine which events are eligible for risk processing.

⸻

Spark Filter and Propagation

Update Spark to filter non-IN_TRANSIT events before aggregation and include static trip fields in the alert output.

Success Criteria

Non-IN_TRANSIT events produce no alerts. Static trip metadata appears in risk-alerts messages alongside the existing alert fields.

⸻

Validation Checklist

* Trucks cycle through all four lifecycle states
* State durations match the specified ranges
* Each trip receives a unique trip_id at Loading
* Static trip metadata is consistent across all events within a trip
* trip_state appears on every telemetry event
* Spark drops non-IN_TRANSIT events before windowed aggregation
* Existing risk logic (delivery_buffer, avg_temperature) remains unchanged for IN_TRANSIT events
* Static trip fields (trip_id, cargo_type, etc.) appear in risk-alerts output
* Trucks return to Idle after Delivery Complete and receive a new trip

⸻

Exit Criteria

Phase 8A is complete when:

* Trucks follow a realistic four-state delivery lifecycle
* Static operational context is attached to each trip and consistent throughout
* trip_state is available for downstream filtering
* Non-IN_TRANSIT events are excluded before windowed aggregation
* Static trip metadata propagates through Spark into risk-alerts
* The pipeline is ready for dynamic operational fields in Phase 8B

⸻

Phase 8A Outcome

Phase 8A is complete.

LogiShield no longer runs a single endless telemetry loop. Each truck cycles through a four-state delivery lifecycle (IDLE → LOADING → IN_TRANSIT → DELIVERY_COMPLETE) driven by a time-based state machine with fixed durations and jitter. Static trip metadata (trip_id, cargo_type, cargo_value, customer_priority, service_level, destination_region) is assigned at the start of each Loading phase and remains consistent for the duration of the trip.

Spark filters non-IN_TRANSIT events before windowed aggregation, preventing idle and loading trucks from generating spurious alerts. Static trip fields propagate through Spark into risk-alerts and are stored in Redis alongside the existing alert fields.

The dashboard active truck list now shows cargo_type and customer_priority. The truck lookup returns full enriched records including all trip metadata. Redis lookups remain fast with the enriched schema.

The pipeline is ready for dynamic operational fields in Phase 8B.
