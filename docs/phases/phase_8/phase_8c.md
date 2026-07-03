Phase 8C — Schema Propagation

Objective

Propagate the enriched trip lifecycle and operational context fields through the existing LogiShield pipeline without changing the core risk logic.

Phase 8A introduced trip lifecycle state and static trip metadata. Phase 8B added dynamic operational fields. Phase 8C ensures those fields flow cleanly through Kafka, Spark, Redis, and the dashboard so the enriched trip model is visible end to end.

The goal is not to change how Spark detects risk.
The goal is to make the enriched schema visible throughout the pipeline.

⸻

Scope

This phase focuses exclusively on:

* Extending Spark’s telemetry schema to include the new trip fields
* Preserving the existing IN_TRANSIT filtering rule
* Forwarding enriched context into risk-alerts
* Storing the enriched state in Redis
* Displaying the new fields in the dashboard

This phase does not include:

* New trip lifecycle behavior
* New dynamic field simulation logic
* New risk thresholds
* AI remediation
* New alert types
* New infrastructure components

⸻

Architecture Being Updated

Simulator
    ↓
Kafka
    ↓
Spark
    ↓
risk-alerts
    ↓
Redis State Consumer
    ↓
Redis
    ↓
Dashboard

No new components are added in this phase.
Only the schema and downstream visibility are expanded.

⸻

Fields To Propagate

Trip Lifecycle Fields

* trip_state
* trip_id

Static Trip Context

* cargo_type
* cargo_value
* customer_priority
* service_level
* destination_region

Dynamic Operational Fields

* route_progress
* estimated_arrival_minutes
* remaining_stops
* driver_hours_remaining

⸻

Spark Changes

1. Extend the Input Schema

Update the Spark telemetry schema so it explicitly parses the new fields from Kafka JSON.

Spark should recognize all enriched fields rather than silently dropping them.

⸻

2. Preserve the Existing Risk Filter

Only events where:

trip_state == IN_TRANSIT

should enter the windowed risk computation path.

Non-IN_TRANSIT events (Idle, Loading, Delivery Complete) should be dropped before windowed aggregation.

This prevents spurious alerts from trucks that are not actively in transit.

⸻

3. Forward Enriched Context Into risk-alerts

The risk-alerts payload should include the enriched trip context alongside the existing risk output.

At minimum, forward:

* trip_state
* trip_id
* cargo_type
* cargo_value
* customer_priority
* service_level
* destination_region
* route_progress
* estimated_arrival_minutes
* remaining_stops
* driver_hours_remaining

The existing risk logic remains unchanged.

Spark still computes the same delivery-buffer and temperature-based risk tiering for IN_TRANSIT events.

⸻

Redis Changes

Update the Redis consumer so each truck hash stores the enriched state from risk-alerts.

Suggested Redis Hash Fields

* trip_state
* trip_id
* cargo_type
* cargo_value
* customer_priority
* service_level
* destination_region
* route_progress
* estimated_arrival_minutes
* remaining_stops
* driver_hours_remaining
* tier
* delivery_buffer
* avg_temperature
* window_start
* window_end
* alert_ts
* reason
* last_update

Redis should remain the current operational state layer, not a historical store.

⸻

Dashboard Changes

Update the dashboard so the enriched fields are visible in the active truck view.

Minimum Dashboard Updates

Add at least:

* trip_state
* cargo_type
* customer_priority
* route_progress
* estimated_arrival_minutes

The dashboard should continue to read only from Redis.

No new business logic should be introduced in the UI.

⸻

Technical Decisions

Spark Still Owns Risk Logic

Phase 8C only changes the schema boundaries.
It does not alter the delivery-buffer or temperature thresholds.

Enriched Context Must Survive the Pipeline

The new fields should be visible in Kafka, Spark, Redis, and the dashboard.

Non-IN_TRANSIT Events Stay Out Of Aggregation

The lifecycle filter remains in place to keep the alert stream clean.

Schema Changes Must Be Explicit

Spark, Redis, and the dashboard should all be updated intentionally so no fields are silently dropped.

⸻

Validation Checklist

* Spark telemetry schema includes the new trip fields
* Non-IN_TRANSIT events are still filtered before aggregation
* risk-alerts includes the enriched operational context
* Redis stores the enriched truck state
* Dashboard displays the new fields from Redis
* Existing risk logic remains unchanged
* trip_state is visible end to end

⸻

Exit Criteria

Phase 8C is complete when:

* The enriched schema flows through Kafka, Spark, Redis, and the dashboard
* The current operational state can be viewed with the new trip context
* The existing risk detection behavior remains unchanged
* The pipeline now represents a realistic logistics lifecycle end to end

⸻

Phase 8C Outcome

At the conclusion of Phase 8C, LogiShield will have a fully propagated enriched trip schema.

This completes the lifecycle-and-context expansion introduced in Phase 8 and makes the dashboard meaningfully more informative without changing the underlying risk detection engine.