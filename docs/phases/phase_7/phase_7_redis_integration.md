Phase 7 — Redis Fleet State Layer

Objective

Add a lightweight operational state layer to LogiShield using Redis so the system can answer “current state” questions without replaying Kafka history.

Up to this point, LogiShield has focused on streaming detection and benchmarking. Phase 7 adds a fast mutable state store that tracks the live fleet state derived from risk-alerts.

The goal is not to change how Spark detects risk.
The goal is to materialize the latest state of the fleet so downstream components can query it efficiently.

⸻

Scope

This phase focuses exclusively on:

* Consuming risk-alerts
* Writing the latest truck state into Redis
* Maintaining fleet-wide counts and summaries
* Preserving the existing Kafka → Spark → alert flow
* Creating a state store that a future dashboard can read directly

This phase does not include:

* AI remediation
* New Spark logic
* New alert generation logic
* Detailed benchmarking
* Full dashboard implementation

⸻

Architecture Being Added

Simulator
    ↓
Kafka
    ↓
Spark
    ↓
risk-alerts
    ↓
Redis state consumer
    ↓
Redis

Redis acts as a materialized view of the fleet’s current status.

⸻

Why Redis Fits Here

Kafka is an event log.
Spark is the risk detection engine.
Redis is the current-state store.

Redis is useful because it can answer questions like:

* Which trucks are RED right now?
* How many trucks are YELLOW?
* What was the last known tier for TRUCK_101?
* When was TRUCK_102 last seen?
* What is the latest fleet summary?

These are state queries, not event replay queries.

⸻

Sub-Phases

Phase 7A — Redis Fleet State Consumer and Fleet Summary

Goal

Build a consumer that reads risk-alerts and writes per-truck state and fleet-wide summary counts into Redis in a single pass.

Note: per-truck state (7A) and fleet summary (7B) are a single implementation. The consumer loop updates both in the same iteration as each alert arrives. They are documented separately as logical concerns but are not separate components.

Responsibilities

* Consume alert messages from Kafka (dedicated consumer group)
* Parse the alert payload
* Write per-vehicle state to Redis with a 7-minute TTL
* Maintain fleet-wide YELLOW/RED counts in Redis
* Update fleet:last_update on each write

Suggested Redis Keys

Per truck (with TTL):

truck:{vehicle_id}

Fleet summary:

fleet:counts
fleet:last_update

Example Truck Record

{
  "vehicle_id": "TRUCK_0001",
  "tier": "RED",
  "delivery_buffer": -4,
  "avg_temperature": 6.4,
  "window_start": "...",
  "window_end": "...",
  "alert_ts": "...",
  "reason": "Delivery buffer below threshold"
}

TTL Design

Every truck:{vehicle_id} key is written with a TTL of 420 seconds (7 minutes).

TTL = window_duration (5 min) + watermark (1 min) + safety buffer (1 min)

This handles the GREEN state gap: risk-alerts only contains YELLOW and RED transitions. When a truck returns to GREEN, no further alerts are emitted. The Redis key expires naturally after ~7 minutes, removing it from active fleet counts without requiring any change to the Spark pipeline.

fleet:counts reflects trucks with active YELLOW/RED alerts only — not total fleet size. This is an explicit architectural tradeoff. See ARCHITECTURE_DECISIONS.md Decision 14.

Success Criteria

risk-alerts can be consumed, the latest truck state appears in Redis with correct TTL, and fleet:counts reflects active non-GREEN vehicles.

⸻

Phase 7C — State Query Layer

Goal

Make the Redis state easy to inspect from code or a future dashboard.

Responsibilities

* Read the latest state for a single truck
* Read fleet counts
* Read recent fleet update metadata
* Confirm that Redis reflects the current alert stream

Success Criteria

A small query script can retrieve current fleet state directly from Redis.

⸻

Data Flow

Spark
    ↓
risk-alerts
    ↓
Redis state consumer
    ↓
Redis
    ↓
Future dashboard / query tools

⸻

Technical Decisions

Redis Stores Current State, Not History

Redis should hold only the latest useful state, not the full event history.

Kafka Remains the Source of Truth for Events

Kafka still holds the event stream. Redis is a derived view.

State Updates Should Be Lightweight

The Redis layer should be fast, predictable, and easy to reason about.

Schema Should Match Alert Output

Redis state should be populated from the finalized risk-alerts schema.

TTL-Based GREEN Recovery (not full fleet state)

risk-alerts contains only YELLOW and RED transitions. GREEN is filtered in Spark. Redis therefore cannot be updated when a truck recovers to GREEN. To prevent stale YELLOW/RED entries accumulating indefinitely, every truck:{vehicle_id} key is written with a 7-minute TTL. A recovered truck generates no new alerts, its key expires, and it falls out of fleet:counts automatically.

This means Redis represents active non-GREEN alert state, not complete fleet state. A truck absent from Redis is either GREEN or not reporting — it is not counted in fleet:counts.

This is a deliberate tradeoff to avoid modifying the Spark alert pipeline in Phase 7. A future phase can emit GREEN transitions to support full fleet-state materialization. See ARCHITECTURE_DECISIONS.md Decision 14 for full reasoning and the future enhancement path.

⸻

Validation Checklist

* Redis service is running (redis:7, port 6379)
* A consumer can read risk-alerts with its own consumer group
* Each truck’s latest state is written to Redis with a 7-minute TTL
* Fleet counts are updated in Redis as alerts arrive
* A query script can read current truck state from Redis
* The Redis state reflects the latest alert stream
* Truck keys expire after ~7 minutes of inactivity (GREEN recovery confirmed)

⸻

Exit Criteria

Phase 7 is complete when:

* Redis stores the live state of the fleet
* risk-alerts are materialized into per-truck Redis keys
* Fleet-wide summary counts are available
* The state layer can be queried independently of Kafka replay

⸻

Phase 7 Outcome

At the conclusion of Phase 7, LogiShield will have a fast operational state layer.

This will allow future dashboard work to read directly from Redis instead of reconstructing current fleet state from the Kafka stream.