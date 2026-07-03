Phase 7A — Redis Fleet State Consumer

Objective

Introduce Redis as LogiShield’s operational state layer by building a consumer that materializes active truck alerts from risk-alerts into Redis.

This phase does not change the existing streaming pipeline.

Instead, it adds a downstream component that continuously converts streaming alerts into a fast, queryable operational view.

⸻

Scope

This phase focuses exclusively on:

* Consuming risk-alerts
* Writing active truck state into Redis
* Maintaining fleet-wide active alert counts
* TTL-based cleanup of recovered trucks
* Keeping Redis synchronized with the alert stream

This phase does not include:

* Dashboard implementation
* AI remediation
* New Spark logic
* New Kafka topics
* Historical analytics

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
Redis State Consumer
    ↓
Redis

The Redis consumer is completely downstream of Spark.

It does not participate in risk detection.

Its only responsibility is maintaining operational state.

⸻

Design Goals

The Redis consumer should:

* remain lightweight
* be easy to reason about
* avoid introducing business logic
* mirror the latest active alerts
* support future dashboards and APIs

⸻

Responsibilities

The consumer should:

1. Join risk-alerts using its own dedicated consumer group.
2. Parse each incoming alert.
3. Write or update the corresponding Redis truck record.
4. Refresh the truck TTL whenever a new alert arrives.
5. Update fleet-wide active alert counts.
6. Update fleet metadata (fleet:last_update).

This should happen within one consumer loop.

⸻

Redis Data Model

Per-Truck State

Redis Hash

truck:{vehicle_id}

Suggested fields:

vehicle_id
tier
delivery_buffer
avg_temperature
window_start
window_end
alert_ts
reason
last_update

The hash should always represent the latest active alert for that truck.

⸻

Fleet Metadata

fleet:counts

Stores:

RED
YELLOW

These represent active alerts only.

⸻

fleet:last_update

Stores the timestamp of the most recent processed alert.

⸻

TTL Strategy

Each truck hash should receive:

TTL = 420 seconds

approximately:

Window Duration
+ Watermark
+ Safety Buffer
5 min
+1 min
+1 min
≈7 min

⸻

Why TTL Exists

Spark only emits:

* YELLOW transitions
* RED transitions

Spark does not emit GREEN recovery events.

Without TTL:

RED

would remain in Redis forever.

Instead:

* every new alert refreshes the TTL
* inactive alerts expire automatically
* recovered trucks naturally disappear from the operational state

⸻

Important Architectural Decision

Redis represents:

Active Operational Alert State

not:

Complete Fleet State

A truck missing from Redis should be interpreted as:

* no active operational alert

rather than:

* explicit GREEN

This is an intentional tradeoff to avoid modifying the Spark pipeline.

⸻

Docker Changes

Add a Redis service to the existing Docker Compose stack.

Requirements:

* Redis 7
* Default configuration
* Default port (6379)

No clustering or persistence changes are required.

⸻

Deliverables

Redis State Consumer

Create a lightweight consumer that continuously updates Redis from risk-alerts.

Success Criteria

Active alerts are immediately reflected in Redis.

⸻

Fleet Summary

Maintain:

fleet:counts
fleet:last_update

during the same consumer loop.

Success Criteria

Fleet metadata always reflects the latest active alert stream.

⸻

TTL Validation

Confirm that inactive truck keys expire naturally.

Success Criteria

Recovered trucks disappear automatically after approximately seven minutes without changing the Spark pipeline.

⸻

Validation Checklist

* Redis service added to Docker Compose
* Redis consumer joins its own Kafka consumer group
* Truck hashes are created correctly
* Truck hashes receive a 7-minute TTL
* TTL refreshes when new alerts arrive
* Fleet counts update correctly
* fleet:last_update updates correctly
* Expired truck keys disappear naturally
* Redis remains synchronized with risk-alerts

⸻

Exit Criteria

Phase 7A is complete when:

* Redis continuously materializes active truck alerts
* Fleet counts are maintained correctly
* TTL cleanup works as expected
* Current operational alert state can be retrieved without replaying Kafka history

⸻

Phase 7A Outcome

At the conclusion of Phase 7A, LogiShield will gain its first operational state layer.

The system will now have two complementary views of the same data:

* Kafka — complete event history
* Redis — fast lookup of the latest active operational alerts

This establishes the foundation for the dashboard and future operational services without changing the existing Spark analytics pipeline.