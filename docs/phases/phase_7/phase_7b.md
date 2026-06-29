Phase 7B — Operations Dashboard

Objective

Build a lightweight operations dashboard that visualizes the current fleet state directly from Redis.

The purpose of this phase is to demonstrate the value of the Redis operational state layer by providing a real-time view of active fleet alerts.

The dashboard should not perform any analytics or business logic.

It should simply present the current operational state maintained by the Redis consumer.

⸻

Scope

This phase focuses exclusively on:

* Reading fleet state from Redis
* Displaying active truck alerts
* Displaying fleet-wide alert summaries
* Displaying system health for major pipeline components
* Refreshing automatically as Redis updates
* Providing a simple operational monitoring interface

This phase does not include:

* AI remediation
* New Spark logic
* New Kafka consumers
* Historical analytics
* Additional alert processing

⸻

Architecture

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
Operations Dashboard

The dashboard is read-only.

Redis remains the single source of operational state.

⸻

Framework

Streamlit

Streamlit is chosen because it is Python-native, requires minimal boilerplate, and produces a clean operational interface suitable for portfolio demonstration.

The goal of this phase is to demonstrate the end-to-end operational pipeline, not frontend engineering.

⸻

Dashboard Goals

The dashboard should answer questions such as:

* Is the pipeline currently healthy?
* Which trucks currently have active alerts?
* How many trucks are RED?
* How many trucks are YELLOW?
* When was the last alert received?
* What is the current state of a specific truck?

The dashboard should not replay Kafka or perform stream processing.

All information should come directly from Redis.

⸻

Core Views

System Health

Display the operational status of major pipeline components.

Components:

* Redis — direct connection check (ping succeeds or fails)
* Kafka — inferred from fleet:last_update staleness
* Spark — inferred from fleet:last_update staleness

Staleness threshold: if fleet:last_update is older than approximately 10 minutes (2× the 5-minute window duration), surface a warning that the upstream pipeline may be stalled.

Also display the timestamp of the most recently processed alert from fleet:last_update.

This section is purely informational and read-only.

Note: Kafka and Spark health are proxy indicators based on data freshness, not direct component checks. This is an intentional constraint of the Redis-only architecture. A stale last_update timestamp means something upstream has likely stalled; it does not identify which component.

⸻

Fleet Summary

Display:

* Active RED truck count
* Active YELLOW truck count
* Last fleet update time

⸻

Active Truck List

Display one row per active truck.

Suggested columns:

* Vehicle ID
* Current Tier
* Delivery Buffer
* Average Temperature
* Alert Reason
* Last Update

Only trucks currently present in Redis should appear.

⸻

Truck Lookup

Allow searching for a vehicle ID.

Display:

* Current tier
* Delivery buffer
* Average temperature
* Alert timestamp
* Reason

If the truck does not exist in Redis:

Display:

No active alerts.

⸻

Refresh Strategy

The dashboard should periodically poll Redis and refresh all views.

Simple polling is sufficient.

Real-time push mechanisms are not required in this phase.

⸻

Technical Decisions

Redis Is The Source Of Truth

The dashboard should never query Kafka or Spark directly.

Redis is responsible for maintaining operational state.

Component health for Kafka and Spark is inferred from fleet:last_update staleness rather than direct checks. This is a deliberate tradeoff to keep the dashboard architecture simple and Redis-only.

⸻

Read-Only Dashboard

The dashboard must never modify Redis.

It only visualizes existing state.

⸻

Keep It Lightweight

The goal is operational visibility, not a production-grade UI.

Focus on clarity rather than appearance.

⸻

Deliverables

System Health View

Display pipeline component status and last processed alert timestamp.

Success Criteria

Operators can immediately identify whether the pipeline is active and when it last processed an alert.

⸻

Fleet Overview

Display active fleet status.

Success Criteria

Operators can immediately identify the current fleet health.

⸻

Active Truck View

Display current truck alert state.

Success Criteria

All active alerts are visible without replaying Kafka.

⸻

Truck Search

Lookup an individual vehicle.

Success Criteria

Truck state can be retrieved directly from Redis.

⸻

Validation Checklist

* Dashboard connects to Redis
* System health section displays Redis connectivity status
* System health section displays fleet:last_update timestamp
* Staleness warning appears when pipeline is inactive
* Fleet summary loads correctly
* Active truck list updates correctly
* Truck lookup works
* Expired Redis keys disappear from the dashboard automatically
* Dashboard performs no business logic
* Dashboard never modifies Redis

⸻

Exit Criteria

Phase 7B is complete when:

* Pipeline health is visible at a glance
* Redis state can be visualized
* Active alerts are displayed correctly
* Fleet summaries are visible
* Individual truck state can be queried
* The dashboard operates entirely from Redis

⸻

Phase 7B Outcome

Phase 7B is complete.

The Streamlit dashboard is live and reading exclusively from Redis. Validated with a large fleet run: 83 YELLOW alerts and 1 RED alert correctly reflected in fleet summary and active truck list. Per-truck lookup confirmed working for arbitrary vehicle IDs. Expired Redis keys disappear from the dashboard automatically as trucks recover.

The streaming pipeline now has a complete demonstrable end-to-end flow:

Telemetry → Kafka → Spark → Redis → Dashboard

This demonstrates how streaming analytics can be transformed into an operational monitoring platform without replaying event history.
