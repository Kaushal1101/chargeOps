# Phase 3 — Real-Time Analytics Engine

## Objective

Build the Spark Structured Streaming pipeline that consumes telemetry from Kafka, performs real-time computations, classifies shipment risk, and produces structured risk alerts.

By the end of this phase, LogiShield should continuously analyze live telemetry and identify Green, Yellow, and Red operational states.

---

# Scope

This phase focuses exclusively on:

- Kafka ingestion with Spark
- Event schema parsing
- Event-time processing
- Sliding window aggregations
- Watermarking
- Risk tier classification
- Alert generation

This phase does not include:

- AI remediation
- Chaos engineering
- Benchmarking
- Dashboarding

---

# Architecture

text Telemetry Simulator         │         ▼ Apache Kafka (fleet-telemetry)         │         ▼ Spark Structured Streaming         │         ▼ Risk Tiering Engine         │         ▼ Apache Kafka (risk-alerts) 

---

# Deliverables

## Stream Processor

Create:

text spark_streaming/stream_processor.py 

Responsibilities:

- Connect to Kafka
- Subscribe to fleet-telemetry
- Parse incoming JSON
- Convert telemetry into typed Spark DataFrames
- Execute real-time analytics
- Emit risk alerts

### Success Criteria

Spark continuously consumes telemetry without errors.

---

## Event Schema Parsing

Use the canonical schema from:

text event_schema.md 

Validate and parse:

- event_id
- event_ts
- vehicle_id
- cargo_temperature
- time_left_to_destination
- sla_time_remaining
- scenario_state

### Success Criteria

Raw Kafka messages become strongly-typed Spark DataFrames.

---

## Event-Time Processing

Use:

text event_ts 

as the authoritative event timestamp.

The pipeline must process events using event-time semantics rather than arrival time.

### Success Criteria

All windows and aggregations use event timestamps.

---

## Watermarking

Apply:

text 5 minute watermark 

to tolerate late-arriving events.

The watermark should be applied before window aggregations.

### Success Criteria

Late events are handled without pipeline failure.

---

## Sliding Window Aggregations

Implement:

text Window Size: 10 minutes Slide Interval: 30 seconds 

Compute per vehicle:

### Delivery Metrics

- Current delivery buffer
- Rolling average delivery buffer

### Temperature Metrics

- Current cargo temperature
- Rolling maximum cargo temperature

### Success Criteria

Spark produces continuously updated rolling metrics.

---

## Derived Metrics

Create:

text delivery_buffer = sla_time_remaining - time_left_to_destination 

This becomes the primary risk signal.

### Success Criteria

Delivery buffer is computed for every telemetry event.

---

## Risk Tier Classification

Implement deterministic risk classification.

Initial logic:

### Green

- Healthy delivery buffer
- Temperature within acceptable range

### Yellow

- Delivery buffer deteriorating
- Temperature approaching threshold

### Red

- Critical delivery buffer
- Temperature violation

Use the threshold values defined by the simulator baseline configuration so Phase 2 and Phase 3 remain aligned.

### Success Criteria

Each event receives a risk tier.

---

## Alert Generation

Create structured alert events.

Output topic:

text risk-alerts 

Alert schema should follow:

text event_schema.md 

Required fields:

- event_id
- event_ts
- vehicle_id
- risk_tier
- delivery_buffer
- cargo_temperature
- reason

### Success Criteria

Yellow and Red events are published to Kafka.

---

## Kafka Validation

Verify through Kafka UI:

- telemetry is consumed
- alerts are generated
- alerts appear in risk-alerts

### Success Criteria

Risk alerts are visible in Kafka UI.

---

# Technical Decisions

## Risk Ownership

Spark is the sole owner of:

- risk classification
- derived metrics
- alert generation

No downstream component should recalculate risk.

---

## Event Ordering

Vehicle events should remain keyed by:

text vehicle_id 

to preserve ordering guarantees within partitions.

---

## Deterministic Logic

Risk classification must be explainable and reproducible.

Avoid:

- AI-based risk detection
- probabilistic scoring
- opaque models

The AI layer will be introduced in Phase 4 and should only explain alerts, not generate them.

---

# Validation Checklist

- [ ] Spark connects to Kafka
- [ ] Telemetry schema parsed successfully
- [ ] Event-time processing enabled
- [ ] Watermark applied
- [ ] Sliding windows functioning
- [ ] Delivery buffer computed
- [ ] Risk tiers generated
- [ ] Alerts written to risk-alerts
- [ ] Alerts visible in Kafka UI
- [ ] Spark application visible in Spark Master UI

---

# Phase 3 Outcome

At the conclusion of this phase, LogiShield will continuously transform raw fleet telemetry into actionable risk intelligence.

The platform will now possess a complete real-time analytics layer capable of identifying emerging delivery and cargo risks before they become operational failures.