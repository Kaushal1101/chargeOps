# Phase 2 — Telemetry Generation

## Objective

Build the telemetry simulation layer that acts as the source of truth for the entire pipeline.

By the end of this phase, the system should continuously generate realistic fleet telemetry events and successfully publish them into Kafka for downstream processing.

---

# Scope

This phase focuses exclusively on:

- Telemetry event generation
- Kafka producer implementation
- Event schema validation
- Scenario injection
- End-to-end Kafka publishing

This phase does not include:

- Spark analytics
- Risk tiering
- Watermarking
- AI agents
- Benchmarking

---

# Deliverables

## Telemetry Simulator

Create:

text simulator/simulator.py 

Responsibilities:

- Generate fleet telemetry events
- Produce events continuously
- Publish events to Kafka
- Follow the canonical schema defined in event_schema.md

### Success Criteria

The simulator can continuously publish valid telemetry events to the fleet-telemetry topic.

---

## Event Schema Implementation

All generated events must conform to the canonical telemetry schema, found in docs/event_schema.md

### Success Criteria

Every event successfully validates against the expected schema before publishing.

---

## Kafka Producer

Implement a Kafka producer using kafka-python.

Responsibilities:

- Connect to Kafka through the host listener
- Serialize events to JSON
- Publish to fleet-telemetry
- Handle connection failures gracefully

**Emit rate:** 1 event per second per vehicle (3 trucks = 3 events/sec total).

### Success Criteria

Messages appear in Kafka UI and are distributed across topic partitions.

---

## Scenario Injection Engine

Implement deterministic operational scenarios.

Each vehicle is initialized with per-vehicle threshold values (`sla_buffer_threshold`, `cargo_temp_threshold`) that are embedded in every event it produces. This makes each event self-describing — Spark evaluates risk on a single row with no stateful join required.

The `scenario_state` field on each event reflects which scenario the simulator is actively injecting. It is the simulator's control variable, not Spark's output.

### Per-Vehicle Threshold Initialization

At simulator startup, assign each truck its thresholds. These values are held in memory by the simulator and stamped onto every event that truck produces.

Suggested defaults (can vary per vehicle to demonstrate per-vehicle support):

| Vehicle | `cargo_temp_threshold` | `sla_buffer_threshold` |
|---------|----------------------|----------------------|
| TRUCK_101 | 5.0°C | 30 min |
| TRUCK_102 | 6.0°C | 25 min |
| TRUCK_103 | 4.5°C | 35 min |

### Scenario 1 — Green State

Normal operating conditions.

Characteristics:

- `delivery_buffer` comfortably above `sla_buffer_threshold`
- `cargo_temperature` below `cargo_temp_threshold`
- No SLA risk

### Scenario 2 — Yellow State

Emerging delivery risk.

Characteristics:

- `delivery_buffer` approaching `sla_buffer_threshold` (within threshold but shrinking)
- `cargo_temperature` approaching `cargo_temp_threshold`
- Potential future SLA breach

### Scenario 3 — Red State

Critical operational failure.

Characteristics:

- `delivery_buffer` below `sla_buffer_threshold`
- `cargo_temperature` exceeding `cargo_temp_threshold`
- Immediate intervention required

### Success Criteria

The simulator can intentionally transition trucks through Green → Yellow → Red states.

---

## Multi-Vehicle Support

Support multiple simulated trucks.

Initial fleet:

text TRUCK_101 TRUCK_102 TRUCK_103 

Each truck should maintain independent telemetry values and scenario progression.

### Success Criteria

Multiple vehicles generate events simultaneously.

---

## Kafka Validation

Use Kafka UI to verify:

- Events are arriving
- Partitions are receiving data
- Event payloads match the schema

### Success Criteria

Telemetry events are visible and inspectable through Kafka UI.

---

# Data Flow

text Telemetry Simulator         │         ▼ Apache Kafka (fleet-telemetry) 

At the end of Phase 2, Kafka should contain a continuous stream of telemetry events ready for Spark consumption.

---

# Technical Decisions

## Event Ownership

The simulator is the sole owner of telemetry events.

It is responsible for:

- Event creation
- Event timestamps
- Scenario state transitions

No downstream service should modify telemetry events.

---

## Event Time

Each event must contain:

text event_ts 

This timestamp will later be used by Spark for:

- Event-time windows
- Watermarking
- Late-data handling

Accurate timestamps are required for future phases.

---

## Event ID Strategy

Each event must be assigned a unique `event_id` using `uuid4`.

UUIDs avoid the shared-counter coordination problem that padded sequential IDs would require across multiple vehicles running in parallel. They are also more representative of a real distributed system.

Format: standard UUID string, e.g. `"3f4a1b2c-8e9d-4f5a-b6c7-1d2e3f4a5b6c"`.

---

## Partition Strategy

Kafka messages should use:

text vehicle_id 

as the message key.

This ensures:

- Consistent partition assignment
- Per-vehicle event ordering
- Future scalability

---

# Exit Criteria

Phase 2 is complete when:

- [ ] simulator.py exists
- [ ] Kafka producer successfully connects
- [ ] Telemetry events are published continuously
- [ ] Events follow the canonical schema
- [ ] Green scenario implemented
- [ ] Yellow scenario implemented
- [ ] Red scenario implemented
- [ ] Multiple vehicles supported
- [ ] Kafka UI shows live event traffic
- [ ] Events are partitioned using vehicle_id

---

# Phase 2 Outcome

At the conclusion of this phase, LogiShield will have a fully operational telemetry generation layer producing realistic streaming events into Kafka.

The pipeline will now possess a live data source, enabling Phase 3 to focus entirely on real-time analytics, windowing, and risk classification using Spark Structured Streaming.