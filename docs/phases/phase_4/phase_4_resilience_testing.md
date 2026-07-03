# Phase 4 — Resilience, Fault Tolerance & Benchmarking

## Objective

Validate that LogiShield continues to operate correctly under realistic streaming conditions and increasing scale.

Up to this point, the project has proven that telemetry can be ingested, processed, windowed, and classified in real time. Phase 4 focuses on testing the assumptions behind the architecture and measuring the system's operational limits.

The goal is not to add new business features.

The goal is to answer:

- Does the pipeline remain correct when events arrive out of order?
- Does watermarking behave as expected?
- How does the system respond to delayed telemetry?
- How resilient is the pipeline to packet loss?
- How does throughput scale as fleet size increases?
- Where are the bottlenecks and failure points?

---

# Scope

This phase focuses exclusively on:

- Out-of-order event handling
- Delayed telemetry validation
- Packet loss simulation
- Fleet scaling
- Throughput benchmarking
- Architecture validation
- Performance measurement

This phase does not include:

- New business logic
- New risk classification logic
- AI remediation
- Dashboard development
- Additional Kafka topics

---

# Architecture Being Validated

The purpose of this phase is to validate the design decisions made in previous phases.

```
Telemetry Simulator
      │
      ▼
Apache Kafka (fleet-telemetry)
      │
      ▼
Spark Structured Streaming
      │
      ├── Event-Time Processing
      ├── Sliding Windows
      ├── Watermarking
      └── Risk Tiering
      │
      ▼
Apache Kafka (risk-alerts)
```

Key architectural assumptions being tested:

- `event_ts` is the source of truth for event-time processing
- Watermarking can tolerate late-arriving events
- Windowed aggregations remain correct under disorder
- Kafka partitioning supports larger fleet sizes
- Spark can process incoming telemetry faster than it arrives

---

# Chaos Injector Location

All chaos and simulation modes for Phase 4A–4C live in:

```
chaos/
```

Specifically:

- `chaos/__init__.py` — package marker
- `chaos/chaos_injector.py` — out-of-order, delayed burst, and packet loss simulation modes

The chaos injector imports `TelemetryEvent` from `simulator.models` and shares the Kafka producer setup with `simulator.simulator`. It does not modify the normal simulator — it is a separate entry point.

The `chaos/` directory was established in the Phase 1 skeleton for exactly this purpose.

---

# Sub-Phase Ordering Rationale

Correctness validation runs before scale testing. Sub-phases 4A–4C validate pipeline behavior at 3 trucks, where failures are easy to reason about. Sub-phase 4D then scales the fleet once correctness is confirmed. This prevents conflating scale bugs with correctness bugs.

```
4A — Out-of-order events      (correctness: event-time processing)
4B — Delayed burst replay     (correctness: watermark boundaries)
4C — Packet loss simulation   (correctness: degradation tolerance)
4D — Fleet scaling            (scale: throughput under load)
4E — Benchmarking & Limits    (measurement: final performance report)
```

---

# Sub-Phases

## Phase 4A — Out-of-Order Event Validation

### Goal

Introduce telemetry that arrives out of order and verify that Spark's event-time windowing remains correct.

Example:
```
12:00 → arrives first
12:02 → arrives second
12:01 → arrives third (out of order)
```

### Questions

- Do event-time windows remain correct?
- Does watermarking behave as expected?
- Do rolling metrics remain accurate?
- Does Spark rely on event timestamps rather than arrival order?

### Deliverables

- `chaos/chaos_injector.py` with out-of-order mode
- Watermark validation results
- Spark behavior documentation

---

## Phase 4B — Delayed Burst Replay

### Goal

Simulate extended telemetry interruptions followed by delayed delivery.

Example:
```
Truck enters tunnel
      ↓
No telemetry for 20 minutes
      ↓
Stored telemetry released at once
```

### Questions

- How does Spark handle delayed bursts?
- Which events are still accepted?
- Which events fall outside the watermark?
- Are aggregations still correct?

### Deliverables

- Delayed replay mode in `chaos/chaos_injector.py`
- Watermark boundary validation
- Delayed-event analysis

---

## Phase 4C — Packet Loss Simulation

### Goal

Introduce controlled packet loss into the telemetry stream.

Example progression:
```
2% → 5% → 10% → 15%
```

### Questions

- Does Spark remain healthy?
- How much does metric quality degrade?
- Does risk classification remain useful?
- Does the stream remain operational?

### Deliverables

- Packet loss mode in `chaos/chaos_injector.py`
- Stability analysis
- Accuracy observations

---

## Phase 4D — Fleet Scaling

### Goal

Scale the simulator beyond the initial development fleet and measure how the pipeline responds.

Example progression:
```
3 trucks → 100 trucks → 500 trucks → 1000 trucks
```

### Questions

- How does throughput scale?
- How does Spark batch duration change?
- Does processing remain stable?
- Does Kafka partitioning remain effective?

### Deliverables

- Configurable fleet size in `simulator/simulator.py`
- Scaling test results
- Updated performance measurements

---

## Phase 4E — Benchmarking & Limits

### Goal

Measure the performance characteristics of the complete pipeline and produce a final benchmark report.

Metrics to collect:

- Fleet size
- Events per second
- Spark input rate
- Spark processing rate
- Batch duration
- End-to-end latency
- Packet loss tolerance
- Delayed-event tolerance

### Questions

- What is the maximum sustainable throughput?
- At what point does Spark fall behind?
- What becomes the bottleneck?
- Which architectural decisions proved most valuable?

### Deliverables

- Benchmark report (format TBD — to be decided at Phase 4E planning)
- Performance summary
- Resume-ready metrics
- Architecture validation findings

---

# Success Criteria

Phase 4 is complete when:

- Out-of-order events have been tested and watermark behavior validated
- Delayed burst replay behavior has been analyzed
- Packet loss tolerance has been measured
- Fleet scaling has been validated
- Throughput limits have been benchmarked
- System bottlenecks have been identified
- Performance metrics have been documented

---

# Phase 4 Outcome

At the conclusion of Phase 4, LogiShield will have moved beyond a functional prototype and become a validated streaming system.

The project will possess measurable evidence that its Kafka partitioning, Spark windowing, event-time processing, and watermarking strategies continue to operate correctly under realistic operational conditions.

Any future AI remediation layer will consume alerts from a pipeline that has already been validated for correctness, resilience, and scale.
