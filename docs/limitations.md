# Known Limitations

This document records known constraints and simplifications in the LogiShield Pipeline.
These are deliberate tradeoffs, not bugs — each is noted here so they are visible during
interviews, code review, or future development.

---

## 1. Simulator-Driven Risk Patterns

**What this means:**
Alert patterns in the pipeline are produced by a scripted injection cycle in the simulator,
not by emergent behavior from real charger physics. Each charger advances through a
deterministic 15-step cycle: GREEN (steps 0–4) → YELLOW (steps 5–9) → RED (steps 10–14),
then repeats. The `Scenario.generate_values()` method produces temperature and session buffer
values calibrated to cross each risk threshold at the right phase.

**Consequence for analytics:**
Statistics derived from the telemetry stream (Phase 11) reflect this cycle. Regional
distributions, connector type breakdowns, and average temperatures will be stable and
deterministic rather than organically variable. In a production system, these patterns
would emerge from real hardware behavior, demand variation, and environmental conditions.

**Why this is acceptable:**
The purpose of the simulator is to exercise the streaming pipeline — windowed aggregations,
watermarking, stateful deduplication, Redis materialization. The pipeline components are
agnostic to whether the values are real or injected. The distributed systems architecture
is valid regardless of the data source.

---

## 2. No Historical Data Store

**What this means:**
Redis stores only the current alert state per charger, with a 7-minute TTL. There is no
time-series database, no event archive, and no persistent query store. Analytics in Phase 11
reflect what is happening in the current Spark micro-batch — not trends over hours or days.

**Consequence:**
Questions like "which region had the most alerts this week?" or "has charger X been
degrading over time?" cannot be answered from the current system. Each pipeline restart
begins from a clean state.

**Why this is acceptable:**
The project demonstrates real-time stream processing, not batch analytics. Adding a
historical store (e.g. ClickHouse, DuckDB, or Parquet sink) is a natural Phase 12+
extension and is architecturally straightforward given that Spark already owns the
derived metrics.

---

## 3. Single-Node Local Infrastructure

**What this means:**
Kafka, Spark, and Redis all run on a single Docker host on a development machine.
There is no replication, no fault tolerance, and no horizontal scaling.

**Consequence:**
Throughput ceilings documented in the Phase 6B benchmark (~6,500 events/sec at 10,000
chargers) reflect local hardware constraints, not the architecture's true ceiling.
A multi-broker Kafka cluster with a distributed Spark cluster would scale orders of
magnitude higher.

**Why this is acceptable:**
The architecture is designed to be horizontally scalable. The single-node setup is
a deployment constraint, not a design constraint. Phase 12 (deployment) targets
a multi-node environment.

---

## 4. Chaos Injector Not Updated for EV Domain

**What this means:**
`chaos/chaos_injector.py` still references the pre-Phase 9 class names (`Fleet`, `Vehicle`)
from the truck delivery domain. It will not run against the current EV simulator.

**Why not yet fixed:**
Chaos testing was not in scope for Phases 9–11. The injector will be updated when chaos
testing resumes in a later phase.
