Phase 5C — Per-Vehicle Cadence & Scheduler Realism

Objective

Refactor the simulator so different vehicles can emit telemetry at different rates rather than sharing one uniform loop cadence.

Phase 5A made the simulator concurrent, and Phase 5B removed the obvious Kafka-side bottleneck. Phase 5C now focuses on making the traffic pattern itself more realistic.

The goal is to simulate a fleet where some trucks are chatty, some are quiet, and event timing is no longer perfectly uniform.

⸻

Scope

This phase focuses exclusively on:

* Per-vehicle emission cadence
* Independent next-emit scheduling per truck
* Non-uniform fleet traffic patterns
* Preserving the canonical telemetry schema
* Maintaining compatibility with the existing Kafka topic and Spark pipeline

This phase does not include:

* Packet loss
* Delayed burst replay
* Malformed input
* New business logic
* New Spark features
* Final benchmarking report

⸻

Architecture Being Tested

Fleet
  │
  ├── Vehicle A → fast cadence
  ├── Vehicle B → medium cadence
  ├── Vehicle C → slow cadence
  └── ...
           │
           ▼
Scheduler / Next-Emit Queue
           │
           ▼
Kafka Producer
           │
           ▼
Apache Kafka (fleet-telemetry)

Key assumptions being tested:

* Not every truck needs to emit on the same tick.
* A scheduler can manage different per-vehicle intervals cleanly.
* The simulator can represent realistic fleet heterogeneity without changing the event schema.
* Spark should still consume the output normally.

⸻

Design Decision

Keep the 4 worker threads from Phase 5A and add a per-shard min-heap scheduler inside each worker. Do not replace threading with a single scheduler thread.

Why keep worker threads

A single scheduler thread at 10,000+ trucks would become the new bottleneck — one thread processing a heap of 10,000 vehicles is similar to the Phase 4D sequential loop problem. The 4-thread model from Phase 5A must be preserved to maintain throughput.

How the scheduler works

Each worker thread replaces its uniform loop with a per-shard min-heap (Python `heapq`) of `(next_emit_time: float, vehicle: Vehicle)` pairs:

1. Pop the vehicle with the earliest `next_emit_time`
2. Sleep until it is due using `stop.wait(max(0, next_emit_time - now))`
3. Emit the event
4. Push `(now + vehicle.interval_seconds, vehicle)` back onto the heap
5. Repeat

This gives each vehicle its own independent emission schedule without any cross-thread coordination.

Vehicle interval_seconds field

Each `Vehicle` needs an `interval_seconds: float` attribute assigned during `Fleet.scaled()`. This is the only change needed to the `Vehicle` class — event generation and schema remain unchanged.

⸻

Cadence Profiles To Support

Three profiles assigned randomly during `Fleet.scaled()`:

| Profile | interval_seconds | Fleet share |
|---------|-----------------|-------------|
| Fast | 0.5s | 20% |
| Normal | 1.0s | 60% |
| Slow | 2.0s | 20% |

Expected average rate: ~1.1 events/truck/sec (0.2×2 + 0.6×1 + 0.2×0.5). Slightly above the Phase 5A baseline of 1.0 events/truck/sec — benchmark comparisons should account for this.

Fast vehicles emit twice as often, useful for simulating active high-frequency telemetry sources. Slow vehicles emit every 2 seconds, simulating intermittently connected trucks.

⸻

Deliverables

Per-Vehicle Scheduling

Introduce a scheduling layer so each vehicle can have its own next emission time.

Success Criteria

The simulator no longer emits all vehicles on the same shared loop cadence.

⸻

Cadence Diversity

Support at least a few distinct vehicle cadence profiles.

Success Criteria

Some vehicles emit more often than others during the same run.

⸻

Preserved Event Contract

The output telemetry must still match the canonical schema already defined in the project docs.

Success Criteria

Kafka and Spark continue to consume the telemetry without schema changes.

⸻

Traffic Realism Observation

Observe the event stream and verify that it no longer looks perfectly uniform.

Success Criteria

The emitted telemetry pattern is visibly non-uniform and better resembles a real fleet.

⸻

Validation Checklist

* Vehicles can have different emission intervals
* A scheduler determines when each vehicle emits
* Fast, normal, and slow cadence profiles are visible
* The simulator no longer emits in a perfectly uniform loop
* Kafka output still conforms to the canonical schema
* Spark still consumes telemetry successfully
* No obvious race conditions or scheduling bugs appear

⸻

Exit Criteria

Phase 5C is complete when:

* Per-vehicle cadence is supported by the simulator
* The traffic pattern is visibly non-uniform
* Vehicles can emit at different rates without breaking the telemetry contract
* The simulator is now a more realistic load generator for later Spark benchmarking

⸻

Phase 5C Outcome

At the end of Phase 5C, LogiShield will have a scheduler-aware simulator that better resembles a real fleet rather than a perfectly regular demo stream.

This sets up later phases where timing variance, jitter, and stress conditions can be introduced more naturally for benchmarking and resilience testing.