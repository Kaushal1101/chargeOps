Phase 5 — Load Realism & Benchmarking

Objective

Evolve LogiShield from a clean streaming demo into a realistic benchmark harness that can stress Spark under heterogeneous traffic patterns, failure modes, and increasing load.

Up to this point, the project has proven correctness under normal conditions, disorder, delay, packet loss, and scaling. Phase 5 focuses on making the simulator and benchmarking pipeline realistic enough to measure Spark meaningfully.

The goal is not to add AI remediation yet.

The goal is to answer:

* Do all trucks behave identically, or do some emit more often than others?
* Does the simulator produce realistic variation in traffic rate and ordering?
* Can the pipeline tolerate malformed or corrupted inputs?
* Can Spark still keep up when traffic is irregular rather than neatly ordered?
* What is the true throughput limit once the load generator is no longer the bottleneck?

⸻

Scope

This phase focuses exclusively on:

* Simulator realism improvements
* Per-truck cadence variation
* Jitter and reordering in normal traffic
* Benchmark harness design
* Spark throughput testing under realistic load

This phase does not include:

* Packet loss profiles — chaos injector remains the entry point for packet loss on demand
* Malformed or corrupted telemetry — deferred to Phase 6
* AI remediation
* New business logic
* New risk classification logic
* New Spark analytics features
* Dashboard development

⸻

Architecture Being Validated

The purpose of this phase is to validate that the simulator is capable of producing a realistic enough workload to benchmark Spark fairly.

Load Profiles / Chaos Modes
        │
        ▼
Telemetry Generation + Scheduling
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

Key assumptions being tested:

* The simulator can produce heterogeneous truck behavior.
* Spark benchmarking is only meaningful if the load generator itself is not the bottleneck.
* Realistic streaming stress includes irregular rates, loss, jitter, and malformed inputs.
* Kafka and Spark should be measured under multiple load profiles rather than one neat traffic pattern.

⸻

Sub-Phases

Phase 5A — Threaded Load Model Refactor

Goal

Refactor the simulator to publish telemetry concurrently using worker threads and fleet sharding, replacing the single sequential loop that bottlenecked at ~2,000 events/sec in Phase 4D.

Design Decisions

* 4 worker threads, each owning a deterministic shard of the fleet
* Single shared KafkaProducer across all threads (thread-safe, better batching)
* threading.Event stop flag for clean shutdown on Ctrl+C
* Per-shard cadence — each thread loops over its vehicles and sleeps the remainder of the second

Questions

* Does threading push the simulator past the ~2,000 events/sec ceiling?
* Does vehicle state remain thread-confined with no race conditions?
* Does the output still match the canonical telemetry schema?

Deliverables

* Threaded simulator with fleet sharding
* Clean shutdown via threading.Event
* Throughput comparison against the sequential baseline

⸻

Phase 5B — Per-Vehicle Cadence and Scheduling Abstraction

Goal

Introduce independent per-vehicle emission schedules so different trucks can emit at different rates.

This requires a dedicated scheduler with per-vehicle next-emit timestamps — a heap or priority queue where the simulator always fires the vehicle whose next-emit time is soonest.

Questions

* Can some trucks emit more frequently than others?
* Does the scheduler remain stable under large fleet sizes?
* Does Spark handle heterogeneous input rates correctly?

Deliverables

* Per-vehicle cadence configuration
* Priority queue or equivalent scheduling abstraction
* Fleet profiles with heterogeneous emission rates

⸻

Phase 5C — Jitter and Natural Reordering Profiles

Goal

Introduce realistic timing variation so truck events do not always arrive in a perfectly repeating order.

This is distinct from Phase 4A chaos injection — Phase 4A applied explicit timestamp backdating on top of a clean simulator. Phase 5C bakes natural jitter into the simulator's own emission timing so the stream is inherently irregular rather than artificially disordered.

Questions

* Does Spark still behave correctly when event order varies naturally?
* Can the simulator produce timing jitter without breaking the canonical event contract?
* Does the stream still look realistic when trucks are not emitted in a fixed sequence?

Deliverables

* Jittered emission profiles
* Reordering-capable scheduling logic
* Observations of how Spark handles irregular arrival patterns

⸻

Phase 5D — Benchmark Harness & Spark Limits

Goal

Use the improved simulator to measure Spark’s real throughput limits under multiple load profiles.

Metrics To Collect

* Fleet size
* Per-truck emission rate
* Events per second
* Spark input rate
* Spark processing rate
* Batch duration
* State memory growth
* Watermark behavior
* Error rate under malformed input

Questions

* At what point does the load generator stop being the bottleneck?
* At what point does Spark begin to fall behind?
* Which load profile is most stressful?
* How does the system behave under combined stress conditions?

Deliverables

* Benchmark report
* Load profile comparison
* Throughput and latency summary
* Bottleneck analysis
* Resume-ready metrics

⸻

Success Criteria

Phase 5 is complete when:

* The simulator can produce non-uniform truck behavior
* Jitter and reordering are supported
* Spark has been benchmarked against multiple realistic load profiles
* The actual bottleneck(s) have been identified
* The resulting metrics are documented clearly

⸻

Phase 5 Outcome

At the conclusion of Phase 5, LogiShield will have a realistic load-generation and benchmarking pipeline that can stress Spark under varied conditions rather than only a perfectly regular stream.

This creates a trustworthy foundation for the AI remediation layer later, because the alert stream will have already been validated under realistic operating pressure.