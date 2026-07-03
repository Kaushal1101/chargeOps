Phase 5B — Kafka Bottleneck Mitigation & Broker Scaling

Objective

Remove Kafka as the current throughput bottleneck so LogiShield can continue toward meaningful Spark benchmarking.

Phase 5A increased simulator concurrency, but the pipeline is still plateauing because the single Docker broker and producer path are capping input rate. This phase focuses on improving Kafka-side throughput so Spark can be measured under a higher, more realistic load.

The goal is not to redesign the streaming architecture.
The goal is to increase the amount of telemetry the current local Kafka setup can accept before becoming the limiting factor.

⸻

Scope

This phase focuses exclusively on:

* Kafka producer throughput tuning
* Kafka broker resource tuning
* Topic partition utilization review
* Throughput validation after tuning
* Identifying whether a single-broker setup is still sufficient
* Preparing the pipeline for later Spark-focused benchmarking

This phase does not include:

* Per-vehicle cadence scheduling
* Jitter and natural reordering
* Packet loss profiles
* Malformed input
* AI remediation
* Final Spark benchmark reporting

⸻

Current Problem

The simulator can now generate higher load, but Spark input rate is still plateauing around the same ceiling because Kafka is the next bottleneck.

Observed symptoms:

* Increasing fleet size does not significantly increase Spark input rate
* Spark processing rate remains above input rate
* Kafka broker is running as a single Docker broker
* Producer-side batching / broker throughput appears to be limiting the stream

This means Spark cannot yet be benchmarked fairly.

⸻

Architecture Being Tuned

Threaded Simulator
      │
      ▼
Kafka Producer
      │
      ▼
Single Docker Kafka Broker
      │
      ▼
Spark Structured Streaming

Key assumptions being tested:

* The producer can batch more efficiently.
* The broker can sustain higher ingress with tuning.
* Topic partitioning is sufficient for the current workload.
* A single-broker Docker setup can be pushed further before requiring a cluster.

⸻

Tuning Strategy

Improve throughput in the least disruptive order first.

Step 1 — Producer Behavior

Reduce unnecessary producer-side overhead.

Focus on:

* avoiding frequent flushes
* allowing natural batching
* increasing linger time where appropriate
* increasing batch size where appropriate
* enabling compression if beneficial
* keeping one shared producer per simulator process

Step 2 — Broker Resources

Increase the resources available to the single Kafka broker in Docker.

Focus on:

* CPU allocation
* memory allocation
* broker-side stability under higher throughput

Step 3 — Topic Partition Review

Verify that the current partition count is not artificially limiting throughput.

Focus on:

* whether fleet-telemetry has enough partitions
* whether the partition count matches the intended scale path
* whether partitioning remains compatible with Spark consumption

Step 4 — Multi-Broker Decision Point

Only if the previous steps are not sufficient, evaluate whether the project should move to a multi-broker Kafka setup.

This should be treated as a fallback, not the default next step.

⸻

Deliverables

Producer Throughput Tuning

Adjust the simulator producer path so events are batched more efficiently and sent with less overhead.

Success Criteria

The simulator can sustain a higher Kafka publish rate than before.

⸻

Broker Resource Tuning

Increase the available broker resources in the local Docker environment.

Success Criteria

Kafka continues to run stably under higher ingress pressure.

⸻

Partition Review

Check whether the current partition count is appropriate for the upgraded load profile.

Success Criteria

Partitioning is no longer the obvious limiter before Spark.

⸻

Throughput Validation

Rerun the fleet scaling tests after each tuning change and record the new ceiling.

Success Criteria

The Spark input rate increases meaningfully beyond the previous plateau.

⸻

Metrics To Capture

For each tuning change, record:

* Kafka publish rate
* Spark input rate
* Spark processing rate
* Batch duration
* Broker CPU / memory usage if available
* Producer settings used
* Topic partition count

⸻

Questions To Answer

* Is the ceiling caused by producer overhead or broker capacity?
* Did batching improvements increase throughput?
* Did larger broker resources help?
* Are partitions sufficient for the current scale target?
* Is Kafka still the limiting factor after tuning?

⸻

Validation Checklist

* Producer flush behavior reviewed
* Producer batching tuned
* Broker CPU / memory allocation reviewed or increased
* Topic partition count reviewed
* Load test rerun after each change
* New throughput ceiling recorded
* Bottleneck source narrowed down further

⸻

Exit Criteria

Phase 5B is complete when:

* Kafka throughput has been improved or its ceiling has been clearly characterized
* The pipeline can sustain more load than before
* It is clear whether Kafka remains the bottleneck
* The project is ready to move to the next simulator realism phase without confusing Kafka limits with Spark limits

⸻

Phase 5B Outcome

At the end of Phase 5B, LogiShield should no longer be blocked by an obvious local Kafka throughput ceiling.

This creates a cleaner foundation for later phases that introduce per-vehicle cadence realism, jitter, and final Spark benchmarking under a better load harness.