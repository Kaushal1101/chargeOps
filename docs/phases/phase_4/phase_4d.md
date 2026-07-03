Phase 4D — Fleet Scaling

Objective

Validate that LogiShield continues to operate correctly as fleet size increases and telemetry volume grows.

Previous Phase 4 sub-phases focused on correctness under disorder, delay, and packet loss. This phase focuses on scale.

The goal is to determine how the existing Kafka + Spark architecture behaves when the number of simulated vehicles increases significantly.

⸻

Scope

This phase focuses exclusively on:

* Fleet scaling
* Increased telemetry volume
* Kafka throughput under load
* Spark throughput under load
* Batch duration analysis
* Partition utilization
* Early bottleneck identification

This phase does not include:

* New business logic
* New risk classification logic
* Packet loss
* Delayed replay
* Out-of-order events
* Final benchmarking report

⸻

Architecture Being Tested

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

The key assumptions under test are:

* Kafka can ingest increasing telemetry volume.
* Spark processing rate remains above input rate.
* Windowed aggregations remain stable under larger workloads.
* Existing partitioning strategy remains effective.
* Batch duration scales predictably.

⸻

Scaling Strategy

Fleet size should become configurable.

The simulator should no longer assume:

3 trucks

Instead, it should support:

3 trucks
↓
100 trucks
↓
500 trucks
↓
1000 trucks

The same event schema, risk logic, and Spark pipeline should remain unchanged.

Only scale should change.

⸻

Deliverables

Configurable Fleet Size

Update the simulator architecture so fleet size can be configured without manually creating vehicle definitions.

Success Criteria

Fleet size can be changed through configuration rather than code edits.

⸻

Scaling Test Runs

Run the pipeline at multiple fleet sizes.

Suggested progression:

Test 1

3 vehicles

Baseline.

Test 2

100 vehicles

First meaningful scale increase.

Test 3

500 vehicles

Moderate scale.

Test 4

1000 vehicles

Large-scale validation.

Additional scale points may be added if the machine remains healthy.

⸻

Spark Observation

Observe the Spark UI during each run.

Record:

* Input Rate
* Processing Rate
* Batch Duration
* State Memory Usage
* Rows Updated
* Watermark Stability

Success Criteria

Spark remains healthy and responsive as fleet size increases.

⸻

Kafka Observation

Observe Kafka UI during each run.

Record:

* Topic throughput
* Partition activity
* Message volume

Success Criteria

Kafka remains stable and continues accepting telemetry without issue.

⸻

Measurements To Capture

For every fleet size:

Metric	Description
Fleet Size	Number of simulated vehicles
Events/sec	Expected telemetry generation rate
Spark Input Rate	Events received per second
Spark Processing Rate	Events processed per second
Batch Duration	Processing time per micro-batch
State Memory	Streaming state usage
Watermark Gap	Watermark behavior under load

Store the results in a simple benchmark log for use in Phase 4E.

⸻

Questions To Answer

Scaling

* How does throughput change as fleet size grows?
* Does Spark continue processing faster than events arrive?

Windowing

* Do windowed aggregations remain stable?
* Does state memory grow linearly?

Partitioning

* Are the existing Kafka partitions sufficient?
* Do any partitions appear overloaded?

Bottlenecks

* What becomes the first constraint?
    * CPU?
    * Memory?
    * Spark state?
    * Kafka throughput?

⸻

Validation Checklist

* Fleet size is configurable
* 100-vehicle test completed
* 500-vehicle test completed
* 1000-vehicle test completed
* Spark remains healthy
* Kafka remains healthy
* Input and processing rates recorded
* Batch duration recorded
* Early bottlenecks identified
* Results documented for Phase 4E

⸻

Exit Criteria

Phase 4D is complete when:

* The simulator supports configurable fleet sizes
* The pipeline has been validated at multiple scales
* Throughput characteristics are understood
* Potential bottlenecks have been identified
* Benchmark data has been collected for final analysis

⸻

Phase 4D Outcome

At the end of Phase 4D, LogiShield will have demonstrated that the architecture can scale beyond the original development fleet.

The project will possess concrete evidence of how Kafka, Spark, windowing, and watermarking behave under increasing telemetry volume, providing the foundation for Phase 4E benchmarking and performance analysis.