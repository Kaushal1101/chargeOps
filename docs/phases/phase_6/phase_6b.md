Phase 6B — Throughput Benchmarking

Objective

Measure how much telemetry LogiShield can sustain end-to-end under fixed, repeatable load conditions.

Phase 6A created the benchmark harness. Phase 6B uses that harness to measure throughput across the current architecture before Redis is introduced.

The goal is to determine how the pipeline behaves as fleet size and event volume increase, and to document the current throughput ceiling in a repeatable way.

⸻

Scope

This phase focuses exclusively on:

* End-to-end throughput measurement
* Fleet-size scaling tests
* Spark input and processing rate measurement
* Kafka ingress observation
* Alert throughput measurement
* Bottleneck identification
* Structured result capture

This phase does not include:

* Redis implementation
* New Spark analytics
* Dashboard implementation
* AI remediation
* Latency analysis beyond throughput-related timing
* Resilience chaos testing

⸻

Architecture Being Measured

Simulator
    ↓
Kafka
    ↓
Spark
    ↓
risk-alerts

This phase does not change the architecture.
It measures how much load this architecture can carry.

⸻

Benchmark Harness Dependency

Phase 6B depends on the benchmark runner created in Phase 6A.

The runner should:

* observe a running pipeline
* query Spark REST API metrics
* record throughput-related measurements
* write results to structured output

No process orchestration should be added in this phase.

⸻

Test Matrix

Run the same benchmark profile at multiple fleet sizes.

Suggested sizes:

* 100 vehicles
* 1,000 vehicles
* 5,000 vehicles
* 10,000 vehicles

If the machine or broker becomes saturated earlier, stop at the failure point and record it clearly.

⸻

Metrics To Capture

For each run, record:

* Fleet size
* Expected events per second
* Actual events generated per second
* Kafka ingress rate
* Spark input rate
* Spark processing rate
* Alerts per second
* Batch duration
* Query uptime
* Whether the pipeline remains stable

Optional if available:

* CPU usage
* Memory usage
* Kafka broker utilization
* Spark state memory usage

⸻

Deliverables

Throughput Benchmark Runs

Execute the benchmark harness against each fleet size and record the observed rates.

Success Criteria

Throughput measurements are collected for each configured fleet size.

⸻

Bottleneck Identification

Determine which component becomes the limit first.

Possible bottlenecks include:

* Python simulator / load generator
* Kafka producer path
* Kafka broker
* Spark input or processing path

Success Criteria

The limiting component is documented for each major scale point.

⸻

Results File

Write all benchmark outputs to a structured file format such as JSON.

Success Criteria

The benchmark results can be compared across runs without manual transcription.

⸻

Benchmark Method

Use the same run conditions for each fleet size:

* same Spark configuration
* same Kafka configuration
* same topic partitioning
* same machine / Docker resource allocation
* same benchmark duration

This keeps the results comparable before Redis is added.

⸻

Questions To Answer

* How does Spark input rate change with fleet size?
* Does Spark processing rate stay above input rate?
* At what fleet size does the pipeline stop scaling linearly?
* Which component becomes the first bottleneck?
* How stable is the pipeline at the maximum sustainable load?

⸻

Validation Checklist

* Benchmark runner can execute throughput runs consistently
* Fleet sizes are tested at multiple scale points
* Spark input and processing rates are recorded
* Kafka ingress is observed
* Alert throughput is recorded
* The first bottleneck is identified
* Results are written to a structured output file
* Runs are comparable across repeated executions

⸻

Exit Criteria

Phase 6B is complete when:

* Throughput data has been collected across multiple fleet sizes
* The current throughput ceiling is clearly documented
* The first limiting component in the architecture is identified
* Results are saved in a repeatable form for future comparison after Redis

⸻

Phase 6B Outcome

At the end of Phase 6B, LogiShield will have a clear throughput baseline for the current pre-Redis architecture.

This baseline becomes a reference point for later Redis comparisons and for understanding how far the existing Kafka + Spark pipeline can scale before a different component becomes the bottleneck.