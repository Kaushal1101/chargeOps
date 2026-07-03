Phase 6A — Benchmark Harness

Objective

Build a repeatable benchmark harness for LogiShield so throughput, latency, alert quality, resilience, and resource usage can be measured consistently before Redis-backed state management is introduced.

This phase is about making benchmarking automated and trustworthy. It is not about improving performance yet.

⸻

Scope

This phase focuses exclusively on:

* Benchmark runner automation
* Standardized workload execution
* Spark metrics collection
* Result capture to structured files
* Baseline benchmark reproducibility
* Alert schema fields needed for latency measurement

This phase does not include:

* Redis implementation
* New Spark business logic
* New Kafka features
* Dashboard implementation
* AI remediation
* Resilience or throughput conclusions yet

⸻

Architecture Being Measured

Simulator
    ↓
Kafka
    ↓
Spark
    ↓
risk-alerts

Phase 6A does not change this architecture.
It creates the harness used to measure it.

⸻

Benchmark Harness Decision

The benchmark harness should be a Python script rather than a manual procedure.

Why

* Easier to rerun
* Easier to compare before/after results later
* Can query Spark REST endpoints automatically
* Can write metrics to JSON for later analysis
* Stronger portfolio artifact than a manual checklist

⸻

Required Alert Schema Fields

To support benchmark timing, alerts must carry the fields needed to compute processing lag.

risk-alerts event fields

* event_id
* vehicle_id
* risk_tier
* delivery_buffer
* avg_temperature
* window_start
* window_end
* alert_ts
* reason

Notes

* event_ts is removed from the alert output schema. It is replaced by the explicit window_start and window_end fields.
* cargo_temperature is renamed to avg_temperature in the alert output schema. This reflects that the value is now an average, not a raw reading.
* window_start and window_end identify the Spark window that produced the alert.
* alert_ts records the wall-clock time when Spark emitted the alert. It is set inside write_on_transition at the moment of write, after the state transition check passes.
* The benchmark harness can then compute:

processing lag = alert_ts - window_end

This is the correct latency metric for a windowed alert pipeline.

⸻

Deliverables

Benchmark Runner

Create a Python benchmark runner that observes a live benchmark run and collects metrics.

The runner assumes Spark and the simulator are already running. It does not start or stop either process. Starting Spark via spark-submit as a subprocess adds significant complexity (JAR packaging, timing, output parsing) with no benefit at this stage.

Responsibilities:

* observe a running benchmark (Spark + simulator already started externally)
* query Spark REST API endpoints at localhost:4040/api/v1
* capture Spark input / processing rates
* capture batch duration
* capture alert throughput
* capture alert timestamps
* write results to a structured output file

Success Criteria

A benchmark run can be launched and recorded in a repeatable way.

⸻

Structured Output Files

Write benchmark results to a machine-readable format.

Preferred format:

* JSON

Optional secondary format:

* CSV

Success Criteria

Results can be compared across multiple runs without manual transcription.

⸻

Spark Metrics Collection

Collect the metrics needed for later throughput and latency analysis.

At minimum:

* input rate
* processing rate
* batch duration
* state memory usage
* watermark drops
* query uptime

Success Criteria

The benchmark runner can capture consistent Spark metrics from the running query.

⸻

Alert Timing Capture

Ensure emitted alert records contain enough information to calculate latency later.

Success Criteria

The benchmark harness can compute window_end → alert_ts latency for each alert.

⸻

Data Flow

Benchmark Runner
        │
        ▼
Spark REST API
        │
        ├── Input Rate
        ├── Processing Rate
        ├── Batch Duration
        └── Watermark / State Metrics
        │
        ▼
Alert Records
        │
        └── window_start / window_end / alert_ts

⸻

Technical Decisions

Automated Over Manual

Use a script, not manual inspection, as the primary benchmark artifact.

Stable Metric Definitions

The benchmark runner should measure only stable boundaries that will remain meaningful after Redis is added.

Window-Based Latency

Latency should be measured from window_end to alert_ts, not from raw event time to alert time.

Same Workload, Same Environment

Future benchmark phases must reuse this harness so before/after comparisons remain valid.

⸻

Validation Checklist

* Benchmark runner exists as a Python script
* Spark metrics can be queried automatically
* Results are written to JSON (or equivalent structured output)
* Alert schema includes window_start, window_end, and alert_ts
* Processing lag can be computed from alert records
* The benchmark harness can be rerun without manual transcription

⸻

Exit Criteria

Phase 6A is complete when:

* A reusable benchmark runner exists
* Spark metrics can be captured automatically
* Alert records contain the timestamps needed for latency analysis
* The benchmark output is structured and repeatable
* The harness is ready to support throughput, latency, and resilience measurements in later sub-phases

⸻

Phase 6A Outcome

At the end of Phase 6A, LogiShield will have a proper benchmarking foundation.

This creates the instrumentation needed to measure throughput, latency, resilience, and resource usage in a way that is repeatable before Redis is introduced and comparable after Redis is introduced.