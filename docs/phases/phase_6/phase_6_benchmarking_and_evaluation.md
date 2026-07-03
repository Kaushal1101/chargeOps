Phase 6 — Benchmarking & Evaluation Framework

Objective

Establish a repeatable and automated benchmarking framework that measures LogiShield’s performance, resilience, and alert quality before introducing Redis-backed state management.

The goal is to create a trustworthy baseline that can later be compared against Redis-enhanced versions of the architecture.

This phase does not aim to improve performance.

It aims to measure and document performance.

⸻

Scope

This phase focuses exclusively on:

* Automated benchmark collection
* Throughput measurement
* Window-processing latency measurement
* Alert quality evaluation
* Resilience evaluation
* Resource utilization measurement
* Baseline metric collection

This phase does not include:

* Redis implementation
* Dashboard implementation
* AI remediation
* New Spark analytics features

⸻

Benchmarking Principles

All benchmarks must:

* Use repeatable workloads
* Use the same machine configuration
* Use the same Spark configuration
* Use the same Kafka configuration
* Be rerunnable after Redis implementation

Metrics collected in this phase become the official pre-Redis baseline.

⸻

Architecture Being Measured

Simulator
    ↓
Kafka
    ↓
Spark
    ↓
risk-alerts

Redis will later augment this architecture but will not replace it.

⸻

Sub-Phase 6A — Benchmark Harness

Goal

Create an automated benchmark harness.

This should be a Python-based benchmark runner rather than a manual procedure.

Responsibilities

* Query Spark REST APIs
* Capture benchmark metrics
* Write benchmark results to JSON
* Standardize benchmark execution

Deliverables

* benchmark_runner.py
* benchmark_results.json

Success Criteria

Benchmark execution becomes repeatable and automated.

⸻

Sub-Phase 6B — Throughput Benchmarking

Goal

Measure pipeline throughput.

Metrics

* Events generated/sec
* Kafka ingress rate
* Spark input rate
* Spark processing rate
* Alerts/sec

Test Sizes

* 100 vehicles
* 1,000 vehicles
* 5,000 vehicles
* 10,000 vehicles

Deliverables

* Throughput report
* Bottleneck analysis

Success Criteria

Pipeline throughput limits are documented.

⸻

Sub-Phase 6C — Window Processing Latency

Goal

Measure how quickly Spark produces alerts after a window is ready for evaluation.

Important Clarification

Do NOT measure:

event_ts → alert emission

because windowing intentionally introduces delay.

Instead measure:

window_end → alert emission

This represents actual processing lag.

Metrics

* Average processing lag
* p50 processing lag
* p95 processing lag
* p99 processing lag

Deliverables

* Latency report

Success Criteria

Window-processing latency is quantified.

⸻

Sub-Phase 6D — Alert Quality Evaluation

Goal

Measure the quality of the generated alert stream.

Metrics

* Total alerts
* Alerts by tier
* State transitions detected
* Duplicate alerts
* Watermark drops
* Alert suppression rate

Deliverables

* Alert quality report

Success Criteria

Alert behavior is quantified and explainable.

⸻

Sub-Phase 6E — Resilience Evaluation

Goal

Measure system behavior under existing chaos modes.

Test Profiles

* Out-of-order events
* Delayed replay
* Packet loss

Metrics

* Stream stability
* Batch duration
* Watermark drops
* Alert correctness

Deliverables

* Resilience report

Success Criteria

Failure-mode behavior is documented.

⸻

Sub-Phase 6F — Resource Utilization

Goal

Measure resource consumption.

Metrics

* CPU usage
* Memory usage
* Spark state memory
* Kafka broker utilization
* Docker container utilization

Deliverables

* Resource utilization report

Success Criteria

Resource consumption is documented.

⸻

Benchmark Output Format

Metric	Value
Fleet Size	
Events/sec	
Spark Input Rate	
Spark Processing Rate	
Batch Duration	
Avg Processing Lag	
p95 Processing Lag	
p99 Processing Lag	
Alerts/sec	
Watermark Drops	
CPU Usage	
Memory Usage	

⸻

Phase 6 Outcome

At the conclusion of Phase 6, LogiShield will possess a complete, automated pre-Redis benchmark baseline.

This baseline becomes the reference point for evaluating Redis-backed state management, dashboards, and future architectural enhancements.