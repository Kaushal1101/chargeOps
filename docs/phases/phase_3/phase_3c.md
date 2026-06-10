Phase 3C — Derived Metrics

Objective

Add the first layer of business logic to the Spark streaming pipeline by computing derived operational metrics from the parsed telemetry DataFrame.

By the end of this phase, LogiShield should be able to calculate delivery buffer-style metrics directly in Spark and display them in the stream output, without yet applying windowing, watermarking, or risk classification.

⸻

Scope

This phase focuses exclusively on:

* Deriving new metrics from parsed telemetry fields
* Adding computed columns to the Spark DataFrame
* Preserving the structured stream output
* Verifying that metric values are visible in the console and Spark UI

This phase does not include:

* Sliding windows
* Watermarking
* Risk tiering
* Alert generation
* AI remediation

⸻

Deliverables

Stream Processor Update

Update the existing Spark streaming job in:

spark_streaming/stream_processor.py

Responsibilities:

* Continue consuming parsed telemetry from Kafka
* Keep the structured DataFrame from Phase 3B
* Compute derived operational metrics as new columns
* Print the updated stream output to the console sink

Success Criteria

The console output shows both the parsed telemetry fields and the newly derived metrics.

⸻

Delivery Buffer Computation

Compute the core delivery buffer metric used throughout the rest of the project.

Formula (from event_schema.md):

    delivery_buffer = sla_time_remaining - time_left_to_destination

This value can be negative if the SLA is already breached. Add it as a new integer column named `delivery_buffer` on the structured DataFrame.

Success Criteria

Each event has a usable computed delivery buffer value.

⸻

Additional Derived Metrics

If helpful for debugging or future phases, compute lightweight supporting metrics that can be derived directly from the parsed telemetry without introducing window logic.

Keep this limited to simple per-event calculations only.

Success Criteria

Derived metrics remain simple, deterministic, and easy to inspect.

⸻

Data Flow

Telemetry Simulator
        │
        ▼
Apache Kafka
(fleet-telemetry)
        │
        ▼
Spark Structured Streaming
        │
        ▼
Parsed Telemetry DataFrame
        │
        ▼
Derived Metrics DataFrame
        │
        ▼
Console Output

⸻

Technical Decisions

Keep the Metrics Simple

This phase should focus only on per-event calculations. Do not introduce aggregation logic yet.

Reuse the Canonical Schema

All derived values must come from the canonical telemetry contract already defined in the project docs.

Preserve Readability

The goal is to make the stream output easier to reason about before introducing time-based analytics.

⸻

Validation Checklist

* Spark still consumes telemetry successfully
* Parsed rows remain intact from Phase 3B
* Derived metrics appear as new DataFrame columns
* Console output displays the computed values clearly
* Spark UI remains healthy and running

⸻

Exit Criteria

Phase 3C is complete when:

* The streaming job consumes structured telemetry from Kafka
* A per-event derived delivery metric is computed in Spark
* The metric is visible in the console output
* No parsing or calculation errors occur

⸻

How to Run

Same command as Phase 3A and 3B:

```bash
source venv/bin/activate
spark-submit \
  --master 'local[*]' \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
  spark_streaming/stream_processor.py
```

Prerequisites:
- Docker stack running (`docker-compose up -d`)
- Simulator running (`python -m simulator.simulator`)
- Spark UI available at `http://localhost:4040` while job is running

---

Phase 3C Outcome

At the conclusion of this phase, LogiShield will have the first meaningful operational signal derived from telemetry.

This creates the foundation for Phase 3D, where the pipeline can begin analyzing metrics over time using sliding windows and event-time processing.