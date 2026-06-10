# Phase 3E — Risk Tiering & Alert Generation

## Objective

Add the final intelligence layer to the Spark streaming pipeline by classifying each truck into Green, Yellow, or Red risk tiers and emitting structured alert events for high-risk conditions.

By the end of this phase, LogiShield should transform rolling telemetry metrics into deterministic operational decisions and publish those decisions to the `risk-alerts` Kafka topic for downstream AI remediation.

---

# Scope

This phase focuses exclusively on:

- Deterministic risk classification
- Mapping rolling telemetry metrics to risk tiers
- Generating structured alert events
- Publishing Yellow and Red alerts to Kafka
- Verifying alert output in Kafka UI

This phase does not include:

- AI remediation logic
- New windowing logic
- New schema design
- Chaos engineering
- Benchmarking

---

# Deliverables

## Stream Processor Update

Update the existing Spark streaming job in:

```
spark_streaming/stream_processor.py
```

Two changes are required:

1. **Add threshold columns to the window aggregation** — `sla_buffer_threshold` and `cargo_temp_threshold` are currently dropped during the Phase 3D windowed aggregation. They must be recovered by adding `first()` aggregations so the risk classifier has per-vehicle thresholds available.

2. **Add risk classification and Kafka alert output** — Apply deterministic tier logic to the windowed DataFrame and write Yellow and Red alerts to `risk-alerts`.

---

# Fix: Threshold Columns in Window Aggregation

The Phase 3D `windowed` aggregation must be updated to include:

```python
first(col("sla_buffer_threshold")).alias("sla_buffer_threshold"),
first(col("cargo_temp_threshold")).alias("cargo_temp_threshold"),
```

`first()` is safe here because `sla_buffer_threshold` and `cargo_temp_threshold` are constant per vehicle — they are set at simulator startup and stamped on every event unchanged.

After this fix, the windowed DataFrame will contain:

| Column | Type |
|--------|------|
| window | struct (start, end) |
| vehicle_id | string |
| avg_delivery_buffer | double |
| max_cargo_temperature | double |
| sla_buffer_threshold | integer |
| cargo_temp_threshold | double |

---

# Risk Tier Classification Logic

Apply deterministic classification using a `when/otherwise` expression.

Rules, evaluated in order:

| Tier | Condition |
|------|-----------|
| RED | `avg_delivery_buffer < 0` OR `max_cargo_temperature > cargo_temp_threshold` |
| YELLOW | `avg_delivery_buffer < sla_buffer_threshold` OR `max_cargo_temperature > cargo_temp_threshold * 0.9` |
| GREEN | all other conditions |

Add `risk_tier` as a new column via `withColumn()`.

---

# Alert Schema

Filter to Yellow and Red rows only. Produce structured alert records matching the canonical schema in `event_schema.md`:

| Field | Source |
|-------|--------|
| `event_id` | `uuid4()` generated per alert via `udf` |
| `event_ts` | `window.end` cast to string (ISO-8601) |
| `vehicle_id` | `vehicle_id` |
| `risk_tier` | `risk_tier` |
| `delivery_buffer` | `avg_delivery_buffer` cast to integer |
| `cargo_temperature` | `max_cargo_temperature` |
| `reason` | Generated string — see format below |

### `reason` field format

Use a `when/otherwise` expression to populate `reason`:

- RED (buffer): `"Delivery buffer breached SLA: {avg_delivery_buffer:.0f}min"`
- RED (temperature): `"Cargo temperature exceeded threshold: {max_cargo_temperature:.2f}C"`
- YELLOW (buffer): `"Delivery buffer below threshold: {avg_delivery_buffer:.0f}min"`
- YELLOW (temperature): `"Cargo temperature approaching threshold: {max_cargo_temperature:.2f}C"`

For simplicity, use a single reason string per alert. If both conditions are RED, the buffer reason takes priority.

---

# Kafka Alert Output

Write alert records to Kafka topic `risk-alerts`.

- Serialize the alert DataFrame as JSON using `to_json(struct(*))` 
- Use `vehicle_id` as the Kafka message key
- Connect to `localhost:9093`
- Output mode: `update`
- Trigger: `processingTime="5 seconds"`

The console sink from previous phases is removed. Validation is done via Kafka UI at `http://localhost:8080`.

---

# Fan-Out Architecture

Spark is a single writer to `risk-alerts`. Downstream consumers read independently from the same topic at their own offsets:

```
Spark → risk-alerts (Kafka topic)
              │
        ┌─────┴──────┐
        ▼            ▼
   AI Agent     Future consumers
   (Phase 4)    (monitoring, etc.)
```

No Spark-side fan-out is needed. Kafka's consumer group model handles isolation between readers.

---

# Data Flow

```
Telemetry Simulator
      │
      ▼
Apache Kafka (fleet-telemetry)
      │
      ▼
Spark Structured Streaming
      │
      ▼
Parsed Telemetry + delivery_buffer
      │
      ▼
Watermarked Sliding Window + threshold columns
      │
      ▼
Risk Tier Classification (GREEN / YELLOW / RED)
      │
      ▼
Filter: YELLOW and RED only
      │
      ▼
Apache Kafka (risk-alerts)
```

---

# Technical Decisions

## Deterministic Rules Only

Risk detection must remain explainable and reproducible. No AI, probabilistic scoring, or opaque heuristics in this phase.

## Spark Owns Detection

Spark is the sole owner of risk classification. The AI agent in Phase 4 consumes alerts and explains their business impact — it does not generate risk decisions.

## `first()` for Per-Vehicle Thresholds

`sla_buffer_threshold` and `cargo_temp_threshold` are constant per vehicle. Using `first()` in the window aggregation safely recovers these values without affecting correctness.

## Console Sink Removed

From this phase onward, Spark writes to Kafka only. Validation moves to Kafka UI. This reflects the production architecture where Kafka is the integration layer between pipeline components.

---

# How to Run

Same command as all previous Phase 3 sub-phases:

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
- Validate output via Kafka UI at `http://localhost:8080` → `risk-alerts` topic

---

# Validation Checklist

- [ ] Spark still consumes telemetry successfully
- [ ] `sla_buffer_threshold` and `cargo_temp_threshold` appear in windowed output
- [ ] `risk_tier` column is computed deterministically
- [ ] GREEN rows are filtered out before alert generation
- [ ] Yellow and Red alerts appear in `risk-alerts` Kafka topic
- [ ] Alert payloads contain all fields from the canonical schema
- [ ] `reason` field is populated with a meaningful string
- [ ] Kafka UI shows live alert traffic in `risk-alerts`
- [ ] Spark UI remains healthy and stable

---

# Exit Criteria

Phase 3E is complete when:

- The streaming job classifies windowed telemetry into GREEN, YELLOW, and RED tiers
- Alert events are generated for YELLOW and RED states only
- Alerts are published to the `risk-alerts` Kafka topic
- Alert payloads match the schema defined in `event_schema.md`
- No classification or write errors occur

---

# Phase 3E Outcome

At the conclusion of this phase, LogiShield will have a complete real-time analytics engine.

The pipeline continuously transforms raw telemetry into structured risk alerts, which sets up Phase 4 where the AI agent consumes those alerts and generates mitigation briefings.
