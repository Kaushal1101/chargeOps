# Phase 3D — Sliding Windows & Watermarking

## Objective

Add time-based stream analysis to the Spark pipeline by computing rolling metrics over event time and handling late-arriving telemetry safely.

By the end of this phase, LogiShield should be able to aggregate parsed telemetry across sliding windows and tolerate out-of-order events using watermarking, without yet performing risk tier classification or alert generation.

---

# Scope

This phase focuses exclusively on:

- Event-time processing
- Sliding window aggregations
- Watermarking
- Per-vehicle rolling metrics
- Validation of late-event behavior

This phase does not include:

- Risk tiering
- Alert generation
- AI remediation
- New schema design
- Simulator changes beyond emitting valid timestamps

---

# Deliverables

## Stream Processor Update

Update the existing Spark streaming job in:

```
spark_streaming/stream_processor.py
```

Responsibilities:

- Continue consuming the structured telemetry stream from Phase 3B
- Continue using the derived metric from Phase 3C
- Apply watermarking to `event_ts`
- Compute rolling window metrics per vehicle
- Keep the output visible in the console sink

---

# Window Configuration

| Parameter | Value |
|-----------|-------|
| Window size | 10 minutes |
| Slide interval | 30 seconds |
| Watermark | 5 minutes |
| Event-time column | `event_ts` (already `TimestampType` from Phase 3B) |

---

# Rolling Metrics

Compute these two aggregations per vehicle per window. Column names must match `event_schema.md` exactly:

| Column | Formula |
|--------|---------|
| `avg_delivery_buffer` | `avg(delivery_buffer)` |
| `max_cargo_temperature` | `max(cargo_temperature)` |

Group by: `vehicle_id` and the sliding window on `event_ts`.

---

# Output Mode Change

**This phase requires changing the console sink output mode from `append` to `update`.**

`append` mode is incompatible with windowed aggregations in Spark Structured Streaming and will throw a runtime error. `update` mode emits updated window results as new data arrives and is the correct choice for watermarked sliding windows.

---

# Watermarking

Apply a 5-minute watermark on `event_ts` before the window aggregation:

```python
.withWatermark("event_ts", "5 minutes")
```

This allows the pipeline to tolerate events arriving up to 5 minutes late without breaking the aggregation. Events outside the watermark boundary are discarded.

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
Parsed Telemetry + delivery_buffer (Phase 3B/3C)
      │
      ▼
Watermark on event_ts (5 min)
      │
      ▼
Sliding Window (10 min / 30 sec slide) grouped by vehicle_id
      │
      ▼
avg_delivery_buffer, max_cargo_temperature per vehicle
      │
      ▼
Console Output (update mode)
```

---

# Technical Decisions

## Event Time Is the Source of Truth

Use `event_ts` (already cast to `TimestampType` in Phase 3B) as the authoritative time reference for all rolling calculations. Do not use Kafka's `timestamp` column.

## Output Mode: `update` Not `append`

Spark Structured Streaming does not support `append` mode for aggregations with watermarking. `update` mode emits a new row for each window group each time it is updated, which is the correct behavior for rolling metrics.

## Watermark Before Window

The `.withWatermark()` call must be applied to the DataFrame before the `.groupBy(window(...))` call. Applying it after will not work correctly.

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
- Spark UI available at `http://localhost:4040` while job is running

**Note:** Because windows are 10 minutes wide with a 5-minute watermark, results will not appear in the console immediately on startup. Allow at least one full slide interval (30 seconds) for the first batch to emit.

---

# Validation Checklist

- [ ] Spark still consumes structured telemetry successfully
- [ ] `event_ts` is used as the event-time column for windowing
- [ ] Watermark of 5 minutes is applied before the window aggregation
- [ ] Sliding window of 10 min / 30 sec slide is configured
- [ ] Aggregations are grouped per `vehicle_id`
- [ ] `avg_delivery_buffer` appears in console output
- [ ] `max_cargo_temperature` appears in console output
- [ ] Console sink uses `update` output mode
- [ ] Spark UI remains healthy and stable

---

# Exit Criteria

Phase 3D is complete when:

- The streaming job computes rolling metrics over event-time windows
- Late events are tolerated by watermarking
- `avg_delivery_buffer` and `max_cargo_temperature` are visible per vehicle in console output
- Output mode is `update`
- No instability or parsing errors occur

---

# Phase 3D Outcome

At the conclusion of this phase, LogiShield will have a proper time-aware streaming analytics layer.

This prepares the pipeline for Phase 3E, where the rolling metrics can finally be mapped into deterministic Green, Yellow, and Red risk tiers.
