# Phase 4A — Out-of-Order Event Validation

## Objective

Validate that LogiShield's event-time windowing and watermarking continue to behave correctly when telemetry arrives out of order.

This phase is about correctness, not scale. The goal is to verify that Spark relies on `event_ts` rather than message arrival order and that the windowed metrics remain stable under disorder.

---

# Scope

This phase focuses exclusively on:

- Out-of-order telemetry generation
- Spark event-time validation
- Watermark behavior under disorder
- Window correctness under delayed arrival
- Observation and documentation of Spark output

This phase does not include:

- Fleet scaling
- Packet loss
- Delayed burst replay
- New business logic
- New risk classification logic
- AI remediation

---

# Architecture Being Tested

```
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
```

The key assumption under test is that Spark's stream logic uses `event_ts` as the source of truth, not the order in which Kafka or Spark receives messages.

---

# Chaos Injector Location

All Phase 4 chaos modes live in:

```
chaos/
```

For this sub-phase, implement the out-of-order mode in:

```
chaos/chaos_injector.py
chaos/__init__.py
```

**Responsibilities:**

- Import and reuse `TelemetryEvent` from `simulator.models`
- Reuse the Kafka producer setup from `simulator.simulator`
- Emit telemetry events with backdated `event_ts` values
- Keep `simulator/simulator.py` unchanged

The chaos injector is a separate entry point. It does not modify the normal simulator.

---

# Out-of-Order Mechanism: Timestamp Backdating

The out-of-order effect is achieved by **backdating `event_ts`** — events are sent to Kafka in normal arrival order, but their `event_ts` values are set to shuffled past timestamps.

This is the correct mechanism for testing Spark's event-time processing. Spark does not care about Kafka message arrival order — it assigns events to windows exclusively based on `event_ts`. Backdating timestamps directly exercises the watermark and windowing logic without introducing Kafka-level complexity.

**How it works:**

Each event is generated normally, then its `event_ts` is shifted backwards by a random offset within the configured delay range before being published to Kafka.

---

# Disorder Progression

Run three passes in sequence. The 5-minute watermark is the critical boundary.

| Pass | Delay Range | Expected Behavior |
|------|-------------|-------------------|
| Slight | ±1 minute | All events accepted. Windows unaffected. |
| Moderate | ±3 minutes | All events accepted. Some arrive late but within watermark. |
| Heavy | ±7 minutes | Events outside 5-min watermark are silently dropped by Spark. Windows may be incomplete. |

The **heavy pass is the most important test** — it directly validates that watermarking drops late events as designed rather than silently corrupting window aggregations.

---

# Deliverables

## `chaos/chaos_injector.py`

Implement a `run_out_of_order(delay_seconds: int)` function that:

1. Generates telemetry events for the standard 3-truck fleet (TRUCK_101, TRUCK_102, TRUCK_103)
2. Backdates each event's `event_ts` by a random offset between `0` and `delay_seconds` seconds
3. Publishes the backdated events to `fleet-telemetry` via `localhost:9093`
4. Prints each event's `vehicle_id`, `scenario_state`, and the applied delay offset
5. Runs continuously until interrupted

Use `delay_seconds` as the single control parameter:
- Slight disorder: `delay_seconds=60`
- Moderate disorder: `delay_seconds=180`
- Heavy disorder: `delay_seconds=420`

---

# Observation Method

The console sink was removed in Phase 3E. Observation happens via two sources:

**Kafka UI** (`http://localhost:8080` → `risk-alerts` topic):
- Confirms alerts are still being generated under disorder
- Inspect `event_ts` values in alert payloads — should reflect window end times, not backdated event times

**Spark UI** (`http://localhost:4040` → Streaming tab):
- Watch input rate, processing rate, and batch duration
- Look for any stability changes between passes

There is no console output to watch. The pipeline writes only to Kafka.

---

# Expected vs. Unexpected Behavior

| Scenario | Expected | Unexpected (indicates a bug) |
|----------|----------|------------------------------|
| Slight/moderate disorder | Alerts continue flowing normally | No alerts, errors, or crashes |
| Heavy disorder | Some events silently dropped; window output may thin out | Spark crashes, corrupted metrics, or events incorrectly included past watermark |
| All passes | Spark UI remains healthy, no processing lag | Batch duration grows unboundedly |

---

# How to Run

**Step 1 — Start Docker stack:**
```bash
docker-compose up -d
```

**Step 2 — Start the Spark job:**
```bash
source venv/bin/activate
spark-submit \
  --master 'local[*]' \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
  spark_streaming/stream_processor.py
```

**Step 3 — Run the chaos injector (in a separate terminal):**
```bash
source venv/bin/activate

# Slight disorder (±1 min)
python -m chaos.chaos_injector --mode out_of_order --delay 60

# Moderate disorder (±3 min)
python -m chaos.chaos_injector --mode out_of_order --delay 180

# Heavy disorder (±7 min — exceeds watermark)
python -m chaos.chaos_injector --mode out_of_order --delay 420
```

**Note:** Clear the Spark checkpoint between passes to reset window state:
```bash
rm -rf /tmp/logishield-checkpoints/risk-alerts
```

---

# Validation Checklist

- [ ] `chaos/__init__.py` and `chaos/chaos_injector.py` exist
- [ ] Slight disorder (±1 min): Kafka receives backdated events, alerts still flow
- [ ] Moderate disorder (±3 min): alerts still flow, no Spark errors
- [ ] Heavy disorder (±7 min): Spark drops late events without crashing, alert rate may decrease
- [ ] Spark UI remains healthy across all passes
- [ ] Behavior differences between passes are documented

---

# Exit Criteria

Phase 4A is complete when:

- Out-of-order telemetry has been injected at all three disorder levels
- Spark continues processing without failure
- Heavy disorder confirms watermark drop behavior (events older than 5 min are excluded)
- Observations across passes are documented in the project log

---

# Phase 4A Outcome

At the end of Phase 4A, LogiShield will have validated one of its most important streaming assumptions: that the system can tolerate disorder in message arrival as long as event time remains authoritative.

This creates the baseline for Phase 4B, where the pipeline will be tested against delayed burst replay and watermark boundary behavior.
