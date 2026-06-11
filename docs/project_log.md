# Project Log

---

## 2026-06-04 — Phase 1: Foundation & Infrastructure

### What Was Completed

- Created component directory skeleton: `simulator/`, `spark_streaming/`, `agent/`, `chaos/`, `benchmarks/`, `configs/`, `scripts/`
- Created `requirements.txt` with initial dependencies: `pyspark==3.5.1`, `kafka-python==2.0.2`, `pydantic==2.7.4`, `pytest==8.2.0`
- Created `docker-compose.yml` with all five required services
- Stood up full Docker stack — all services healthy
- Both Kafka topics created with correct partition configuration

### Service Port Map

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8081 |
| Kafka Broker (host access) | localhost:9093 |
| Kafka Broker (container) | kafka:9092 |
| Spark Master (internal) | spark://spark-master:7077 |
| Zookeeper | localhost:2181 |

### Topic Configuration Validated

| Topic | Partitions | Replication Factor | Status |
|-------|------------|-------------------|--------|
| `fleet-telemetry` | 3 | 1 | Created |
| `risk-alerts` | 3 | 1 | Created |

### Key Decisions Made

**`bitnami/spark` replaced with `apache/spark:3.5.1`**
Bitnami has removed their Spark images from Docker Hub. Switched to the official Apache image. Configuration difference: `bitnami/spark` used `SPARK_MODE` env vars; `apache/spark` uses explicit `spark-class` commands in the `command:` field.

**Kafka dual-listener config applied**
`PLAINTEXT://kafka:9092` for container-to-container (Spark). `PLAINTEXT_HOST://localhost:9093` for host-machine access (simulator, scripts). `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` enforced to prevent accidental topic creation with wrong partition counts.

**Init container for topic creation**
`kafka-init` runs `kafka-topics` commands after Kafka passes its healthcheck, then exits. Fully automated — `docker compose up -d` creates all infrastructure and topics in one step.

### Phase 1 Complete

All exit criteria satisfied. Phase 2 can begin.

---

## 2026-06-06 — Phase 1: Python Environment and Smoke Test

### What Was Completed

- Python virtual environment initialized at `venv/` using Python 3.12
- `pip install -r requirements.txt` completed cleanly
- `scripts/smoke_test.py` created and passed — Spark connected to Kafka and accessed `fleet-telemetry`

### Key Decisions Made

**Python 3.12 enforced for venv**
System default was Python 3.14, which caused `pydantic-core==2.18.4` to fail during wheel build. Root cause: `pydantic-core` uses PyO3 0.21.2, which only supports up to Python 3.12. Recreated venv with `python3.12 -m venv venv`. This matches the Python 3.12 target specified in the Phase 1 plan.

**Smoke test uses host-side Kafka listener**
`smoke_test.py` connects via `localhost:9093` because it runs on the host machine. When Spark runs inside Docker in Phase 2, it must connect via `kafka:9092` instead. This distinction must be preserved when writing the Phase 2 streaming job.

### Smoke Test Result

```
[SMOKE TEST] Row count: 0
[SMOKE TEST] PASSED — Spark connected to Kafka and accessed fleet-telemetry
```

Row count of 0 is expected — no simulator is running yet. Empty topic is a valid state at this stage.

---

## 2026-06-09 — Phase 2: Telemetry Generation

### What Was Completed

- `simulator/models.py` — Pydantic `TelemetryEvent` model with `create()` factory and `to_json_bytes()` serializer
- `simulator/simulator.py` — continuous telemetry simulator for 3 trucks publishing to `fleet-telemetry` at 1 event/sec per vehicle
- `simulator/__init__.py` — package marker enabling `python -m simulator.simulator` entry point
- `requirements.txt` updated: `kafka-python` replaced with `kafka-python-ng==2.2.3`
- Simulator validated: events visible in Kafka UI with correct schema and partition distribution

### Key Decisions Made

**`kafka-python` replaced with `kafka-python-ng==2.2.3`**
`kafka-python==2.0.2` fails to import on Python 3.12 due to a broken `kafka.vendor.six.moves` vendor path. `kafka-python-ng` is the actively maintained fork with an identical API and Python 3.12 support.

**Scenario state is step-driven, not state-machine-driven**
Each truck advances through GREEN (steps 0–4) → YELLOW (steps 5–9) → RED (steps 10–14) using `step % 15`. No transition logic needed — the step counter alone determines scenario. Simpler and fully deterministic.

**Each event is self-describing**
`sla_buffer_threshold` and `cargo_temp_threshold` are stamped onto every event. Spark can evaluate risk on a single row with no external join. This was an architecture decision from Phase 2 planning and is now confirmed in the schema.

**Simulator entry point: `python -m simulator.simulator`**
Run from project root with venv active. Connects to Kafka via `localhost:9093` (host-side listener). Kafka and Spark must be up via `docker-compose up -d` first.

### Validation Results

- 3 trucks (TRUCK_101, TRUCK_102, TRUCK_103) publishing simultaneously
- Events correctly cycling GREEN → YELLOW → RED per truck
- Messages visible in Kafka UI under `fleet-telemetry` topic
- Partitioning confirmed across all 3 partitions using `vehicle_id` as key

### Phase 2 Complete

All exit criteria satisfied. Phase 3 (Spark Structured Streaming) can begin.

---

## 2026-06-10 — Phase 3A: Spark Ingestion Foundation

### What Was Completed

- `spark_streaming/__init__.py` — package marker
- `spark_streaming/stream_processor.py` — Spark Structured Streaming job consuming from `fleet-telemetry` and printing raw events to console
- Phase 3A plan doc updated with Kafka connector JAR requirement, run instructions, and broker address decision

### Key Decisions Made

**Local mode for Phase 3**
Spark runs via `spark-submit --master local[*]` on the host machine rather than submitting to the Docker cluster. Simpler networking, easier debugging, sufficient for all Phase 3 sub-phases. Spark UI available at `localhost:4040`.

**Kafka connector JAR required at submit time**
`spark-sql-kafka-0-10_2.12:3.5.1` must be passed via `--packages`. Without it Spark cannot read Kafka streams. JAR is downloaded on first run and cached locally.

**`startingOffsets: earliest`**
Ensures the job immediately processes the existing message backlog on startup rather than waiting for new events. Useful for validation and replay.

**Run command**
```bash
spark-submit \
  --master 'local[*]' \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
  spark_streaming/stream_processor.py
```

### Validation Results

- All 3 trucks (TRUCK_101, TRUCK_102, TRUCK_103) visible in console output
- Events correctly partitioned: TRUCK_101/102 on partition 1, TRUCK_103 on partition 2
- GREEN, YELLOW, and RED scenario states all observed in live data
- Historical backlog consumed from offset 3 (Batch 0), live data from offset ~13,936 (Batch 1+)
- Multiple batches processed with no errors

### Phase 3A Complete

Kafka → Spark connectivity proven. Phase 3B can begin.

---

## 2026-06-10 — Phase 3B: Schema Parsing & Structured DataFrames

### What Was Completed

- `spark_streaming/stream_processor.py` updated to parse raw Kafka JSON payloads into typed Spark columns using `from_json()` and an explicit `StructType`
- `event_ts` cast to `TimestampType` — ready for event-time windowing in Phase 3D
- Raw `value` column dropped after parsing — redundant once structured columns exist
- Kafka metadata (`topic`, `partition`, `offset`, `timestamp`) preserved alongside telemetry fields

### Key Decisions Made

**`event_ts` cast to `TimestampType` in 3B, not deferred**
`event_ts` arrives as an ISO-8601 string. Casting it here avoids a refactor in Phase 3D where it is required as the event-time column for windowing and watermarking.

**Raw `value` column dropped**
Once `from_json()` extracts structured columns, the original JSON string is redundant and adds noise to every console row.

### Phase 3B Complete

Raw Kafka messages now produce a fully typed Spark DataFrame. Phase 3C can begin.

---

## 2026-06-10 — Phase 3C: Derived Metrics

### What Was Completed

- `spark_streaming/stream_processor.py` updated to compute `delivery_buffer` as a derived column via `withColumn()`

### Key Decisions Made

**`delivery_buffer = sla_time_remaining - time_left_to_destination`**
First business logic signal in the pipeline. Can be negative if SLA is already breached. Defined in `event_schema.md` — Spark implementation matches exactly.

**`temp_headroom` deferred to Phase 3D/3E**
Would require a one-line addition to `event_schema.md`. Deferred to keep 3C minimal and add it in the phase where it is actually needed for risk tiering logic.

### Phase 3C Complete

First operational signal derived from telemetry. Phase 3D (windowing and watermarking) can begin.

---

## 2026-06-10 — Phase 3E: Risk Tiering & Alert Generation

### What Was Completed

- `spark_streaming/stream_processor.py` finalized with full risk pipeline: `first(sla_buffer_threshold/cargo_temp_threshold)` added to windowed aggregation; deterministic GREEN/YELLOW/RED classification; GREEN filtered out; canonical alert records built with `reason` strings; alerts written to `risk-alerts` Kafka topic
- `ARCHITECTURE_DECISIONS.md` updated with decisions 7–10 (checkpoint location, duplicate alerts, deterministic event_id, formatting deviations) and Known Issue 1 (YELLOW eclipse)

### Validation Results

- Alerts confirmed in `risk-alerts` topic via Kafka console consumer
- All 7 canonical alert fields present: `event_id`, `event_ts`, `vehicle_id`, `risk_tier`, `delivery_buffer`, `cargo_temperature`, `reason`
- GREEN events correctly filtered — only YELLOW/RED reach `risk-alerts`
- `reason` field populated correctly (e.g. `"Cargo temperature exceeded threshold: 6.48C"`)
- No errors during execution

### Known Issues

- **YELLOW alerts eclipsed by RED** — 10-minute windows capture ~40 simulator cycles, so `max_cargo_temperature` always reflects RED-level values. YELLOW tier is reachable in theory but invisible in practice. Documented in `ARCHITECTURE_DECISIONS.md` as Known Issue 1. Not blocking Phase 4.
- **`event_ts` not ISO-8601** — Spark's `.cast("string")` produces `"2026-06-10 21:33:45"` format. Fix deferred to Phase 4.
- **`reason` float precision** — temperature values emitted at full double precision rather than 2dp. Fix deferred to Phase 4.

### Phase 3 Complete

Full pipeline proven end-to-end: Simulator → Kafka (fleet-telemetry) → Spark → Kafka (risk-alerts). Phase 4 (AI remediation agent) can begin.

---

## 2026-06-11 — Phase 4A: Out-of-Order Event Validation

### What Was Completed

- `chaos/__init__.py` — package marker
- `chaos/chaos_injector.py` — out-of-order chaos mode via timestamp backdating, with `--mode` and `--delay` CLI flags
- `docs/phases/phase_4/phase_4a.md` — plan with mechanism definition, disorder progression, observation method, and run commands
- All three disorder passes executed and validated

### Key Decisions Made

**Timestamp backdating as the out-of-order mechanism**
Events arrive in Kafka in normal order but with `event_ts` shifted backwards by a random offset within the configured delay range. This directly exercises Spark's event-time logic — Spark assigns events to windows based solely on `event_ts`, not arrival order.

**Checkpoint must be cleared only after Spark is stopped**
Clearing `/tmp/logishield-checkpoints/risk-alerts` while Spark is running causes `HDFSStateStore` failures as Spark tries to commit to deleted state files. Correct procedure: kill Spark first, then clear checkpoint, then restart.

### Validation Results

| Pass | Delay Range | Result |
|------|-------------|--------|
| Slight | ±1 min | All events accepted, alerts flowing normally |
| Moderate | ±3 min | All events accepted, within 5-min watermark boundary |
| Heavy | ±7 min | Spark correctly dropped events exceeding watermark, stream remained stable |

- Spark UI at `localhost:4040` confirmed watermark advancing in real time
- Heavy pass confirmed watermark drop behavior — events with `event_ts` older than `current_time - 5min` were silently excluded from windows
- No crashes or processing errors across any pass
- Spark batch duration remained stable throughout

### Phase 4A Complete

Event-time windowing and watermarking validated under disorder. Spark correctly uses `event_ts` as the source of truth regardless of arrival order. Phase 4B (delayed burst replay) can begin.

---

## 2026-06-11 — Simulator & Chaos OOP Refactor

### What Was Completed

- `simulator/simulator.py` refactored to introduce three classes: `Scenario`, `Vehicle`, `Fleet`
- `chaos/chaos_injector.py` refactored to introduce `ChaosInjector` class
- Standalone functions `next_scenario()`, `generate_values()`, and `FLEET` dict list removed from `simulator.py`

### Class Responsibilities

| Class | Owns |
|---|---|
| `Scenario` | State resolution (`from_step()`), value generation (`generate_values()`) |
| `Vehicle` | Identity, thresholds, step counter, event construction (`generate_event()`, `advance()`) |
| `Fleet` | Collection of vehicles (`default()` classmethod, `__iter__`) |
| `ChaosInjector` | Event timestamp transformation only (`apply_out_of_order()`, `apply_delayed_burst()`) |

### Key Decisions Made

**OOP applied to simulator and chaos components**
`Scenario`, `Vehicle`, and `Fleet` encapsulate identity, state, and behavior. `ChaosInjector` owns event transformation only — it holds no fleet, producer, topic, or CLI state. This separation keeps each class testable in isolation and makes the chaos modes composable.

**Functional style retained for Spark (`spark_streaming/stream_processor.py`)**
Spark Structured Streaming pipelines are transformation chains. Wrapping them in a class would add indirection with no benefit — the data flow is already expressed clearly as a sequence of DataFrame operations. OOP was deliberately not applied here.

**`create_producer()` kept as a module-level function**
It is a stateless factory. A class wrapper would be noise.

**Run loops kept as module-level functions**
The `run()` function in `simulator.py` and the `run_out_of_order()` / `run_delayed_burst()` functions in `chaos_injector.py` own the producer, fleet, and loop lifecycle. These are coordination concerns, not behavior that belongs on any class.

**`Vehicle.generate_event(event_ts=None)` as the unification point**
Both normal simulation and chaos modes use the same `generate_event()` method. Normal simulator passes `None` (uses `now()`); chaos injector constructs a backdated `datetime` and passes it in. No special-casing needed in either path.

### Fix Applied During Review

Cursor deviated from the original `generate_values()` ranges in ways that would have broken the risk tiering guarantees:
- RED: changed to `sla_time_remaining = sla_threshold - randint(5,20)` — delivery_buffer was no longer guaranteed negative
- YELLOW: switched from percentage-based temperature (`threshold * 0.9`) to absolute subtraction

Corrected to preserve the original relationships: RED always produces negative `delivery_buffer`; YELLOW temperature stays in the `(threshold * 0.9, threshold * 0.95)` band that Spark's YELLOW condition expects.
