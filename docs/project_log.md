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

---

## 2026-06-12 — Phase 4B: Delayed Burst Replay

### What Was Completed

- `chaos/chaos_injector.py` updated with `delayed_burst` mode — 30 events sent instantly with `event_ts` backdated by a fixed offset
- `--vehicle` flag added so any truck can be targeted
- All four delay scenarios executed and validated

### Validation Results

| Scenario | `--delay` | Rows Dropped by Watermark | Result |
|----------|-----------|--------------------------|--------|
| Small | 120s | 0 | All events accepted, alerts appeared in `risk-alerts` |
| Near-boundary | 240s | 0 | All events accepted, within 5-min watermark |
| Over-boundary | 420s | ~4 | Some events dropped, stream remained stable |
| Extreme | 1200s | ~20 of 30 | Most events dropped, stream remained healthy |

### Key Observations

**Burst mechanism is timestamp backdating, not actual sleep**
Events are sent to Kafka instantly. The "delay" is applied solely to `event_ts`. Spark uses `event_ts` for watermark decisions — this is the correct way to simulate a connectivity outage without waiting real time.

**Watermark warm-up: the most important insight from this phase**
In the 1200s scenario, 10 of 30 burst events were accepted despite being 20 minutes old. This is expected Spark behavior:
- Spark updates the watermark at the **end** of each micro-batch, and applies it to the **next** batch
- On a fresh start (after checkpoint clear), the watermark begins at epoch (effectively 0)
- The first micro-batches process events before the watermark has advanced enough to reject stale events
- Once TRUCK_102/103's live events push the watermark past `now - 5 min`, the remaining burst events are correctly dropped

Analogy: a nightclub bouncer who only lets people in from the last 5 minutes, but has no reference point when the club first opens — he lets the first arrivals in regardless of their ID timestamp, then enforces the rule once he knows what "now" is.

**Implication for production systems:** watermark-based filtering cannot be relied upon immediately after a stream restart. There is a warm-up window during which late events may slip through. This is a known Spark design characteristic, not a bug.

### No Crashes or Stream Failures

Spark remained stable across all four scenarios. Input rate spiked briefly during each burst then returned to the normal simulator's baseline. Watermark continued advancing throughout, driven by TRUCK_102 and TRUCK_103.

### Phase 4B Complete

Watermark boundary behavior validated under delayed burst conditions. Spark correctly separates acceptable lateness from stale events once the watermark is established. Phase 4C can begin.

---

## 2026-06-12 — Phase 4C: Packet Loss Simulation

### What Was Completed

- `chaos/chaos_injector.py` updated with `packet_loss` mode
- `--loss-rate` float flag added to CLI (e.g. `--loss-rate 0.15` for 15%)
- `--delay` made optional and guarded per mode with `parser.error()` — consistent with `--loss-rate`
- Packet drops confirmed via terminal output across all three loss rates

### Validation Results

| Scenario | `--loss-rate` | Result |
|----------|--------------|--------|
| Light | 0.03 | DROPPED lines visible, stream looks nearly normal |
| Moderate | 0.05 | Missing samples begin to appear |
| Heavy | 0.15 | Meaningful drop rate confirmed in summary output |

### Key Design Decisions

**Packet loss is not tested against Spark in this phase**
The goal is to confirm that events are being dropped before reaching Kafka, not to observe Spark's reaction. Spark handles variable input rates gracefully — it processes whatever arrives in each micro-batch and does not crash from reduced throughput.

**The real risk of packet loss is silent data quality degradation, not instability**
Spark keeps running and alerts keep flowing, but risk classifications may be based on incomplete data. If dropped events happen to be the RED-state events, the pipeline produces a false YELLOW or no alert — with no error or indication that data was missing. This is a harder failure mode to detect than a crash.

**Watermark stalling is only a risk at extreme loss rates (near 100%)**
At 10-15% loss, enough events arrive to keep the watermark advancing. At near-total loss, the watermark would freeze, windows would never close, and state would accumulate until OOM. Not a concern at the rates tested.

**Drop logic is inline, not on the ChaosInjector class**
One `random.random() < loss_rate` check before `producer.send()`. No class method warranted for a single conditional.

**vehicle.advance() always runs regardless of drop**
The vehicle's internal state progresses even when an event is dropped. Packet loss doesn't freeze the truck — it just means that step's telemetry never reached Kafka.

### Phase 4C Complete

Packet loss mechanism confirmed working. Events are randomly dropped before Kafka publication at the configured rate, with a summary printed on exit. Phase 4D (fleet scaling and throughput benchmarking) can begin.

---

## 2026-06-13 — Phase 4D: Fleet Scaling

### What Was Completed

- `simulator/simulator.py` updated with `Fleet.scaled(n)` classmethod and `--fleet-size` CLI flag
- `run()` updated with time-compensated sleep: `sleep(max(0.0, 1.0 - elapsed))`
- `main()` added with argparse entry point
- `chaos/chaos_injector.py` updated to use `Fleet.scaled(3)` in all three run functions for consistency
- Default `--vehicle` for `delayed_burst` updated from `TRUCK_101` to `TRUCK_0001`
- 20-truck fleet validated locally

### Key Decisions Made

**`Fleet.scaled(n)` added alongside `Fleet.default()`**
`Fleet.default()` was not removed — it would have silently broken the chaos injector. `Fleet.scaled(n)` generates N vehicles with auto-incremented IDs (`TRUCK_0001`...`TRUCK_N`) and randomised thresholds within the same operational ranges as the original fleet.

**Vehicle ID format change: TRUCK_101 → TRUCK_0001**
`Fleet.scaled()` uses a 4-digit zero-padded format. The chaos injector was updated to match so all components use the same vehicle ID scheme. The old 3-digit IDs (TRUCK_101/102/103) no longer appear anywhere.

**Time-compensated sleep**
`sleep(1)` replaced with `sleep(max(0.0, 1.0 - elapsed))`. At small fleet sizes the difference is negligible. At large fleet sizes (500+) the loop itself takes meaningful time and the fixed sleep would cause each vehicle's rate to drift below 1 event/sec. The compensated version keeps the per-vehicle rate accurate regardless of fleet size.

**`run()` now always calls `Fleet.scaled()`**
The default of `--fleet-size 3` produces the same 3-vehicle behaviour as before, just with TRUCK_0001/0002/0003 instead of TRUCK_101/102/103.

### Scale Test Results

| Fleet Size | Input Rate (events/sec) | Processing Rate (events/sec) | Batch Pattern |
|------------|------------------------|------------------------------|---------------|
| 100 | ~90 | ~600 | 1-second bursts |
| 500 | ~400 | ~900 | 1-second bursts |
| 1000 | ~900 | ~1500 | 1-second bursts |
| 10000 | ~2000 (ceiling) | ~3000 | Continuous stream |

### Bottleneck Finding

**The Python simulator saturates at ~2,000 events/sec. Spark never became the bottleneck.**

At 10,000 trucks the simulator loop takes longer than 1 second, so `max(0.0, 1.0 - elapsed)` hits 0 and the simulator runs flat out. Python's single-threaded GIL and sequential loop caps throughput at ~2,000 events/sec — not Kafka, not Spark.

Spark's processing rate (3,000 events/sec) stayed above input rate (2,000 events/sec) even at maximum simulator throughput. The pipeline has more headroom than the simulator can exercise.

**Load pattern shifts at scale**
Up to ~1,000 trucks, events arrive in discrete 1-second bursts — all N events land at once, then Spark has idle time before the next cycle. At 10,000 trucks the loop never sleeps, producing a continuous stream with no idle gaps. These are fundamentally different load profiles. The burst pattern is easier for Spark because it gets breathing room between batches.

**Kafka showed no stress at any fleet size tested.**
The Python simulator saturated before Kafka had any chance to become constrained.

### Known Limitation

To find Spark's true throughput ceiling, a faster event source is needed — a multi-threaded or multi-process Python producer, or a JVM-based producer. Improving the simulator to push past 2,000 events/sec is deferred to a future phase.

### Phase 4D Complete

Fleet scaling validated up to the simulator's throughput ceiling. The pipeline architecture (Kafka + Spark Structured Streaming) has demonstrated headroom beyond what the current Python simulator can exercise. Phase 5 (load realism and benchmarking) can begin.

---

## 2026-06-14 — Phase 5A: Threaded Load Model Refactor

### What Was Completed

- `simulator/simulator.py` refactored to use 4 worker threads with round-robin fleet sharding
- Single shared `KafkaProducer` across all threads (thread-safe, better batching)
- `threading.Event` stop flag for clean shutdown on Ctrl+C — `stop.wait(timeout)` replaces `time.sleep()` so threads wake immediately on shutdown
- Diagnostic event-rate counter added (`_record_event()`, `_rate_reporter()`) to measure Python's generation rate independently of Kafka throughput
- Fleet sharding via `vehicles[i::num_threads]` — round-robin, perfectly even distribution, empty-shard guard for small fleets

### Benchmark Results

#### Python Generation Rate (standalone, no Spark running)

| Fleet Size | Python ev/s | Sleep behaviour |
|------------|-------------|-----------------|
| 3,000 | 3,000 | Sleep filling gap — 1 event/truck/sec |
| 10,000 | 10,000 | Sleep ≈ 0 — loop takes exactly 1 second |
| 20,000 | ~10,500 | Sleep = 0 — Python running flat out |
| 40,000 | ~10,400 | Sleep = 0 — ceiling confirmed |

**Python ceiling: ~10,500 ev/s with 4 threads. 5x improvement over the sequential simulator (2,000 ev/s).**

#### Full Pipeline (Python + Kafka + Spark running simultaneously)

| Metric | Value |
|--------|-------|
| Python diagnostic ev/s | ~3,000 |
| Spark input rate | ~3,000 |
| Gap between Python and Spark | None |

### Bottleneck Finding

**The local Docker Kafka broker is the bottleneck under full pipeline load, not Python and not Spark.**

When Spark is running, the Kafka broker handles two simultaneous workloads:
- Produce path: Python → Kafka (~10,000 msg/sec attempted)
- Consume path: Kafka → Spark (reading and tracking offsets)

A single-node Docker broker on a MacBook Air cannot sustain both at full speed. It caps at ~3,000 msg/sec total throughput. When the broker slows down, the producer's internal buffer fills and `producer.send()` blocks — which is why the Python diagnostic also drops to ~3,000. Python is not slow; it is waiting on Kafka.

The key evidence: without Spark running, Python generates 10,000 ev/s. With Spark running, both Python diagnostic and Spark input rate drop to the same ~3,000 ev/s ceiling simultaneously. No gap between producer and consumer — the shared constraint is the broker.

**In a real production environment** (multi-broker Kafka cluster, more partitions, or a managed service), this ceiling would be orders of magnitude higher. The architecture is sound; the local single-node broker is the hardware constraint.

### Phase 5A Complete

Python is no longer the simulator bottleneck. The threading refactor pushed the standalone ceiling from 2,000 to ~10,500 ev/s. Under full pipeline load, the constraint is the local Kafka broker at ~3,000 msg/sec. Phase 5B (Kafka bottleneck mitigation) can begin.

---

## 2026-06-14 — Phase 5B: Kafka Bottleneck Mitigation

### Step 1 — Producer Tuning

**Settings applied:**

| Parameter | Old (default) | New |
|-----------|--------------|-----|
| `linger_ms` | 0 | 10 |
| `batch_size` | 16,384 | 65,536 |
| `buffer_memory` | 33,554,432 | 67,108,864 |
| `compression_type` | None | lz4 |

`lz4` used instead of `snappy` — snappy native library failed to download due to network timeout. lz4 is marginally faster than snappy at equivalent compression ratios and installed cleanly via pip.

**Results after producer tuning:**

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Python diagnostic ev/s (full pipeline) | ~3,000 | ~7,700 | +157% |
| Spark input rate | ~2,500 | ~3,300 | +32% |

The Python diagnostic improvement (+157%) confirms that the previous ceiling was largely producer back-pressure — `linger_ms=0` was causing 10,000 individual produce requests per second to the broker. Batching with `linger_ms=10` reduced that dramatically.

Spark input rate improvement is smaller (+32%) because the broker is still handling dual produce/consume load on a single node. Partition count is the next variable to address.

### Step 2 — Partition Increase

Both `fleet-telemetry` and `risk-alerts` increased from 3 partitions to 12.

Rationale: 3 partitions was designed for the original 3-truck fleet. At 10,000+ trucks, funnelling all produce and consume traffic through 3 partitions creates per-partition contention at the broker. 12 partitions is divisible by 4 (producer threads) and provides more parallel tasks for Spark's `local[*]` executor.

`docker-compose.yml` updated to recreate topics at 12 partitions on fresh stack startup.

**Results after partition increase:**

| Metric | After producer tuning | After partition increase |
|--------|----------------------|--------------------------|
| Python diagnostic ev/s | ~7,700 | ~11,000 |
| Spark input rate | ~3,300 | ~2,700 |

Spark input rate did not improve meaningfully from the partition increase. The bottleneck has moved.

### Final Bottleneck Finding

**Spark is now the bottleneck — not Kafka.**

With producer tuning in place, Python generates 11,000 ev/s and Kafka accepts all of it (no back-pressure on the producer). Kafka then stores the messages and waits. Spark pulls from Kafka at 2,700 ev/s — not because Kafka is slow at delivering, but because Spark is a pull-based consumer that requests the next batch only after finishing the current one.

Spark's 2,700 ev/s ceiling is determined by how long each micro-batch takes: JSON parsing, sliding window aggregations, stateful watermarking, and risk tiering for 10,000 trucks on a shared MacBook Air. Kafka is doing exactly what it should — buffering the gap between a fast producer and a slower consumer.

**Bottleneck progression across phases:**

| Phase | Bottleneck | Ceiling |
|-------|-----------|---------|
| Phase 4D | Python sequential loop | ~2,000 ev/s |
| Phase 5A | Kafka broker (dual load, no batching) | ~3,000 ev/s |
| Phase 5B | Spark processing (windowing, state, parsing) | ~2,700 ev/s consumed |

### Phase 5B Complete

Kafka is no longer the ceiling. Python generates 11,000 ev/s, Kafka buffers successfully, and Spark consumes at its own processing pace (~2,700 ev/s). The pipeline is now correctly structured with Kafka acting as the decoupling buffer between a fast producer and a throughput-bound stream processor.

---

## 2026-06-14 — Phase 5C: Per-Vehicle Cadence and Scheduling

### What Was Completed

- `Vehicle` class extended with `interval_seconds: float = 1.0` field
- `Vehicle.__lt__()` added to support heap comparison tiebreaking when two vehicles share the same next-emit timestamp
- `Fleet.scaled()` assigns cadence profiles via a `_cadence()` helper on construction
- `worker()` replaced the uniform shard loop with a per-shard min-heap (`heapq`) scheduler
- ±30% proportional jitter added to each emission interval via `random.uniform(0.7, 1.3)`
- `import heapq` added

### Cadence Profiles

| Profile | Base Interval | Jitter Range | Fleet Share |
|---------|--------------|--------------|-------------|
| Fast | 0.5s | 0.35–0.65s | 20% |
| Normal | 1.0s | 0.7–1.3s | 60% |
| Slow | 2.0s | 1.4–2.6s | 20% |

### Key Design Decisions

**Per-shard heap, not a single global scheduler**
A single scheduler thread at 10,000+ trucks would reintroduce the Phase 4D sequential bottleneck. Each of the 4 worker threads maintains its own `heapq` over its vehicle shard — no cross-thread coordination needed, throughput from Phase 5A is preserved.

**Staggered heap initialisation**
Initial emit times are offset by `interval_seconds * i / len(shard)` so all vehicles in a shard don't fire simultaneously at startup. Without this, the first second produces a burst equal to the full shard size.

**Proportional jitter over fixed ranges**
`±` jitter as a multiplier (`random.uniform(0.7, 1.3)`) preserves the semantic meaning of fast/normal/slow profiles. A flat random range (e.g. 0–5s for all slow trucks) would collapse the profiles into noise and make cadence diversity unobservable.

**`__lt__` on Vehicle for heap safety**
`heapq` compares tuple elements in order. If two vehicles share the same float timestamp (unlikely but possible), Python falls through to comparing the `Vehicle` objects. Without `__lt__`, this raises `TypeError`. Resolved by comparing `vehicle_id` strings as a stable tiebreaker.

### Phase 5C Complete

The simulator now produces a non-uniform, jittered traffic pattern with distinct fast, normal, and slow vehicles. The output is visibly heterogeneous and more representative of a real fleet than a perfectly regular loop. Phase 5D (benchmark harness and Spark limit testing) can begin.

---

## 2026-06-15 — Spark Shuffle Partitions Fix and Simulator Rate Characterisation

### Spark Configuration Fix

Added `spark.sql.shuffle.partitions=8` to `stream_processor.py`.

Spark's default is 200 shuffle partitions. In `local[*]` mode on an 8-core MacBook Air, this creates 200 parallel tasks during every shuffle operation when only 8 cores are available — 192 tasks are immediately queued and idle. This was a significant source of overhead on every micro-batch.

Setting it to 8 (matching available cores) removed this overhead. After the fix, Spark input rate matched the Python simulator output rate, confirming that the previous ~2,700 ev/s ceiling was a **Spark configuration problem, not a hardware bottleneck**.

**Conclusion: the laptop is not the bottleneck. Spark was misconfigured.**

Migration to remote VMs is not necessary at this stage.

### Simulator Rate Characterisation

With the Phase 5C per-vehicle cadence scheduler, the simulator no longer emits at `fleet_size × 1 ev/s`. The min-heap scheduler fires each vehicle only when it is due, so the actual output rate is determined by the cadence profile distribution:

| Profile | Interval | Fleet share | Contribution |
|---------|----------|-------------|--------------|
| Fast | 0.5s | 20% | 0.40 ev/truck/s |
| Normal | 1.0s | 60% | 0.60 ev/truck/s |
| Slow | 2.0s | 20% | 0.10 ev/truck/s |
| **Average** | | | **~1.1 ev/truck/s theoretical** |

In practice, the observed rate for a 10,000-truck fleet is consistently **~7,000 ev/s** rather than the theoretical ~11,000 ev/s. The gap is attributable to heap operation overhead (heappush/heappop per emission), `stop.wait()` scheduling granularity, and jitter reducing fast truck throughput.

The simulator ceiling remains **~11,000 ev/s regardless of fleet size** — adding more trucks beyond the point where the heap saturates the 4 worker threads does not increase throughput further. This ceiling is now Spark's effective input ceiling under the current configuration.

---

## 2026-06-16 — Phase 5D: State Transition Alerting & Temperature Signal Simplification

### What Was Completed

- `spark_streaming/stream_processor.py` updated to use `avg(cargo_temperature)` in windowed aggregation — `max` removed from imports and pipeline
- `foreachBatch` output mode added with `write_on_transition()` function and driver-side `last_tiers: dict[str, str]` state
- Alert records now written to `risk-alerts` via `df.write.format("kafka")` inside `foreachBatch` instead of a direct streaming sink
- Alerts suppressed when a vehicle's risk tier is unchanged from the previous batch

### Key Design Decisions

**`avg` over `max` for cargo temperature**
`max_cargo_temperature` in a 10-minute window always captured the worst RED-level reading in the window, even when most of the window was GREEN or YELLOW. This made YELLOW alerts structurally invisible — the single highest temperature always dominated. Switching to `avg` weights the classification by the proportion of time spent near the threshold, which restores YELLOW visibility and makes the tier reflect sustained thermal drift rather than a single spike.

**Driver-side dict for state tracking (`last_tiers`)**
The state is a simple `{vehicle_id: tier}` dict that lives in the Spark driver process. This is the simplest correct implementation: no RDD operations, no distributed state stores, no Kafka offsets to manage. It is reset on Spark restart (acceptable) and sufficient for this project's observability goals.

**`foreachBatch` with `df.write.format("kafka")` for output**
The streaming sink writes every update to Kafka in `update` mode, which cannot be filtered before writing. `foreachBatch` gives access to each micro-batch as a regular DataFrame, allowing the transition check to happen before any write. The write itself uses Kafka's batch mode (`df.write`) rather than a separate KafkaProducer, keeping the implementation within the Spark API boundary.

### Validation Results

- `risk-alerts` topic confirmed active with `--fleet-size 5`
- YELLOW alerts now appear alongside RED — `avg` temperature restored YELLOW visibility as expected
- Repeated identical-tier alerts suppressed — duplicate emissions no longer flood the topic
- Alert stream shows **YELLOW ↔ RED oscillation** as the dominant pattern, not identical repetition

### Key Observation: Window Boundary Oscillation

The alert stream oscillates between YELLOW and RED rather than emitting a single transition. This is expected behavior, not a bug.

**Why it happens:** The 10-minute sliding window fires every 30 seconds. Each firing computes `avg_cargo_temperature` over a different mix of events — as the window slides, the blend of GREEN/YELLOW/RED data shifts. When a truck is transitioning between phases, the average hovers near the threshold boundary. One 30-second window produces an average just above RED threshold; the next produces one just below — causing the tier to genuinely alternate.

**Why this is correct:** Each oscillation represents a real tier change, not a duplicate. The `last_tiers` filter is working as designed — it suppresses consecutive identical tiers (RED→RED, YELLOW→YELLOW) and allows genuine transitions through. An oscillating truck is meaningfully different from one firmly in RED: it is at the classification boundary, which is the most operationally interesting region.

**Comparison to before Phase 5D:**

| Before Phase 5D | After Phase 5D |
|---|---|
| RED, RED, RED, RED (same tier repeated per window) | RED, YELLOW, RED, YELLOW (genuine transitions only) |
| No transition filter | Transition filter active |
| YELLOW structurally invisible | YELLOW visible when avg is near threshold |

### Phase 5D Complete

Exit criteria satisfied:
- `avg_cargo_temperature` is the sole temperature signal in risk classification
- `max_cargo_temperature` removed entirely
- Alerts fire only on tier transitions — duplicate identical-state alerts suppressed
- YELLOW tier restored as a visible, meaningful risk state
- `risk-alerts` topic reflects genuine state changes rather than window-by-window repetition

---

## 2026-06-24 — Phase 6A: Benchmark Harness

### What Was Completed

- `benchmarks/__init__.py` — package marker enabling `python -m benchmarks.bench` entry point
- `benchmarks/bench.py` — benchmark runner that observes a live pipeline by consuming the `risk-alerts` Kafka topic, measuring processing lag (`alert_ts - window_end`) and alert throughput, querying the Spark REST API for job durations, and writing results to a structured JSON file
- `requirements.txt` updated: `requests==2.32.3` added for Spark REST calls
- `.gitignore` updated: `benchmarks/results/` excluded so raw result files are not committed
- Three pipeline bugs discovered and fixed during harness testing

### Pipeline Bugs Fixed During Phase 6A

**Bug 1: timezone mismatch (lag always null)**

`window_end` was emitted in local time (UTC+8, e.g. `"2026-06-24 18:06:00"`) while `alert_ts` was in UTC (`"2026-06-24T10:06:00+00:00"`). The computed lag was always −8 hours, always filtered as negative, lag_samples always 0.

Root cause: Spark uses the JVM system timezone when casting `window.start`/`window.end` to string unless the session timezone is explicitly set.

Fix: `.config("spark.sql.session.timeZone", "UTC")` added to SparkSession in `stream_processor.py`. `window_end` now emits in UTC and the two timestamps are comparable.

**Bug 2: backlog replay contaminating lag measurement**

With `startingOffsets: earliest`, the benchmark consumer replayed every alert ever written to `risk-alerts`. Alerts from days earlier had lag values equal to their age (e.g. 7.6 days), making the lag metric meaningless.

Fix: changed to `startingOffsets: latest` so the pipeline only processes new events from the moment Spark starts. Past backlog is ignored.

**Bug 3: negative lag (wrong output mode)**

With `outputMode("update")`, Spark emits alerts for windows that are still open. At emission time, `alert_ts` (wall clock when Spark writes) is earlier than `window_end` (the future time when the window will close). This produces systematically negative lag — structurally impossible to measure correctly.

Fix: changed to `outputMode("append")`. In append mode, Spark holds a window result until the watermark has advanced past `window_end`, guaranteeing the window is fully closed before emitting. `alert_ts` is always after `window_end`, producing positive lag values representing actual processing latency.

**Lag formula:** `lag = alert_ts − window_end`

With `append` mode, 5-minute window, 5-minute watermark, and 5-second trigger, expected lag was 60–90 seconds. Observed lag was ~35–38 seconds with the initial configuration, confirming the formula was working.

### Spark REST API Finding

The harness initially attempted to query Spark's streaming metrics via REST (`/streaming/statistics`, `/streaming/batches`). Both returned 404. These are legacy DStream endpoints. Spark Structured Streaming does not expose streaming-specific metrics via the REST API in Spark 3.5.1 local mode. The `spark_jobs` section in the initial harness used job-level duration data instead, which was a proxy rather than a true streaming metric. This limitation was noted and addressed in Phase 6B.

### Key Decisions Made

**Observer-only harness**
The benchmark runner is entirely passive. It does not start or stop the simulator, Spark, or Kafka. It connects to an already-running pipeline, observes for a fixed duration, and writes results. This keeps the harness simple, composable, and free of process management complexity.

**Kafka consumer for alert measurement**
Rather than polling an internal Spark metrics endpoint, the harness consumes `risk-alerts` directly. This measures what the pipeline actually delivers to downstream consumers, not an internal Spark accounting figure.

**JSON output with structured schema**
Results are written as a single JSON object per run. This allows programmatic comparison across fleet sizes without manual transcription and makes the output diff-able in git if the gitignore is relaxed in the future.

### Phase 6A Complete

Benchmark harness functional. Pipeline timezone, offset, and output mode bugs resolved. Lag measurement producing correct positive values (~35s). Results written to structured JSON.

---

## 2026-06-24 — Phase 6B: Throughput Benchmarking

### What Was Completed

**Harness upgrade**

- `stream_processor.py`: watermark reduced `"5 minutes"` → `"1 minute"`, window reduced `"10 minutes"` → `"5 minutes"`, `_ProgressWriter(StreamingQueryListener)` added to write per-batch streaming metrics to `/tmp/logishield-spark-progress.jsonl`
- `bench.py`: dual-topic consumer (`fleet-telemetry` + `risk-alerts`), JSONL progress file tailing replacing Spark REST calls, `telemetry_ingress` section added, debug prints and skip counters removed, JSONL historical bleed fix applied

**Benchmark runs at four fleet sizes**

Each run: 300 seconds observation, Spark running continuously across fleet-size changes, simulator restarted between sizes.

### Benchmark Results

| Metric | 100 vehicles | 1,000 vehicles | 5,000 vehicles | 10,000 vehicles |
|---|---|---|---|---|
| ev/s (ingress) | 105.50 | 1,114.17 | 3,921.94 | 6,544.05 |
| spark input rps | 104.71 | 1,115.40 | 3,930.10 | 6,672.51 |
| spark process rps | 514.93 | 6,968.78 | 12,780.81 | 20,053.51 |
| trigger avg ms | 1,147 | 927 | 1,969 | 1,851 |
| trigger p95 ms | 1,848 | 1,472 | 3,370 | 3,245 |
| batch samples | 143* | 60 | 60 | 59 |
| alerts total | 0 | 0 | 399 | 2 |
| lag avg (s) | n/a | n/a | 68.69 | 69.50 |

*100-vehicle batch_samples=143 is inflated. This run predated the JSONL historical-bleed fix (see below). Spark metrics for that run are averaged over ~12 minutes of session history rather than the 300-second window. The ev/s and spark_input_rps values are still accurate.

### Harness Issues Found and Fixed During Phase 6B

**Issue 1: FLEET_TELEMETRY_TOPIC not subscribed (Cursor truncation)**

Cursor's implementation of the dual-topic consumer defined `FLEET_TELEMETRY_TOPIC` as a constant but did not add it to the `KafkaConsumer` subscription — the consumer only subscribed to `RISK_ALERTS_TOPIC`. As a result, `telemetry_ingress` would always show 0 events.

Fix: added `FLEET_TELEMETRY_TOPIC` to the `KafkaConsumer(...)` constructor alongside `RISK_ALERTS_TOPIC`.

**Issue 2: JSONL historical bleed (Spark metrics contaminated by pre-benchmark history)**

`_ProgressWriter` appends one JSON line per Spark batch to `/tmp/logishield-spark-progress.jsonl`. The file is cleared only when Spark restarts (in `onQueryStarted`). When Spark keeps running between fleet-size changes, the file accumulates all batches from session start. `bench.py` initialized `last_progress_line = 0`, so each benchmark run read all historical lines, averaging Spark metrics across the entire session rather than just the 300-second observation window.

Evidence: 100-vehicle run showed `batch_samples: 143` (expected ~60 for 300s ÷ 5s trigger = 60 batches). 143 batches × 5s ≈ 715 seconds of Spark history was being averaged.

Fix: `last_progress_line` now initialises to the current line count of the JSONL file at benchmark start, skipping all pre-existing history. The 1,000/5,000/10,000-vehicle runs all show `batch_samples: 59–60`, confirming the fix.

**Issue 3: Debug prints left in bench.py (Cursor truncation)**

`_debug_printed`, `_missing_fields`, `_parse_failures`, `_negative_lags` state variables and their associated `[debug]` stderr prints were not removed by Cursor due to prompt truncation. These were removed manually.

### Key Decisions Made

**JSONL append over single-file overwrite for streaming metrics**

`_ProgressWriter.onQueryProgress` appends one JSON line per batch rather than overwriting a single JSON file. This eliminates the race condition where `bench.py` reads the file mid-write and gets a partial or corrupted JSON object. Each line is a complete self-contained record, and the reader skips lines already processed using `last_progress_line`.

**`triggerExecution` key from `durationMs` dict**

Spark's `durationMs` is a dict with multiple keys (e.g. `addBatch`, `getBatch`, `triggerExecution`). `triggerExecution` represents total wall-clock time for the batch from trigger to completion — the correct value to compare against the 5-second trigger interval to assess whether Spark is keeping up.

**5-minute window / 1-minute watermark for Phase 6B**

Reduced from 10-minute window / 5-minute watermark used in Phases 3–5. The shorter window reduces cold start from 15 minutes to 6 minutes and makes lag values more meaningful at 60–90s rather than 360–600s. The watermark/window ratio (1:5) is preserved so the relative semantics are unchanged.

**Cold start only required once per session**

When Spark keeps running between fleet-size changes, only the simulator is restarted. Existing Spark state and window aggregations carry over. Fleet-size changes after the first only require 2–3 minutes (watermark advance + one window slide) rather than the full 6-minute cold start.

### Findings

**Finding 1: Kafka is not the bottleneck at any scale tested**

`spark_input_rps` tracks `events_per_second` exactly at every fleet size — no backpressure, no lag behind the broker. Kafka is consuming from the simulator and Spark is consuming from Kafka at the same rate, with no gap.

**Finding 2: Spark is not the bottleneck at any scale tested**

Trigger duration never exceeded 67% of the 5-second trigger budget (p95 = 3,370ms at 5,000 vehicles; p95 = 3,245ms at 10,000 vehicles). Spark has headroom remaining at the maximum fleet size tested.

`spark_process_rps` running higher than `spark_input_rps` is expected: each input row falls into multiple overlapping windows (5-min window / 30-sec slide = up to 10 windows per event), so Spark produces more output rows from aggregations than it receives as input.

**Finding 3: The Python simulator is the throughput ceiling**

Per-vehicle event rate drops sharply as fleet size scales beyond what the heap scheduler can sustain:

| Fleet size | ev/s | ev/s per vehicle |
|---|---|---|
| 100 | 105.50 | 1.055 |
| 1,000 | 1,114.17 | 1.114 |
| 5,000 | 3,921.94 | 0.784 |
| 10,000 | 6,544.05 | 0.654 |

Expected linear throughput at 5,000 vehicles would be ~5,500 ev/s. Observed is 3,922 ev/s — a 29% shortfall. The per-vehicle rate falls because the 4-thread heap scheduler accumulates overhead as each shard grows: more heappush/heappop operations per second, scheduling granularity limits from `stop.wait()`, and jitter effects. This is consistent with the ceiling characterised in Phase 5A/5B (~10,500 ev/s Python ceiling, ~7,000 ev/s in practice).

**Finding 4: Alert metrics are transition-driven, not throughput-driven**

Alerts are only emitted by `write_on_transition` when a vehicle's tier changes. With 5-minute windows averaging across ~20 simulator scenario cycles (15-second GREEN/YELLOW/RED loop), each vehicle settles into a stable average tier shortly after startup. Tier transitions happen once per vehicle at warm-up; thereafter the tier is stable and no further alerts are emitted.

The benchmark consumer uses `auto_offset_reset="latest"` and misses the warm-up transition burst. This explains:
- 100 and 1,000 vehicles: 0 alerts (all transitions during 6-minute cold start, before consumer joined)
- 5,000 vehicles: 399 alerts with 68.69s lag (some transitions fell within the benchmark window)
- 10,000 vehicles: 2 alerts (transitions mostly occurred before the benchmark consumer joined; lag of ~69.5s confirms the pipeline was working correctly for the 2 that arrived)

Alert throughput is not a meaningful pipeline throughput metric under this design. Telemetry ingress rate and Spark input rate are the correct measurements for bottleneck analysis.

**Finding 5: Processing lag is stable at 68–70 seconds**

At 5,000 vehicles (the only fleet size with a meaningful lag sample), lag avg = 68.69s, p50 = 68.96s, p95 = 70.93s. The tight distribution confirms the lag is deterministic: 5-minute window + 1-minute watermark + 5-second trigger = ~66 seconds minimum, with ~2–5 seconds of Spark write overhead on top. This is the expected value and indicates the pipeline is processing in real time with no accumulating backlog.

### Phase 6B Complete

Exit criteria satisfied:
- Throughput data collected at 100, 1,000, 5,000, and 10,000 vehicles
- Current throughput ceiling documented: Python simulator is the limiting component at ~6,500 ev/s for 10,000 vehicles
- Kafka and Spark both have headroom at all tested fleet sizes
- Results saved to `benchmarks/results/` in structured JSON for future Redis comparison
- Bottleneck identified: Python simulator heap scheduler, not Kafka or Spark

---

## 2026-06-24 — Phase 7 Pre-Work: Redis Service, TTL Design, ADR

### What Was Completed

- `redis:7` service added to `docker-compose.yml` with healthcheck on port 6379
- `ARCHITECTURE_DECISIONS.md` Decision 14 written: TTL-based GREEN state recovery — `truck:{vehicle_id}` keys expire after 7 minutes so recovered trucks fall out of `fleet:counts` without modifying the Spark pipeline
- `docs/phases/phase_7/phase_7_redis_integration.md` created: merged Phase 7A/7B design, TTL strategy, and validation checklist
- Phases 7A and 7B merged into a single implementation plan

### Key Decisions Made

**TTL is the mechanism for GREEN state recovery**
Spark does not emit GREEN transitions — the `write_on_transition` filter only passes YELLOW and RED alerts to `risk-alerts`. This means Redis would accumulate stale YELLOW/RED keys forever for trucks that have recovered. The solution: set a 7-minute TTL on every `truck:{vehicle_id}` key, refreshed on each alert. A truck that goes quiet (because Spark is no longer generating alerts for it) will have its key expire naturally. 7 minutes = 5-minute window + 1-minute watermark + ~1 minute of write overhead — the minimum time before a key can legitimately go stale.

**Phases 7A and 7B merged**
The original plan split Redis consumer (7A) and Streamlit dashboard (7B) into sequential phases. Because the consumer and dashboard are independent components with no shared code, they were implemented together in a single pass.

---

## 2026-06-29 — Phase 7: Redis State Layer and Streamlit Operations Dashboard

### What Was Completed

- `redis_consumer/state_consumer.py` — lightweight Kafka consumer that reads `risk-alerts` and materializes per-truck state into Redis
- `dashboard/app.py` — read-only Streamlit operations dashboard that visualizes fleet state from Redis
- `redis_consumer/__init__.py` — package marker enabling `python -m redis_consumer.state_consumer` entry point
- `dashboard/__init__.py` — package marker
- `requirements.txt` updated: `redis==5.0.8`, `streamlit==1.41.1` added
- `ARCHITECTURE_DECISIONS.md` Decision 15: Redis as the operational state layer

### Redis State Consumer (`redis_consumer/state_consumer.py`)

**Key patterns written:**

| Redis key | Type | Contents | TTL |
|-----------|------|----------|-----|
| `truck:{vehicle_id}` | Hash | `vehicle_id`, `tier`, `delivery_buffer`, `avg_temperature`, `window_start`, `window_end`, `alert_ts`, `reason` | 420s |
| `fleet:counts` | Hash | `RED` count, `YELLOW` count | None (manually maintained) |
| `fleet:last_update` | Hash | `ts`, `vehicle_id`, `tier` | None |

**Startup reconciliation:** On process start, the consumer rebuilds `fleet:counts` by scanning all `truck:*` keys and summing their tiers. This self-heals any count drift caused by TTL expiry during downtime — the consumer does not need to replay Kafka history to recover a correct count.

**Incremental count update:** When a truck transitions tier (e.g. YELLOW → RED), the consumer decrements the old tier count and increments the new one in the same write. No full scan needed per message.

**Consumer group:** Uses its own Kafka consumer group (`logishield-redis-state`), independent of any benchmarking or Spark consumer. Starts from `latest` — does not replay historical alerts.

### Streamlit Dashboard (`dashboard/app.py`)

Four sections:

1. **System Health** — Redis ping + pipeline staleness (inferred from `fleet:last_update.ts` compared to wall clock; stale threshold = 600s)
2. **Fleet Summary** — RED and YELLOW metric tiles from `fleet:counts`
3. **Active Trucks** — table of all `truck:*` hashes, sorted by severity (RED first, then YELLOW), showing `vehicle_id`, `tier`, `delivery_buffer`, `avg_temperature`, `reason`, `last_update`
4. **Truck Lookup** — free-text input to fetch a single `truck:{vehicle_id}` hash via `r.hgetall()`

The dashboard auto-refreshes every 5 seconds via `time.sleep(5)` + `st.rerun()`. It performs no business logic and never modifies Redis.

**Staleness as a proxy for pipeline health**
Rather than attempting to ping Kafka or Spark directly (which would require connections the dashboard should not own), the dashboard uses `fleet:last_update.ts` freshness as a signal. If the timestamp is more than 10 minutes old, the pipeline is assumed stalled. This is a deliberate design choice — it keeps the dashboard dependency-free from Kafka and Spark and avoids false negatives from intermittent network latency.

### Validation Results

- Validated with 5,000-truck fleet run
- `83 YELLOW` and `1 RED` alert correctly reflected in fleet summary metrics
- Active truck list sorted correctly with RED entry appearing above YELLOW entries
- Per-truck lookup confirmed working: arbitrary `vehicle_id` input returned full hash
- Expired truck keys disappeared automatically from the active truck list without any code change or Redis command
- `fleet:counts` remained consistent with visible truck keys throughout the run

### Run Commands

```bash
# Terminal 1 — Redis consumer
python -m redis_consumer.state_consumer

# Terminal 2 — Dashboard
streamlit run dashboard/app.py
```

### Phase 7 Complete

The streaming pipeline now has a complete demonstrable end-to-end flow:

**Telemetry → Kafka (`fleet-telemetry`) → Spark → Kafka (`risk-alerts`) → Redis → Dashboard**

Kafka owns event history. Spark owns risk detection. Redis owns current operational state. The dashboard provides a read-only operational view without touching the upstream pipeline or replaying Kafka history.

---

## 2026-06-29 — Phase 8A: Trip Lifecycle, Static Trip Metadata, Spark IN_TRANSIT Filter

### What Was Completed

- `simulator/simulator.py` — four-state trip lifecycle state machine added to `Vehicle`
- `simulator/simulator.py` — `TripContext` dataclass added with static trip metadata generation
- `simulator/models.py` — `TelemetryEvent` schema expanded with `trip_state` and six static trip fields
- `spark_streaming/stream_processor.py` — `IN_TRANSIT` filter added before watermarking; static trip fields propagated through aggregation into `risk-alerts`
- `redis_consumer/state_consumer.py` — six new trip fields stored in `truck:{vehicle_id}` hashes
- `dashboard/app.py` — `TRUCK_COLUMNS` updated to surface `cargo_type` and `customer_priority`
- `ARCHITECTURE_DECISIONS.md` Decision 16: IN_TRANSIT filter rationale

### Trip Lifecycle State Machine

Trucks cycle through four states driven by wall-clock deadlines with jitter:

| State | Duration | Trigger |
|-------|----------|---------|
| `IDLE` | 30–90s | Initial state; re-entered after DELIVERY_COMPLETE |
| `LOADING` | 1–3 min | Begins when IDLE deadline passes; TripContext assigned here |
| `IN_TRANSIT` | 5–15 min | Begins when LOADING deadline passes; risk telemetry meaningful |
| `DELIVERY_COMPLETE` | 1 tick | Immediately transitions back to IDLE |

`_maybe_advance_lifecycle()` is called at the top of `generate_event()`. It checks `time.time() >= self._state_deadline` and advances the state machine if true. The state machine requires no explicit FSM framework — a sequence of `if/elif` blocks on `self.trip_state` is sufficient.

### TripContext Dataclass

`TripContext` is a Python `dataclass` assigned at the moment the truck enters `LOADING` (not `IN_TRANSIT`) so that static metadata is stable before the first in-transit event is emitted.

Fields assigned at creation:

| Field | Values |
|-------|--------|
| `trip_id` | UUID4 |
| `cargo_type` | Pharmaceuticals / Fresh Food / Frozen Goods / Electronics / General Freight |
| `cargo_value` | $5,000–$500,000 |
| `customer_priority` | Standard / Priority / Critical |
| `service_level` | Standard / Express / Same-Day |
| `destination_region` | North / South / East / West |

Fields are immutable for the trip duration. When a truck re-enters `IDLE`, `self._trip_context` is set to `None`.

### Event Schema Changes

`TelemetryEvent` now carries `trip_state` plus all six `TripContext` fields. Non-IN_TRANSIT events emit empty strings / `0.0` for context fields where no `TripContext` exists.

### Spark Changes

**Filter before watermark:** `in_transit = with_metrics.filter(col("trip_state") == "IN_TRANSIT")` is applied before `.withWatermark()`. This ensures that IDLE, LOADING, and DELIVERY_COMPLETE events never enter the windowed aggregation path and cannot produce spurious alerts from their `delivery_buffer = 0.0` and `cargo_temperature = 0.0` values.

**Static field propagation:** All six trip fields added to the `.select()` after `from_json`, aggregated with `first()` in the windowed `.agg()`, and included in `alert_records`. They now appear in every `risk-alerts` message alongside the existing risk fields.

### Key Design Decisions

**Filter at `trip_state`, not at value level**
An alternative was to filter events where `delivery_buffer == 0.0` or `cargo_temperature == 0.0`. This would be fragile — a legitimate IN_TRANSIT event could theoretically have these values near zero during a GREEN phase. Filtering on `trip_state == "IN_TRANSIT"` is semantically correct and explicit.

**`TripContext` assigned at LOADING, not IN_TRANSIT**
If assigned at IN_TRANSIT, the first emitted in-transit event would have no context yet (race condition). Assigning at LOADING gives the context one full state duration to be stable before any in-transit events are emitted.

**`first()` for static fields in aggregation**
Trip metadata is constant within a trip — all events in a window share the same `trip_id`, `cargo_type`, etc. `first()` correctly captures this: any row in the window will have the same value, so the first one is authoritative.

### Validation Results

- Trucks confirmed cycling through all four states in terminal output
- IDLE/LOADING/DELIVERY_COMPLETE events confirmed not producing any alerts in `risk-alerts`
- IN_TRANSIT events producing YELLOW/RED alerts as expected
- `cargo_type`, `customer_priority`, `trip_id`, and other static fields confirmed present in `risk-alerts` messages
- Dashboard active truck list showed `cargo_type` and `customer_priority` columns populated correctly
- Per-truck lookup returned full enriched records including all six trip fields

### Phase 8A Complete

LogiShield no longer runs a single endless telemetry loop. Each truck now follows a realistic delivery lifecycle. The risk pipeline operates only on operationally meaningful events. The system is ready for dynamic operational fields in Phase 8B.

---

## 2026-06-29 — Phase 8B: Dynamic Operational Fields and Trip-Scoped Alert Deduplication

### What Was Completed

- `simulator/models.py` — four dynamic fields added to `TelemetryEvent`: `route_progress`, `estimated_arrival_minutes`, `remaining_stops`, `driver_hours_remaining`
- `simulator/simulator.py` — `TripContext` extended with `remaining_stops_initial`, `shift_hours`, `trip_start_time`; `_compute_dynamic_fields()` method added to `Vehicle`
- `spark_streaming/stream_processor.py` — schema, select, aggregation, and alert payload updated for all four dynamic fields; `last_tiers` key changed from `vehicle_id` to `(vehicle_id, trip_id)`
- `ARCHITECTURE_DECISIONS.md` Decision 17: transition detection key rationale

### Dynamic Fields

Four fields are computed on every IN_TRANSIT event from fixed formulas:

| Field | Formula | Aggregation in Spark |
|-------|---------|----------------------|
| `route_progress` | `elapsed / trip_duration`, clamped to `[0, 1]` | `max()` — furthest point reached in window |
| `estimated_arrival_minutes` | `(1 - route_progress) × trip_duration_minutes`, rounded | `min()` — shortest remaining time in window |
| `remaining_stops` | `initial_stops - int(route_progress / threshold_interval)` | `min()` — fewest stops remaining in window |
| `driver_hours_remaining` | `shift_hours - elapsed_hours` | `min()` — most depleted value in window |

`trip_start_time` is set to `time.time()` at the exact moment the truck enters IN_TRANSIT (not when LOADING begins), so `elapsed` is accurate from first in-transit event.

During non-IN_TRANSIT states: all four fields emit `0.0` / `0`.

### Aggregation Strategy: max/min over first

Dynamic fields change during the window. `first()` would capture the start-of-window value — the stalest possible reading. Instead:
- `max(route_progress)` captures the truck's furthest position during the window
- `min(remaining_stops)`, `min(driver_hours_remaining)`, `min(estimated_arrival_minutes)` capture the most operationally conservative (worst-case) values

This means the alert payload reflects the state of the truck at the end of the window rather than the beginning, which is more actionable for operations teams.

### Bug Fixed: Trip-Scoped Alert Deduplication

**The bug:** `write_on_transition` tracked `last_tiers` as `dict[str, str]` keyed on `vehicle_id`. After Phase 8A, trucks complete trips and start new ones. If truck `TRUCK_0001` emitted a `RED` alert on trip 1, went idle, and then re-entered `IN_TRANSIT` on trip 2 — also generating a `RED` alert — `write_on_transition` would see `last_tiers["TRUCK_0001"] == "RED"` unchanged and suppress the alert entirely. Redis and the dashboard would remain stale until Spark restarted.

**Confirmed in validation:** Trucks on their second trip generated no Redis updates. Dashboard remained empty.

**The fix:** Changed `last_tiers` to `dict[tuple[str, str], str]` keyed on `(vehicle_id, trip_id)`. Each UUID `trip_id` is unique, so every new trip is guaranteed to emit at least one alert on first tier classification, regardless of what the previous trip's final tier was.

**Impact:** One-line change to the key expression and the type annotation. Deduplication behavior within a single trip is identical.

### Validation Results

- `route_progress` confirmed advancing from `0.0` toward `1.0` across successive windows
- `estimated_arrival_minutes` confirmed decreasing as `route_progress` increases
- `remaining_stops` confirmed decrementing at correct thresholds
- `driver_hours_remaining` confirmed decreasing during transit and resetting to a new value on each new trip
- Dynamic fields confirmed `0 / 0.0` during IDLE, LOADING, DELIVERY_COMPLETE
- All four fields confirmed present in `risk-alerts` messages
- Trip-scoped deduplication fix confirmed: trucks on second/third trips now generate alerts correctly
- Existing risk logic (delivery_buffer threshold, avg_temperature threshold, tier classification) unchanged

### Phase 8B Complete

The telemetry stream now carries realistic, time-varying operational context on every IN_TRANSIT event. Each alert in `risk-alerts` contains a full operational snapshot: risk tier, delivery urgency, cargo details, route progress, stops remaining, driver hours, and trip identity. Redis consumer and dashboard column updates for the new dynamic fields are deferred to Phase 8C.

---

## 2026-06-30 — Phase 8C: Schema Propagation

### What Was Completed

- `spark_streaming/stream_processor.py` — `lit("IN_TRANSIT").alias("trip_state")` added to `alert_records` select so `trip_state` appears in every `risk-alerts` message
- `redis_consumer/state_consumer.py` — five new fields added to `truck:{vehicle_id}` hash: `trip_state`, `route_progress`, `estimated_arrival_minutes`, `remaining_stops`, `driver_hours_remaining`
- `dashboard/app.py` — `TRUCK_COLUMNS` extended with `trip_state`, `route_progress`, `estimated_arrival_minutes`; `route_progress` formatted as a percentage string (e.g. `"43%"`) via `_fmt_route_progress()`
- `docs/phases/phase_8/phase_8c.md` — plan doc written; `route_id` removed from field list (field does not exist in the simulator schema)

### Bug Fixed: `trip_state` Missing from `risk-alerts` Payload

`trip_state` was parsed from Kafka and used as the IN_TRANSIT filter (`col("trip_state") == "IN_TRANSIT"`), but was never added to the windowed aggregation or the `alert_records` select. It was silently absent from every `risk-alerts` message. The Redis consumer stored an empty string and the dashboard column showed nothing.

Since all alerts by definition originate from IN_TRANSIT events (the filter guarantees this), `lit("IN_TRANSIT")` is the correct and simplest fix — no aggregation change required.

### Key Design Decisions

**`lit("IN_TRANSIT")` over `first(col("trip_state"))` in aggregation**
Adding `first(col("trip_state"))` would have required an aggregation change and a schema change in the windowed agg. Since the IN_TRANSIT filter upstream guarantees the value is always `"IN_TRANSIT"`, `lit("IN_TRANSIT")` in the `alert_records` select is equivalent, simpler, and requires no upstream changes.

**`route_progress` displayed as percentage, stored as float**
Redis stores the raw float (`"0.4321"`) so downstream consumers retain full precision. The dashboard formats it to `"43%"` at display time via `_fmt_route_progress()`. The conversion is isolated to the display layer and does not affect any other consumer.

**`remaining_stops` and `driver_hours_remaining` in Redis but not dashboard table**
Both fields are stored in Redis and visible in the per-truck JSON lookup (Section 4 of the dashboard). They are not added to `TRUCK_COLUMNS` to keep the active truck table readable — the five columns already added (`trip_state`, `route_progress`, `estimated_arrival_minutes`, plus the existing `cargo_type`, `customer_priority`) give operators the most actionable at-a-glance context.

### Phase 8C Complete

The enriched trip schema now flows end-to-end through the full pipeline:

**Simulator → Kafka → Spark → `risk-alerts` → Redis → Dashboard**

---

## 2026-07-03 — Phase 9: Domain Pivot to EV Charging Network Operations

### Why We Pivoted

After completing Phase 8, the underlying streaming architecture was mature: a stateful Kafka → Spark → Redis → Dashboard pipeline with realistic lifecycle simulation, windowed risk classification, event-time watermarking, and transition-deduplication alerting. However, the "truck delivery monitoring" narrative was generic and lacked a clear operational problem.

The decision was made to migrate the domain to **EV charging network operations** — a real-time platform for monitoring the health and availability of a distributed EV charging network across Singapore. The operational problem is more compelling: operators need to know which chargers are degraded, faulted, or overloaded in real time, before sessions fail or customers are stranded.

The architecture is unchanged. Every distributed systems property built across Phases 1–8 is preserved:
- Kafka as the central event bus
- Spark Structured Streaming with sliding windows and watermarking
- Stateful foreachBatch transition deduplication
- Redis per-asset state with TTL-based eviction
- Streamlit operations dashboard

Only the domain vocabulary changed. Trucks became chargers. Trips became sessions. Cargo temperature became charger temperature. The delivery buffer became the session buffer.

The pivot also positions the project for a future capability that was not feasible in the truck domain: **geographic charger recommendations**. Every charger now carries real Singapore coordinates (`charger_lat`, `charger_lng`) in its schema, enabling a future map-based view and best-charger query without any pipeline changes.

---

## 2026-07-03 — Phase 9A: Simulator and Data Model

### What Was Completed

- `simulator/models.py` — `TelemetryEvent` migrated to EV schema: `charger_id`, `charger_temperature`, `session_state`, `session_id`, `connector_type`, `energy_requested_kwh`, `user_tier`, `charging_speed`, `site_region`, `session_progress`, `power_output_kw`, `energy_delivered_kwh`, `session_time_remaining`, `session_buffer_threshold`, `temp_threshold`, `estimated_completion_minutes`. New static fields: `charger_lat`, `charger_lng`, `rated_power_kw`, `site_id`.
- `simulator/simulator.py` — `Vehicle` → `Charger`, `Fleet` → `Network`, `TripContext` → `SessionContext`. Lifecycle states renamed: `IDLE → AVAILABLE`, `LOADING → INITIALIZING`, `IN_TRANSIT → CHARGING`, `DELIVERY_COMPLETE → SESSION_COMPLETE`. `Scenario.generate_values()` updated for charger temperature and session timing ranges. Singapore site pool added (`_SITES`, 10 locations). `_rated_power()` helper assigns kW capacity by connector type. Kafka topic updated to `charger-telemetry`. CLI arg `--fleet-size` → `--network-size`.
- `docker-compose.yml` — `kafka-init` updated to create `charger-telemetry` topic instead of `fleet-telemetry`.

### Bug Fixed During Review: `site_region` Inconsistency

Cursor assigned `site_region` via `SessionContext.generate()`, which picked randomly from `_SITE_REGIONS`. This meant a charger physically at Changi Airport (East) could report `site_region="North"` during a session — inconsistent with `charger_lat`, `charger_lng`, and `site_id`.

Fix: removed `site_region` from `SessionContext`. Added it as a static field on `Charger`, assigned from the `_SITES` pool at construction time and emitted on every event from `self.site_region`. Guarantees geographic consistency across all charger fields.

### Key Design Decisions

**`site_region` as a static charger property, not a session property**
`site_region` reflects physical location, which does not change between sessions. Assigning it per-session would decouple it from `charger_lat`/`charger_lng`/`site_id`, breaking geographic consistency. The fix stores it on the `Charger` alongside the other static location fields.

**`estimated_completion_minutes` replaces both `time_left_to_destination` and `estimated_arrival_minutes`**
The old schema had two time fields that both expressed "time remaining." In the EV schema, these are merged into a single clean field: `estimated_completion_minutes`. The Scenario-generated value drives the risk signal (session buffer); the dynamically computed value from `_compute_dynamic_fields()` is no longer needed as a separate field.

**`power_output_kw` replaces `remaining_stops`**
Real-time power delivery is the EV equivalent of operational progress. A charger derated to 30% of rated capacity is the most operationally interesting live signal for network operators.

### Bug Found During Testing: `charger-telemetry` Topic Missing

After updating `docker-compose.yml`, Docker containers were already running from the previous configuration. `kafka-init` only runs on first start and does not re-run. The `charger-telemetry` topic did not exist, causing the Kafka producer's `send()` call to block waiting for topic metadata — worker threads appeared to run but generated 0 events.

Fix: created the topic manually with `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic charger-telemetry --partitions 12 --replication-factor 1`. For future reference: whenever `docker-compose.yml` topic names change, restart Docker or create missing topics manually if containers are already running.

---

## 2026-07-03 — Phase 9B: Spark Stream Processor

### What Was Completed

- `spark_streaming/stream_processor.py` — `TELEMETRY_SCHEMA` updated to EV field names. `structured` select updated. `session_buffer = session_time_remaining - estimated_completion_minutes` replaces `delivery_buffer`. Filter updated: `col("session_state") == "CHARGING"`. `groupBy` updated to `charger_id`. All 18 aggregations updated including new fields: `charger_lat`, `charger_lng`, `rated_power_kw`, `site_id`, `max_power_output_kw`, `max_energy_delivered_kwh`. Risk tiering updated to reference `avg_session_buffer` and `avg_charger_temperature`. `alert_records` select updated with EV reason strings. Deduplication key updated to `(charger_id, session_id)`. Topic subscription updated to `charger-telemetry`.

### What Was Not Changed

SparkSession configuration, watermark duration, window size and slide, `foreachBatch` pattern, checkpoint location, output topic (`risk-alerts`), `gen_id` UDF, trigger interval, output mode. The distributed systems core is identical.

### Checkpoint Note

Checkpoint must be cleared before restarting Spark after this change:
`rm -rf /tmp/logishield-checkpoints/risk-alerts`

---

## 2026-07-03 — Phase 9C: Redis Consumer and Dashboard

### What Was Completed

- `redis_consumer/state_consumer.py` — `_recount_fleet()` → `_recount_network()`. All key prefixes updated: `truck:` → `charger:`, `fleet:counts` → `network:counts`, `fleet:last_update` → `network:last_update`. `hset` mapping updated to all 23 EV fields including geographic fields. `CHARGER_KEY_TTL_SECONDS` renamed. Verbose log updated.
- `dashboard/app.py` — `TRUCK_COLUMNS` → `CHARGER_COLUMNS` with EV field set. All Redis key reads updated. Section headers updated: Network Summary, Active Chargers, Charger Lookup. `_fmt_route_progress()` → `_fmt_session_progress()`. Scan pattern updated to `charger:*`. Sort key updated to `charger_id`. Lookup updated to `charger:{charger_id}`.

### Phase 9 Complete

The full pipeline now operates as an EV charging network operations platform:

**Charger Simulator → `charger-telemetry` → Spark → `risk-alerts` → Redis → Dashboard**

Every layer carries the EV schema end-to-end. The distributed systems architecture built across Phases 1–8 is fully preserved. The domain is now operationally meaningful, geographically anchored to Singapore, and structured to support future capabilities (map view, best-charger recommendations) without pipeline changes.

Every layer — Spark output, Redis hash, and dashboard table — carries `trip_state`, route progress, and arrival estimates alongside the existing risk fields. The pipeline now represents a realistic logistics lifecycle observable from telemetry through to the operations dashboard without any change to the underlying risk detection logic.

---

## 2026-07-04 — Phase 10A: Real Singapore Charger Data — Source Evaluation

### What Was Completed

- Evaluated LTA DataMall EVCBatch API as the source for real Singapore EV charger inventory
- Fetched raw dataset using pre-signed S3 URL mechanism (API key authenticated, URL valid for 5 minutes)
- Confirmed data quality: 2,708 locations, 8,877 charging points, real coordinates and operator names
- Added `data/chargers_raw.json` to `.gitignore` (3.1 MB raw API response, not committed)

### Dataset Quality

| Metric | Value |
|---|---|
| Total locations | 2,708 |
| Total charging points | 8,877 |
| Geographic coverage | Singapore-wide, WGS84 coordinates |
| Connector types present | Type 2, CCS2 (Combo 2), CHAdeMO |
| Power range | 3.7 – 480.0 kW |
| Operators identified | SP Mobility, ComfortDelGro Engie, Shell, Charge+, Strides YTL, and others |
| Last updated | 2026-07-03 16:35:00 |

### Data Source Decision

LTA DataMall met all acceptance criteria: 8,877 charger records, latitude and longitude present for all records, real site names and addresses, power ratings for all entries, and major operator names correctly attributed. No fallback to a curated hand-built dataset was needed.

---

## 2026-07-04 — Phase 10B: Data Normalisation

### What Was Completed

- `scripts/build_charger_snapshot.py` — one-time normalisation script that reads `data/chargers_raw.json` and writes `data/chargers.json`
- `data/chargers.json` — committed normalised snapshot of 8,877 real Singapore EV charger records

### Normalisation Logic

- One charger record per `chargingPoint` in the raw dataset, keyed by the first `evCpId`
- Best plug type selected per charging point: highest `powerRating` wins
- Connector type mapped: `"Type 2"` → `"Type2"`, `"Combo 2"` → `"CCS2"`, `"CHAdeMO"` → `"CHAdeMO"`
- Site region derived from WGS84 coordinates using Singapore bounding boxes (South/North/East/West/Central)
- Site ID slugified from site name (alphanumeric + underscores, max 50 characters)

### Output Statistics

| Metric | Value |
|---|---|
| Total chargers | 8,877 |
| East | 3,077 |
| Central | 2,576 |
| West | 1,785 |
| North | 1,384 |
| South | 55 |
| Type2 connectors | 8,095 |
| CCS2 connectors | 782 |
| Power range | 3.7 – 480.0 kW |

To regenerate the snapshot: `python scripts/build_charger_snapshot.py` (requires `data/chargers_raw.json`; re-fetch via `scripts/fetch_chargers.py`).

---

## 2026-07-04 — Phase 10C: Simulator Integration

### What Was Completed

- `simulator/simulator.py` — `Network.from_dataset()` classmethod added; loads from `data/chargers.json`
- `Charger.__init__` updated to accept `connector_type` as a constructor parameter, stored as `self.connector_type`
- `SessionContext.generate()` updated to accept `connector_type: str` — sessions now use the charger's real connector type rather than a random selection
- `_SITES`, `_rated_power()`, `_CONNECTOR_TYPES`, `_SITE_REGIONS` removed — all static charger properties now come from the dataset
- CLI `--network-size` samples N chargers from the dataset (default: 3 for development; pass `8877` for full network)

### Key Design Decision

**`connector_type` flows dataset → Charger → SessionContext, not random**
Each physical charger supports a fixed connector type. Sessions at that charger must use the charger's type. Removing the random selection ensures every telemetry event carries the correct connector type for the physical hardware it represents.

---

## 2026-07-04 — Phase 10D: Alert Map

### What Was Completed

- `dashboard/app.py` — Alert Map section added between Network Summary and Active Chargers
- Uses `st.pydeck_chart` with a `pydeck.ScatterplotLayer` over Singapore
- RED alerts rendered as red dots `[220, 38, 38]`, YELLOW alerts as yellow dots `[234, 179, 8]`
- Tooltip shows `charger_id`, `site_id`, tier, and reason on hover
- Map centered on Singapore (lat 1.352, lng 103.820), zoom 11

### No Pipeline Changes

`charger_lat` and `charger_lng` were already stored in every `charger:{id}` Redis hash since Phase 9. The map reads from the same Redis keys as the Active Chargers table — no schema changes, no new Redis writes, no Spark changes required.

### Phase 10 Complete

The simulator now loads real Singapore EV charger records as its network inventory. Charger IDs, coordinates, site names, connector types, power ratings, and operator names reflect the actual Singapore public charging network as of July 2026. The dashboard map shows alerted chargers at their real geographic locations across Singapore.

---

## 2026-07-05 — Phase 11: Real-Time Network Analytics

### What Was Completed

- `spark_streaming/stream_processor.py` — second streaming query added in parallel with the existing risk alert query. `write_stats` foreachBatch function computes three groupBy aggregations per micro-batch (by region, by connector type, network-wide) and writes results directly to Redis. `count` added to PySpark imports. `import redis as redis_client` added. `query.awaitTermination()` replaced with `spark.streams.awaitAnyTermination()`.
- `dashboard/app.py` — Network Statistics section (Section 1.5) added between System Health and Network Summary. Reads `stats:network`, `stats:region:*`, and `stats:connector:*` from Redis. Renders four network-wide metric tiles, a regional breakdown table, and a connector type breakdown table. Degrades gracefully to a caption when Spark stats sink is not yet running.
- `docs/phases/phase_11/phase_11_analytics.md` — phase plan with questions being answered, Redis key design, and sub-phase structure
- `docs/limitations.md` — created: documents simulator-driven risk patterns, absence of historical store, single-node infrastructure constraint, and chaos injector staleness

### Redis Keys Written by Stats Sink

| Key pattern | Contents |
|---|---|
| `stats:region:{region}` | `active_sessions`, `avg_temperature`, `avg_session_progress`, `avg_session_buffer`, `avg_power_kw`, `last_update` |
| `stats:connector:{type}` | `active_sessions`, `avg_power_kw`, `avg_rated_power_kw`, `avg_utilization_pct`, `avg_energy_kwh`, `last_update` |
| `stats:network` | `total_active_sessions`, `avg_temperature`, `avg_session_buffer`, `avg_power_kw`, `last_update` |

### Key Design Decisions

**Stats tapped from `charging` df, not the windowed aggregation**
The windowed aggregation groups by `charger_id` and collapses many events into one row per charger per window. Tapping it for regional stats would lose the event count signal and produce averages of averages. The `charging` df (post-filter, pre-window) contains one row per raw event, giving correct per-batch counts and true field averages.

**foreachBatch with in-batch groupBy, not a second windowed query**
Regional stats don't need the same 5-minute sliding window as risk classification — a per-batch snapshot is sufficient. Using foreachBatch with `.groupBy()` inside the function is simpler, has no state to manage, and updates every 5 seconds rather than waiting for a window to close.

**`avg_session_buffer` meaning**
`session_buffer = session_time_remaining - estimated_completion_minutes`. Positive means sessions are on track; negative means sessions are collectively projected to overrun. The network-wide and regional averages give operators a quick read on whether the network is under session time pressure.

**Separate checkpoint for stats query**
`/tmp/logishield-checkpoints/charger-stats` is independent of `/tmp/logishield-checkpoints/risk-alerts`. The risk-alerts checkpoint remains valid across this change — no checkpoint clear required.

### Phase 11 Complete

The dashboard now surfaces real-time network analytics derived directly from the telemetry stream. Regional load, connector utilization, and network-wide averages update every 5 seconds alongside the existing alert view.

---

## 2026-07-10 — Phase 12: FastAPI Layer, Dashboard Decoupled from Redis, Full Containerization

### What Was Completed

- `api/main.py` — new FastAPI service exposing read-only Redis state over HTTP. Endpoints: `/health`, `/summary`, `/chargers`, `/chargers/{id}`, `/stats/network`, `/stats/regions`, `/stats/connectors`
- `dashboard/app.py` — refactored to be a pure HTTP client to the API. All direct Redis reads removed. Dashboard now has no Redis dependency; the API owns all data access
- `Dockerfile` — created with `python:3.11-slim` base. Java (`default-jre-headless`) added for Spark. Spark-Kafka connector JAR pre-downloaded into Ivy cache at build time so container startup is instant
- `docker-compose.yml` — `simulator`, `redis-consumer`, `api`, `dashboard`, and `stream-processor` all containerized. `spark-master` and `spark-worker` removed (unused — Spark runs in `local[*]` mode inside `stream-processor`). `redis-init` service added: runs `redis-cli FLUSHALL` on every startup before consumers begin writing, guaranteeing a clean state each run. `FLEET_SIZE` environment variable controls simulator network size (default: 20)
- `spark_streaming/stream_processor.py` — `KAFKA_BOOTSTRAP`, `REDIS_HOST`, `REDIS_PORT` now read from environment variables (defaults preserve local dev behaviour). `spark.jars.packages` config baked into `SparkSession` so no `spark-submit` invocation needed
- `requirements.txt` — `fastapi==0.115.6`, `uvicorn[standard]==0.34.0` added

### Bug Fixed: Stats Sink Inflated Session Counts

`write_stats` used `count("*")` to compute `active_sessions` and `total_active_sessions`. With a 5-second micro-batch and ~1 event/second per charger, each charger contributed ~5 rows per batch — inflating session counts by ~5x.

Fix: replaced `count("*")` with `countDistinct("charger_id")` in all three aggregations (by region, by connector type, network-wide). `countDistinct` imported from `pyspark.sql.functions`.

### Key Design Decisions

**API as the single Redis access boundary**
Before this phase, the dashboard read directly from Redis. Moving all Redis access into the API means the dashboard has no knowledge of the data store — it only knows HTTP. This makes the dashboard testable in isolation, decouples it from Redis key schema changes, and is the correct layering for a production system.

**Spark containerized in `local[*]` mode, not submitted to a cluster**
The `spark-master` and `spark-worker` containers were present but never used — the Spark job ran in `local[*]` on the host machine. This phase formalizes that: `stream-processor` is a container that runs `python spark_streaming/stream_processor.py`, which starts Spark in `local[*]` mode within the container. This is correct for a single-node demo and removes the orphaned cluster containers.

**Redis flushed on every startup via `redis-init`**
A one-shot service runs `redis-cli -h redis FLUSHALL` after Redis is healthy and before `redis-consumer` and `stream-processor` start. This guarantees that every `docker-compose up -d` produces a clean dashboard with no stale state from a previous run.

**Kafka connector JAR pre-downloaded at image build time**
The previous workflow required `spark.jars.packages` to download the JAR on first run (~10–30s, network-dependent). Triggering a SparkSession at build time populates `~/.ivy2` in the image layer. Subsequent container starts skip the download entirely.

**`FLEET_SIZE` environment variable**
Fleet size is set via `FLEET_SIZE=N docker-compose up -d`, eliminating the need to edit `docker-compose.yml`. Defaults to 20.

### Clean Start Workflow

```bash
# Full clean reset with new fleet size
docker-compose down -v && FLEET_SIZE=50 docker-compose up -d
```

`down -v` removes ZooKeeper and Redis volumes. `redis-init` flushes Redis before consumers start. Spark checkpoints are discarded with the container. No manual steps required.

### Phase 12 Complete

The entire stack now runs from a single command. No local Java, Python, or `spark-submit` required. `FLEET_SIZE=N docker-compose up -d` is the only command needed to start the pipeline at any scale.
