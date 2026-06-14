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
