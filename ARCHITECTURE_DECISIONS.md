# Architecture Decision Log: LogiShield Pipeline

## 🚀 Project Overview

The **LogiShield Pipeline** is a real-time logistics risk detection system that ingests continuous fleet telemetry, classifies operational risk using stream processing, and generates AI-assisted remediation recommendations.

> **Analogy:** Think of this system like an air traffic control network — but for ground freight.
> - **The Simulator:** Individual trucks broadcasting their status, like transponders emitting position and vitals.
> - **Apache Kafka:** The central radio network carrying every transmission from every truck simultaneously.
> - **Spark Structured Streaming:** The control room analysts processing the incoming stream in real time, computing trends and flagging emerging problems.
> - **Risk Tiering Engine:** The duty officer classifying each shipment as Green, Yellow, or Red based on what the analysts report.
> - **AI Remediation Agent:** The operations commander who reads a flagged alert and generates the intervention order.

---

## 🏗️ Core Design Decisions

### Decision 1: Self-Describing Events (Fat Events)

**Context:** Risk classification in Spark requires per-vehicle thresholds — different trucks carry different cargo with different temperature tolerances and SLA windows. The original options were to hardcode global thresholds in Spark, or send an initialization message per vehicle that Spark would join against the telemetry stream.

**The Decision:** Embed per-vehicle threshold fields (`sla_buffer_threshold`, `cargo_temp_threshold`) directly into every telemetry event, repeating them on each message alongside the sensor readings.

**Justification:** The initialization message approach requires Spark to maintain a stateful join between two streams — holding each vehicle's threshold in a state store and waiting for it before classifying any telemetry event. This introduces ordering dependencies, restart recovery complexity, and significant Phase 3 scope expansion. The self-describing event pattern trades a small amount of message bloat for a dramatic reduction in processing complexity: Spark evaluates risk on a single row with no joins, no per-vehicle state store, and no sensitivity to message ordering. At 3 vehicles × 1 event/sec, the redundancy is completely negligible. Every event is independently interpretable, which also improves debuggability and replay safety.

---

### Decision 2: Dual Kafka Listener Configuration

**Context:** Kafka runs inside Docker, but two different categories of clients need to connect to it: Spark (running inside the same Docker network) and the simulator and local scripts (running on the host machine).

**The Decision:** Configure Kafka with two separate advertised listeners — `PLAINTEXT://kafka:9092` for container-to-container traffic and `PLAINTEXT_HOST://localhost:9093` for host-machine access.

**Justification:** A single listener cannot serve both contexts. If Kafka advertises `localhost`, containers cannot reach it by that name. If it advertises `kafka`, the host machine cannot resolve that hostname. The dual-listener approach is the standard solution and maps cleanly to the actual network topology: Spark always connects via `kafka:9092`, the simulator and scripts always connect via `localhost:9093`. This distinction is critical to preserve — mixing the two listeners is a common source of silent connectivity failures.

---

### Decision 3: Official Apache Spark Image over Bitnami

**Context:** The project originally planned to use `bitnami/spark` for the Docker Compose Spark cluster, which is a common choice in tutorials and examples.

**The Decision:** Switch to `apache/spark:3.5.1`, the official Apache image.

**Justification:** Bitnami removed their Spark images from Docker Hub, making `bitnami/spark` unavailable for new environments. Beyond availability, the official image is a more durable dependency — it is maintained by the Apache project itself and will track official releases. The configuration difference is minor: `bitnami/spark` used `SPARK_MODE` environment variables, while `apache/spark` requires explicit `spark-class` commands in the `command:` field of the Compose service definition.

---

### Decision 4: Init Container for Kafka Topic Creation

**Context:** Kafka topics must exist with the correct partition count before any producer or consumer connects. Auto-creation was considered but carries risk — if a client connects before the topic exists, Kafka will create it with default settings (typically 1 partition), silently undermining the partitioning strategy.

**The Decision:** Add a `kafka-init` service to Docker Compose that runs `kafka-topics` commands after Kafka passes its healthcheck, creates both topics with the correct configuration, and exits.

**Justification:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` is enforced on the broker to prevent any client from accidentally creating a topic with wrong partition counts. The init container runs once per `docker compose up`, is fully automated, and ensures both `fleet-telemetry` and `risk-alerts` always exist with 3 partitions and replication factor 1 before any other service starts. This eliminates an entire class of subtle ordering bugs.

---

### Decision 5: UUID v4 for Event IDs

**Context:** Each telemetry event requires a unique `event_id`. The most readable alternative — a padded sequential integer (e.g. `evt_000001`) — was considered but requires a shared counter across all vehicles.

**The Decision:** Generate `event_id` using `uuid4`.

**Justification:** With multiple vehicles emitting events concurrently, a sequential counter requires either a shared lock (serializing event generation) or a per-vehicle namespace (adding coordination logic). UUIDs are stateless, require no coordination, and are the standard identifier strategy in distributed systems. The minor loss of human readability in logs is an acceptable tradeoff.

---

### Decision 6: Python 3.12 Enforced for Virtual Environment

**Context:** The system default Python version on the development machine was 3.14. The first `pip install -r requirements.txt` run failed during the `pydantic-core` wheel build.

**The Decision:** Create the virtual environment explicitly with `python3.12 -m venv venv`.

**Justification:** `pydantic==2.7.4` depends on `pydantic-core`, which is compiled via PyO3. PyO3 version 0.21.2 only supports up to Python 3.12. Python 3.14 caused a hard build failure. Python 3.12 was already installed on the machine and is the version explicitly targeted in the Phase 1 specification. The project must always be run inside this venv to ensure dependency compatibility.

---

### Decision 7: Spark Checkpoint Location at /tmp (Dev Only)

**Context:** The Spark Kafka write sink requires a `checkpointLocation` — without it the query fails with `AnalysisException` on startup. This was not in the Phase 3E spec; Cursor added it correctly.

**The Decision:** Use `/tmp/logishield-checkpoints/risk-alerts` for development.

**Consequences:**
- First run creates the directory; subsequent runs resume from the saved offsets
- macOS may purge `/tmp` on reboot — if this happens, the checkpoint is lost and the job resets
- If the schema or output topic changes and offsets become stale, delete the checkpoint directory: `rm -rf /tmp/logishield-checkpoints/risk-alerts`

**Deferred decision for benchmarking/production:** Move to a stable path such as `./.checkpoints/risk-alerts` inside the project directory. Do not commit the checkpoint directory to git.

---

### Decision 8: `update` Output Mode Produces Duplicate Alerts (Deferred to Phase 4)

**Context:** With a 30-second slide and 10-minute window, `update` mode emits every window that changed in each micro-batch. The same `(window_end, vehicle_id)` pair will be re-emitted multiple times as new telemetry updates the rolling average — each emission produces a new Kafka message with a new `event_id`. A sustained RED condition will generate approximately 20 duplicate alerts per truck per emission cycle.

**Current state:** Accepted for Phase 3E. The pipeline is functionally correct — risk detection works — but the alert volume is noisy.

**Three options for Phase 4:**
1. **Live with it — agent deduplicates downstream.** Simplest. AI agent filters by `(window_end, vehicle_id)` before processing. No pipeline changes.
2. **Switch to `append` mode.** Emits each window exactly once after the watermark closes it. Adds ~5 minutes of latency. Cleaner data, worse demo responsiveness.
3. **`foreachBatch` with transition state.** Track the last-emitted tier per vehicle; only write when the tier changes. Most production-realistic. Most implementation complexity.

**Recommended for Phase 4:** Option 1 (agent deduplication) to unblock Phase 4, then Option 3 for the benchmarking phase.

---

### Decision 9: Alert `event_id` Should Be Deterministic Hash (Deferred to Phase 4)

**Context:** Related to Decision 8. Each duplicate emission of the same window generates a fresh UUID, making `event_id` useless for downstream deduplication — the AI agent cannot tell whether two alerts represent the same event or two distinct ones.

**Deferred decision:** Replace `uuid4()` with `hash(window_end || vehicle_id || risk_tier)` as the alert `event_id`. This makes the ID deterministic and idempotent — the same window-vehicle-tier combination always produces the same ID, enabling safe deduplication by `event_id` alone.

**Implement in Phase 4** alongside the deduplication strategy chosen for Decision 8.

---

### Decision 10: Alert Timestamp and Reason Formatting (Deferred to Phase 4)

**Context:** Two formatting deviations from the canonical schema were identified in Phase 3E:

1. **`event_ts` format mismatch.** The risk alert `event_ts` is produced by `.cast("string")` on `window.end`, which yields Spark's default format: `"2026-06-10 21:33:45"` (space separator, no timezone). The simulator and `event_schema.md` use ISO-8601: `"2026-06-10T21:33:45.123456Z"`. Any downstream parser expecting ISO-8601 will fail on alert events. Fix: replace `.cast("string")` with `date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'")`.

2. **`reason` field float precision.** The spec defines temperature reasons as `"...: 6.78C"` (2 decimal places). The current implementation uses `.cast("string")` which emits full double precision: `"...: 6.7831234567C"`. Fix: replace `.cast("string")` with `format_number(col("max_cargo_temperature"), 2)`.

**Implement both in Phase 4** when the alert schema is hardened for AI agent consumption.

---

### Decision 11: Threaded Simulator with Fleet Sharding (Phase 5A)

**Context:** The sequential simulator loop bottlenecked at ~2,000 events/sec in Phase 4D — Python's GIL and single-threaded iteration over the fleet was the ceiling, not Kafka or Spark.

**The Decision:** Replace the single loop with 4 worker threads, each owning a deterministic round-robin shard of the fleet (`vehicles[i::4]`). A single shared `KafkaProducer` is used across all threads. A `threading.Event` stop flag replaces `time.sleep()` in worker loops so threads wake immediately on shutdown.

**Justification:** Kafka sends are I/O-bound, not CPU-bound — threads help despite the GIL because each thread spends most of its time waiting on the producer's internal buffer, not computing. One producer per process is correct: `kafka-python-ng`'s `KafkaProducer` is thread-safe and batches more efficiently with a single shared buffer than with one producer per thread. `stop.wait(timeout)` instead of `time.sleep()` is critical for responsive shutdown — without it, Ctrl+C leaves threads sleeping for up to 1 second before they can exit.

**Result:** Standalone Python ceiling increased from ~2,000 ev/s to ~10,500 ev/s (5x improvement).

---

### Decision 12: Kafka Producer Tuning — Batching, Buffer, and LZ4 Compression (Phase 5B)

**Context:** After threading, the full pipeline (Python + Kafka + Spark) plateaued at ~3,000 ev/s. Python diagnostic and Spark input rate were at the same ceiling, meaning Kafka back-pressure was throttling the producer down to Spark's speed. The default `linger_ms=0` was causing ~10,000 individual produce requests per second to the broker.

**The Decision:** Set `linger_ms=10`, `batch_size=65536`, `buffer_memory=67108864`, and `compression_type="lz4"` on the shared producer.

**Justification:**
- `linger_ms=10` allows messages to accumulate for 10ms before sending, reducing broker request count by ~100x at high throughput
- `batch_size=65536` (64KB) allows larger batches to form, amortising per-request overhead
- `buffer_memory=67108864` (64MB, doubled from default) reduces the frequency of back-pressure blocking under load
- `lz4` chosen over `snappy` — both are fast low-overhead compressors; lz4 installed cleanly via pip while snappy's native dependency failed to download. JSON telemetry payloads compress well (~291 bytes → ~150 bytes), reducing broker I/O

**Result:** Python diagnostic under full pipeline load increased from ~3,000 ev/s to ~7,700 ev/s. Spark input rate increased from ~2,500 to ~3,300 ev/s.

---

### Decision 13: Topic Partition Count Increased from 3 to 12 (Phase 5B)

**Context:** `fleet-telemetry` and `risk-alerts` were created with 3 partitions — designed for the original 3-truck fleet. At 10,000+ trucks with 4 producer threads, all traffic was funnelled through 3 partitions, creating per-partition contention at the broker.

**The Decision:** Increase both topics to 12 partitions. `docker-compose.yml` kafka-init updated to create topics at 12 partitions on fresh stack startup.

**Justification:** 12 is divisible by 4 (producer threads), allowing each thread to target a distinct set of partitions. It also provides more parallel read tasks for Spark's `local[*]` executor. Topics were deleted and recreated (rather than altered in place) to purge accumulated messages from earlier test runs which were causing Spark to fall behind on a large backlog.

**Result:** Partition increase did not materially improve Spark input rate (~2,700 ev/s) — confirming the bottleneck had moved from Kafka to Spark's processing capacity, not partition contention. The decision is still correct: 3 partitions was under-provisioned for the current scale and would have become a bottleneck at higher Spark throughput.

---

### Decision 14: TTL-Based GREEN State Recovery for Redis Fleet State (Phase 7)

**Context:** The `risk-alerts` Kafka topic only receives YELLOW and RED transitions. GREEN is filtered out in Spark before reaching Kafka (`alerts = tiered.filter(col("risk_tier") != "GREEN")`). A Redis state consumer reading only `risk-alerts` has no signal for when a truck returns to GREEN. Without handling this, a truck that was RED and recovers will remain RED in Redis indefinitely, and `fleet:counts` will accumulate inflated YELLOW/RED counts over time.

**Options considered:**
1. Emit GREEN transitions to `risk-alerts` — modifying the Spark pipeline so the topic becomes a full state-change stream
2. TTL-based expiry on `truck:{vehicle_id}` keys in Redis
3. A separate reconciliation process that periodically recomputes state from Kafka history

**The Decision:** Use TTL-based key expiry on `truck:{vehicle_id}` Redis keys. TTL is set to `window_duration + watermark + safety_buffer = 5 min + 1 min + 1 min = 7 minutes`. If a truck returns to GREEN, no further alerts are emitted and the key expires naturally after ~7 minutes.

**Justification:** Emitting GREEN transitions is the most complete solution but changes the semantics of `risk-alerts` from an alert-only stream to a full state-change stream — a significant architectural shift. The TTL approach requires zero changes to the existing pipeline, is simple to reason about, and provides an acceptable approximation: Redis reflects *active non-GREEN alert state*, not complete fleet state. The 7-minute expiry window is long enough that a truck actively in YELLOW or RED will be refreshed well before expiry (alerts fire on tier changes; a sustained risk state will produce at least one alert per window close, every ~66 seconds), and short enough that recovered trucks are purged within one observation window.

**Explicit limitation:** Redis materializes active non-GREEN alert state, not the complete fleet state. A truck absent from Redis has either never generated an alert, or returned to GREEN and had its key expire. `fleet:counts` reflects trucks with active YELLOW/RED alerts only, not total fleet size.

**Future enhancement path:** Emit GREEN state transitions as a distinct event type in `risk-alerts` (or a dedicated topic) to support full fleet-state materialization without expiration-based cleanup. This would allow Redis to hold the definitive current tier for every vehicle regardless of recovery timing, and is the correct approach before any production dashboard is built.

---

### Known Issue 1: YELLOW Alerts Eclipsed by RED in Long-Running Windows

**Observed:** During Phase 3E validation, only RED alerts appeared in `risk-alerts`. No YELLOW alerts were produced despite the simulator cycling through YELLOW states.

**Root cause:** The simulator completes a full GREEN→YELLOW→RED cycle every 15 seconds. The aggregation window is 10 minutes wide. Within any 10-minute window, ~40 full cycles occur — meaning every window contains RED-level temperature events. Since `max_cargo_temperature` picks the highest value in the window, even a single RED event pushes the max above the threshold and classifies the entire window as RED. YELLOW events within the same window are eclipsed.

**Impact:** YELLOW tier classification is theoretically correct but practically unreachable in normal operation. The pipeline will produce RED alerts when conditions are dangerous, but the intermediate YELLOW warning stage is effectively invisible.

**Options to address in Phase 4 or beyond:**
1. Use `avg(cargo_temperature)` instead of `max` — smoother signal, less sensitive to transient spikes
2. Slow the simulator cycle (e.g. GREEN for 60 steps, YELLOW for 30, RED for 15) so windows capture distinct phases
3. Separate the temperature and buffer metrics into independent classifiers rather than combining them with `max`

**Not blocking Phase 4** — the AI agent will still receive meaningful RED alerts. Revisit when tuning alert quality.
