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

**Context:** The system default Python version on the development machine was 3.14. The first `pip install -r requirements.txt` run failed during the `pydantic-core` wheel build.

**The Decision:** Create the virtual environment explicitly with `python3.12 -m venv venv`.

**Justification:** `pydantic==2.7.4` depends on `pydantic-core`, which is compiled via PyO3. PyO3 version 0.21.2 only supports up to Python 3.12. Python 3.14 caused a hard build failure. Python 3.12 was already installed on the machine and is the version explicitly targeted in the Phase 1 specification. The project must always be run inside this venv to ensure dependency compatibility.
