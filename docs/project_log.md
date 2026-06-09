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
