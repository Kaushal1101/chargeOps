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

### Remaining Phase 1 Items

- [ ] `scripts/smoke_test.py` — Spark-to-Kafka connectivity validation
- [ ] Spark-to-Kafka smoke test passes
- [ ] Python virtual environment initialized and `pip install -r requirements.txt` confirmed clean
