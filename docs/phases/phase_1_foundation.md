# Phase 1 — Foundation & Infrastructure

## Objective

Establish a reproducible local development environment and define the architectural contracts that all future components will follow.

By the end of this phase, the project should have a working Kafka + Spark environment, finalized documentation, a validated Spark-to-Kafka connection, and stable event contracts that prevent implementation drift.

---

## Scope

This phase focuses exclusively on:

- Project structure
- Architecture documentation
- Event schema definition
- Docker infrastructure
- Kafka setup and topic creation
- Spark cluster setup
- Infrastructure smoke testing
- Python environment setup

This phase does not include:

- Telemetry simulation
- Spark business logic
- Risk tiering
- AI agents
- Benchmarking

---

## Deliverables

### Documentation

- [x] README.md
- [x] CLAUDE.md
- [x] cursor/architecture_rules.mdc
- [x] docs/architecture.md
- [x] docs/event_schema.md

#### Success Criteria

Documentation clearly defines system purpose, architecture, component responsibilities, event contracts, and development workflow.

---

### Python Environment

- [ ] `requirements.txt` at repo root
- [ ] Virtual environment initialized (`venv/`)

Initial dependencies to include:

- `pyspark`
- `kafka-python`
- `pydantic`
- `pytest`

#### Success Criteria

```bash
source venv/bin/activate && pip install -r requirements.txt
```

Completes without errors. All packages importable in a Python 3.12 environment.

---

### Infrastructure

Create a Docker Compose environment containing:

- [ ] Zookeeper
- [ ] Apache Kafka
- [ ] Spark Master
- [ ] Spark Worker
- [ ] Kafka UI (required)

#### Kafka Networking

Kafka running inside Docker must advertise two separate listeners to support both container-to-container communication and host-machine access:

| Listener | Address | Used By |
|----------|---------|---------|
| `PLAINTEXT` | `kafka:9092` | Spark (container-to-container) |
| `PLAINTEXT_HOST` | `localhost:9093` | Simulator, scripts (host machine) |

This is a common source of Kafka connectivity failures in Docker. The `docker-compose.yml` must configure both `KAFKA_ADVERTISED_LISTENERS` and `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` explicitly before any components attempt to connect.

- Spark connects via `kafka:9092`
- The simulator and any locally-run scripts connect via `localhost:9093`

#### Success Criteria

```bash
docker compose up -d
```

All five services launch successfully and reach healthy status.

---

### Topic Initialization

Create Kafka topics with explicit configuration:

| Topic | Partitions | Replication Factor | Partition Key |
|-------|------------|-------------------|---------------|
| `fleet-telemetry` | 3 | 1 | `vehicle_id` |
| `risk-alerts` | 3 | 1 | `vehicle_id` |

Partition count of 3 is the development default. It supports the `hash(vehicle_id) mod N` strategy defined in the architecture, provides meaningful parallelism for local testing, and can be revisited before any production scaling decisions.

Replication factor of 1 is appropriate for a single-broker local environment.

#### Success Criteria

Both topics are visible and healthy in Kafka UI at `http://localhost:8080` (or configured port).

---

### Infrastructure Smoke Test

Add a lightweight Spark-to-Kafka connectivity smoke test to validate the infrastructure before Phase 2 begins.

Location: `scripts/smoke_test.py`

The smoke test must:

- Initialize a local SparkSession
- Connect to the `fleet-telemetry` Kafka topic via `kafka:9092`
- Confirm the connection succeeds and the topic is readable
- Report success or failure with a clear output message
- Contain no business logic (no parsing, no risk scoring, no windowing)

The smoke test should not fail if the topic has zero messages — an empty topic is a valid state at this stage.

#### Success Criteria

```bash
source venv/bin/activate && python scripts/smoke_test.py
```

Exits with a success message confirming Spark connected to Kafka and accessed the `fleet-telemetry` topic.

---

### Repository Structure

Target structure after Phase 1 is complete:

```text
logishield-pipeline/
│
├── simulator/
├── spark_streaming/
├── agent/
├── chaos/
├── benchmarks/
├── configs/
│
├── docs/
│   ├── architecture.md
│   ├── event_schema.md
│   └── phases/
│       └── phase_1_foundation.md
│
├── scripts/
│   └── smoke_test.py
│
├── cursor/
│   └── architecture_rules.mdc
│
├── CLAUDE.md
├── README.md
├── docker-compose.yml
└── requirements.txt
```

#### Success Criteria

Directory structure is committed and stable before implementation begins.

---

## Technical Decisions

### Event-Driven Architecture

All services communicate through Kafka. No service should directly call another service.

```text
Producer
    │
    ▼
  Kafka
    │
    ▼
Consumer
```

---

### Event Ownership

| Owner | Produces |
|-------|---------|
| Simulator | Telemetry events (`fleet-telemetry`) |
| Spark | Derived metrics and risk alerts (`risk-alerts`) |
| AI Agent | Business impact assessments and remediation summaries |

---

### Source of Truth

| Concern | Document |
|---------|---------|
| Architecture | `docs/architecture.md` |
| Event contracts | `docs/event_schema.md` |
| Development workflow | `CLAUDE.md` |
| Cursor implementation rules | `cursor/architecture_rules.mdc` |

---

## Exit Criteria

Phase 1 is complete when all of the following are true:

- [x] All documentation files exist and are committed
- [ ] `requirements.txt` exists and installs cleanly
- [ ] `docker compose up -d` launches all five services successfully
- [ ] Kafka is reachable
- [ ] Spark Master is healthy
- [ ] Spark Worker is healthy
- [ ] Kafka UI is accessible at configured port
- [ ] `fleet-telemetry` topic exists with 3 partitions
- [ ] `risk-alerts` topic exists with 3 partitions
- [ ] Both topics visible and healthy in Kafka UI
- [ ] Smoke test passes — Spark successfully connects to Kafka and accesses `fleet-telemetry`
- [ ] Event schema is finalized
- [ ] Project structure is committed

---

## Phase 1 Outcome

At the conclusion of this phase, LogiShield should possess a fully operational local streaming environment, a validated Spark-to-Kafka connection, and a complete set of architectural contracts.

Phase 2 should be able to focus entirely on telemetry simulation and stream processing logic without revisiting infrastructure or design decisions.
