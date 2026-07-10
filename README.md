# LogiShield — Real-Time EV Charging Network Operations

A real-time infrastructure operations platform that monitors the health and availability of a distributed EV charging network. The system ingests continuous telemetry from simulated charging stations across Singapore, processes the stream with Apache Spark, classifies charger health in real time, and surfaces operational alerts through a Redis-backed dashboard.

## Architecture

```text
Charger Simulator (Singapore charging network digital twin)
        │
        ▼
Apache Kafka (charger-telemetry)
        │
        ▼
Spark Structured Streaming (session-scoped windowed risk classification)
       │ │
       │ └─── Redis (stats:network / stats:region:* / stats:connector:*)
       ▼
Apache Kafka (risk-alerts)
        │
        ▼
Redis Consumer → Redis (charger:* / network:counts / network:last_update)
        │
        ▼
FastAPI Operational API (read-only Redis over HTTP)
        │
        ▼
Streamlit Dashboard (network operations console)
```

## What It Does

Each simulated charger cycles through a realistic session lifecycle: `AVAILABLE → INITIALIZING → CHARGING → SESSION_COMPLETE`. During active charging sessions, Spark continuously evaluates two risk signals:

- **Charger temperature** — whether the hardware is approaching or exceeding its thermal threshold
- **Session buffer** — whether the session is projected to complete within its scheduled window

Chargers are classified GREEN, YELLOW, or RED on a rolling 5-minute sliding window. Only YELLOW and RED transitions produce alerts, avoiding alert storms from sustained conditions. Alert state is materialized into Redis and displayed on the operations dashboard in real time.

## Tech Stack

| Layer | Technology |
|---|---|
| Simulation | Python, Pydantic |
| Message broker | Apache Kafka |
| Stream processing | Apache Spark Structured Streaming, PySpark |
| Operational state | Redis |
| Operational API | FastAPI, Uvicorn |
| Dashboard | Streamlit |
| Infrastructure | Docker Compose |

## Distributed Systems Properties

- **Event-time processing** — Spark windows on `event_ts` (when the event occurred), not ingestion time
- **Watermarking** — late-arriving events handled gracefully without corrupting stream state
- **Sliding windows** — 5-minute windows with 30-second slide intervals for continuous trend detection
- **Stateful deduplication** — alerts only fire on tier transitions, not on every window evaluation
- **TTL-based eviction** — charger state expires from Redis after inactivity, keeping the dashboard current

## Charger Network

The simulator loads real Singapore EV charger inventory from `data/chargers.json`, a normalised snapshot built from the LTA DataMall EVCBatch dataset (sourced July 2026).

| Metric | Value |
|---|---|
| Total chargers | 8,877 |
| Regions | North, South, East, West, Central |
| Connector types | Type2 (8,095), CCS2 (782) |
| Power range | 3.7 – 480.0 kW |
| Operators | SP Mobility, ComfortDelGro Engie, Shell, Charge+, Strides YTL, and others |

Use `--network-size N` to sample N chargers for development. Omit or pass the full count for a production-scale run. Every charger carries real `charger_lat` / `charger_lng` coordinates, visible on the dashboard map.

## Prerequisites

- Docker Desktop

## Running the Pipeline

```bash
# Start the full stack with a fleet of 20 chargers
FLEET_SIZE=20 docker-compose up -d
```

The dashboard is available at `http://localhost:8501`. Kafka UI is at `http://localhost:8080`.

For a clean reset (wipes all state and restarts with a new fleet size):

```bash
docker-compose down -v && FLEET_SIZE=50 docker-compose up -d
```

`down -v` removes volumes so ZooKeeper and Redis start clean. Redis is also flushed automatically on every `up -d` before consumers start. Spark checkpoints live inside the container and are discarded on container removal.

The `FLEET_SIZE` cap is the number of real chargers in `data/chargers.json` (8,877). Omitting `FLEET_SIZE` defaults to 20.

## Repository Structure

```text
├── simulator/          Charger network digital twin and session lifecycle simulation
├── spark_streaming/    Spark Structured Streaming risk classification pipeline
├── redis_consumer/     Kafka consumer that materializes alert state into Redis
├── dashboard/          Streamlit network operations console
├── benchmarks/         Throughput and latency measurement suite
├── chaos/              Fault injection utilities (out-of-order events, delayed bursts)
├── configs/            Shared configuration
└── docs/               Architecture, schema, and phase planning documentation
```
