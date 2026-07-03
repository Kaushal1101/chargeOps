# LogiShield Pipeline Architecture

## Overview

LogiShield is a real-time EV charging network operations platform. It ingests continuous telemetry from simulated charging stations, processes the stream with Apache Spark Structured Streaming, classifies charger health in real time, and surfaces operational alerts through a Redis-backed Streamlit dashboard.

The platform follows an event-driven architecture with clear separation between telemetry generation, stream processing, risk detection, and operational state management.

> **Note:** Phases 1–8 of this project used a truck delivery monitoring domain. Phase 9 migrated the domain to EV charging network operations while preserving the full streaming architecture. Historical phase documentation in `docs/phases/` reflects the original domain. All living documentation (this file, `event_schema.md`, `README.md`) describes the current EV charging system.

---

## High-Level Architecture

```text
┌──────────────────────────┐
│    Charger Simulator     │
│ (Singapore charging      │
│  network digital twin)   │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│      Apache Kafka        │
│   charger-telemetry      │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  Spark Structured        │
│     Streaming            │
│ (session-scoped windowed │
│  risk classification)    │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│      Apache Kafka        │
│      risk-alerts         │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│    Redis State Layer     │
│  (per-charger hash,      │
│   TTL-based eviction)    │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  Streamlit Dashboard     │
│ (network operations      │
│  console)                │
└──────────────────────────┘
```

---

## Component Responsibilities

| Component | Responsibility |
|---|---|
| Simulator | Generates realistic EV charger telemetry and session lifecycle events across a simulated Singapore charging network |
| Kafka | Serves as the event bus between producers and consumers |
| Spark Streaming | Processes telemetry events, computes windowed metrics, and classifies charger health |
| Risk Tiering Engine | Assigns GREEN, YELLOW, or RED based on charger temperature and session buffer signals |
| Redis Consumer | Materialises alert state into Redis hashes keyed by charger ID |
| Streamlit Dashboard | Read-only network operations console showing active charger alerts and session context |
| Chaos Injector | Simulates out-of-order events, delayed bursts, and packet loss |
| Benchmarking Suite | Measures throughput, latency, and consumer lag |

---

## Data Flow

1. The charger simulator emits a telemetry event for each charger on a per-charger interval.
2. The event is published to the `charger-telemetry` Kafka topic, keyed by `charger_id`.
3. Spark consumes and parses the event against `TELEMETRY_SCHEMA`.
4. Events where `session_state != "CHARGING"` are filtered out before aggregation.
5. A 5-minute sliding window (30-second slide) computes `avg_session_buffer` and `avg_charger_temperature` per charger.
6. The risk tiering logic assigns GREEN, YELLOW, or RED.
7. Only YELLOW and RED results pass the alert filter.
8. `foreachBatch` deduplication emits to `risk-alerts` only on tier transitions.
9. The Redis consumer reads `risk-alerts` and writes per-charger state to `charger:{charger_id}` hashes.
10. The Streamlit dashboard reads from Redis and auto-refreshes every 5 seconds.

---

## Kafka Configuration

### Topics

| Topic | Producers | Consumers | Partitions |
|---|---|---|---|
| `charger-telemetry` | Simulator | Spark | 12 |
| `risk-alerts` | Spark | Redis consumer | 12 |

### Dual Listener Configuration

Kafka is configured with two advertised listeners:
- `PLAINTEXT://kafka:9092` — for container-to-container traffic (used by Spark inside Docker)
- `PLAINTEXT_HOST://localhost:9093` — for host-machine access (used by the simulator, Redis consumer, and scripts)

### Partitioning

Events are partitioned by `charger_id`. All telemetry from a given charger lands on the same partition, preserving per-charger event ordering within the topic.

### Consumer Groups

| Consumer | Topic | Group ID |
|---|---|---|
| Spark Streaming | charger-telemetry | Managed via Spark checkpoint |
| Redis State Consumer | risk-alerts | `logishield-redis-state` |

---

## Risk Model

Risk classification is deterministic and reproducible. Two independent signals are evaluated per charger per window.

### Signal 1: Charger Temperature

Compared against `temp_threshold`, a per-charger value embedded in every telemetry event.

- GREEN: `avg_charger_temperature ≤ temp_threshold * 0.9`
- YELLOW: `avg_charger_temperature > temp_threshold * 0.9`
- RED: `avg_charger_temperature > temp_threshold`

### Signal 2: Session Buffer

`session_buffer = session_time_remaining - estimated_completion_minutes`

Measures whether the session is projected to complete within its scheduled window.

- GREEN: `avg_session_buffer ≥ session_buffer_threshold`
- YELLOW: `0 ≤ avg_session_buffer < session_buffer_threshold`
- RED: `avg_session_buffer < 0` (session projected to overrun)

### Tier Assignment

```
RED    if avg_session_buffer < 0 OR avg_charger_temperature > temp_threshold
YELLOW if avg_session_buffer < session_buffer_threshold OR avg_charger_temperature > temp_threshold * 0.9
GREEN  otherwise
```

Only YELLOW and RED trigger alerts.

---

## Session Lifecycle

Each charger cycles through a four-state session lifecycle continuously:

```
AVAILABLE → INITIALIZING → CHARGING → SESSION_COMPLETE → AVAILABLE
```

| State | Duration | Risk scoring |
|---|---|---|
| AVAILABLE | 30–90 seconds | No |
| INITIALIZING | 60–180 seconds | No |
| CHARGING | 5–15 minutes | Yes — only state eligible for risk classification |
| SESSION_COMPLETE | 1 tick | No |

Session context (connector type, energy requested, user tier, charging speed) is assigned at INITIALIZING and remains constant through SESSION_COMPLETE.

---

## Redis State Layer

Per-charger state is stored in Redis hashes keyed by `charger:{charger_id}`. Keys expire after 420 seconds of inactivity.

Supporting keys:
- `network:counts` — running RED and YELLOW counts, updated incrementally on each tier transition
- `network:last_update` — timestamp and charger ID of the most recent processed alert, used by the dashboard staleness check

On startup, the Redis consumer rebuilds `network:counts` by scanning all `charger:*` keys. This self-heals count drift caused by TTL expiry during downtime.

---

## Reliability and Fault Tolerance

### Event-Time Processing
Calculations are based on `event_ts` (when the event occurred), not when it arrives at Spark.

### Watermarking
Late-arriving events are tolerated up to 1 minute behind the watermark without corrupting stream state.

### Sliding Windows
5-minute windows with a 30-second slide interval provide continuous trend detection with overlapping coverage.

### Stateful Deduplication
The `write_on_transition` foreachBatch handler tracks the last-emitted tier per `(charger_id, session_id)` pair. Alerts only fire when the tier changes, preventing alert storms from sustained conditions.

### TTL-Based Eviction
Charger hashes in Redis expire after 420 seconds. The dashboard only shows chargers with active alerts, automatically clearing stale entries.

---

## Design Principles

- **Event-Driven First** — All major interactions occur through streaming events
- **Modular Components** — Each service can be developed and tested independently
- **Deterministic Risk Detection** — Risk classification is explainable and reproducible
- **Self-Describing Events** — Per-charger thresholds are embedded in every event; Spark needs no external joins
- **Observability by Default** — Progress logging, staleness checks, and diagnostic rate reporting
- **Schema-Forward Design** — Geographic coordinates and session metadata are present in the schema now, enabling future capabilities without pipeline changes
