# Event Schema Specification

This document defines the canonical event schemas used throughout the LogiShield Pipeline.

All services (Simulator, Spark Streaming, Redis Consumer, Dashboard) must adhere to these contracts to prevent schema drift.

> **Note:** Phases 1–8 used a truck delivery schema (`vehicle_id`, `cargo_temperature`, `delivery_buffer`, etc.). Phase 9 migrated to the EV charging schema defined here. The old schema is preserved in `docs/phases/phase_8/` for historical reference.

---

# 1. Telemetry Event

Kafka Topic: `charger-telemetry`

Emitted by the charger simulator on every tick. Represents a single state update from one EV charging station.

## Schema

### Identity and Timing

| Field | Type | Description |
|---|---|---|
| `event_id` | string (UUID v4) | Unique event identifier |
| `event_ts` | string (ISO-8601) | Timestamp when the event occurred — used for Spark event-time windowing |
| `charger_id` | string | Charger identifier, e.g. `SG-0001` |
| `scenario_state` | string | Simulator injection state: `GREEN`, `YELLOW`, or `RED` — distinct from the `risk_tier` computed by Spark |

### Session Lifecycle

| Field | Type | Description |
|---|---|---|
| `session_state` | string | `AVAILABLE`, `INITIALIZING`, `CHARGING`, or `SESSION_COMPLETE` |
| `session_id` | string (UUID v4) | Unique identifier for the current session. Empty string when AVAILABLE. |

### Static Charger Properties (set at construction, never change)

| Field | Type | Description |
|---|---|---|
| `charger_lat` | float | WGS84 latitude (Singapore range: ~1.25–1.47) |
| `charger_lng` | float | WGS84 longitude (Singapore range: ~103.6–104.0) |
| `rated_power_kw` | float | Charger's nameplate capacity in kW |
| `site_id` | string | Site name, e.g. `Changi_Airport` |
| `site_region` | string | `North`, `South`, `East`, `West`, or `Central` |
| `temp_threshold` | float | Maximum acceptable charger temperature in °C |
| `session_buffer_threshold` | integer | Minimum acceptable session buffer in minutes before YELLOW risk |

### Static Session Context (assigned at INITIALIZING, immutable for the session)

| Field | Type | Description |
|---|---|---|
| `connector_type` | string | `CCS2`, `CHAdeMO`, `Type2`, or `HPC` |
| `energy_requested_kwh` | float | Energy the vehicle needs this session (kWh) |
| `user_tier` | string | `Standard`, `Priority`, or `Corporate` |
| `charging_speed` | string | `Standard`, `Fast`, or `Ultra-Fast` |

### Dynamic Session Fields (evolve during CHARGING)

| Field | Type | Description |
|---|---|---|
| `session_progress` | float | 0.0 → 1.0 as session completes |
| `estimated_completion_minutes` | integer | Estimated minutes until session completes |
| `session_time_remaining` | integer | Minutes remaining in the scheduled session window |
| `power_output_kw` | float | Real-time power delivery in kW |
| `energy_delivered_kwh` | float | Cumulative energy delivered this session |
| `charger_temperature` | float | Current charger hardware temperature in °C |

All dynamic fields emit 0 or 0.0 when `session_state != "CHARGING"`.

**Note on `scenario_state`:** This reflects the scenario the simulator is actively injecting, not the risk tier independently calculated by Spark. In normal operation these align. During chaos testing they may diverge. `scenario_state` is the simulator's control variable; `risk_tier` is Spark's output.

**Note on self-describing events:** `temp_threshold` and `session_buffer_threshold` are per-charger values repeated on every event. This makes each event independently interpretable — Spark evaluates risk on a single row with no external joins or per-charger state stores.

---

## Example Payload (CHARGING)

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "event_ts": "2026-07-03T10:15:30.000000Z",
  "charger_id": "SG-0004",
  "scenario_state": "YELLOW",
  "session_state": "CHARGING",
  "session_id": "f9e8d7c6-b5a4-3210-fedc-ba9876543210",
  "charger_lat": 1.3644,
  "charger_lng": 103.9915,
  "rated_power_kw": 150.0,
  "site_id": "Changi_Airport",
  "site_region": "East",
  "temp_threshold": 52.0,
  "session_buffer_threshold": 8,
  "connector_type": "CCS2",
  "energy_requested_kwh": 54.3,
  "user_tier": "Priority",
  "charging_speed": "Fast",
  "session_progress": 0.42,
  "estimated_completion_minutes": 18,
  "session_time_remaining": 21,
  "power_output_kw": 112.4,
  "energy_delivered_kwh": 22.8,
  "charger_temperature": 47.6
}
```

---

# 2. Derived Streaming Metrics

Computed by Spark within the 5-minute sliding window. Not produced by the simulator.

| Field | Type | Description |
|---|---|---|
| `session_buffer` | integer | `session_time_remaining - estimated_completion_minutes`. Negative means session is projected to overrun. |
| `avg_session_buffer` | float | Rolling average session buffer over the window |
| `avg_charger_temperature` | float | Rolling average charger temperature over the window |
| `risk_tier` | string | `GREEN`, `YELLOW`, or `RED` — independently determined by Spark |

---

# 3. Risk Alert Event

Kafka Topic: `risk-alerts`

Produced by Spark when a charger enters YELLOW or RED status. Only emitted on tier transitions — a sustained RED does not produce repeated alerts.

## Schema

| Field | Type | Description |
|---|---|---|
| `event_id` | string (UUID v4) | Alert identifier |
| `window_start` | string | Start of the aggregation window |
| `window_end` | string | End of the aggregation window |
| `alert_ts` | string (ISO-8601) | Timestamp when the alert was emitted |
| `charger_id` | string | Charger identifier |
| `risk_tier` | string | `YELLOW` or `RED` |
| `session_buffer` | integer | Average session buffer over the window |
| `avg_temperature` | float | Average charger temperature over the window |
| `reason` | string | Human-readable trigger description |
| `session_state` | string | Always `CHARGING` (non-CHARGING events are filtered before aggregation) |
| `session_id` | string | Session identifier |
| `connector_type` | string | Connector type for this session |
| `energy_requested_kwh` | float | Session energy demand |
| `user_tier` | string | Customer tier |
| `charging_speed` | string | Charging speed tier |
| `site_region` | string | Geographic region |
| `charger_lat` | float | Charger latitude |
| `charger_lng` | float | Charger longitude |
| `rated_power_kw` | float | Charger's nameplate capacity |
| `site_id` | string | Site name |
| `session_progress` | float | Session progress at time of alert |
| `power_output_kw` | float | Peak power output in the window |
| `energy_delivered_kwh` | float | Energy delivered so far this session |
| `estimated_completion_minutes` | integer | Estimated minutes to session completion |

### Reason String Format

| Condition | Example |
|---|---|
| RED — session overrun | `"Session projected to overrun by 4min"` |
| RED — temperature | `"Charger temperature exceeded threshold: 53.2C"` |
| YELLOW — buffer | `"Session buffer below threshold: 3min"` |
| YELLOW — temperature | `"Charger temperature approaching threshold: 47.8C"` |

---

## Example Payload

```json
{
  "event_id": "c3d4e5f6-a7b8-9012-cdef-345678901234",
  "window_start": "2026-07-03 10:10:00",
  "window_end": "2026-07-03 10:15:00",
  "alert_ts": "2026-07-03T10:15:05.123456+00:00",
  "charger_id": "SG-0004",
  "risk_tier": "YELLOW",
  "session_buffer": 3,
  "avg_temperature": 47.8,
  "reason": "Session buffer below threshold: 3min",
  "session_state": "CHARGING",
  "session_id": "f9e8d7c6-b5a4-3210-fedc-ba9876543210",
  "connector_type": "CCS2",
  "energy_requested_kwh": 54.3,
  "user_tier": "Priority",
  "charging_speed": "Fast",
  "site_region": "East",
  "charger_lat": 1.3644,
  "charger_lng": 103.9915,
  "rated_power_kw": 150.0,
  "site_id": "Changi_Airport",
  "session_progress": 0.58,
  "power_output_kw": 118.2,
  "energy_delivered_kwh": 31.5,
  "estimated_completion_minutes": 13
}
```

---

# Event Lifecycle

```text
Telemetry Event (all session states)
          │
          ▼
  charger-telemetry
          │
          ▼
  Spark — filter: session_state == CHARGING
          │
          ▼
  Windowed aggregation (5min / 30s slide)
          │
          ▼
  Risk tiering (GREEN / YELLOW / RED)
          │
          ▼
  Transition filter (emit only on tier change)
          │
          ▼
      risk-alerts
          │
          ▼
  Redis per-charger state (charger:{charger_id})
```

---

# Design Rules

### Schema Stability
Fields defined here should not be renamed or removed without updating all dependent services and clearing the Spark checkpoint.

### Event-Time Processing
`event_ts` is the authoritative timestamp for Spark windowing and watermarking.

### Source of Truth
- Simulator owns telemetry events and per-charger thresholds
- Spark owns derived metrics and risk alerts
- Redis consumer owns operational state materialisation

No component should modify fields owned by another component.

### Self-Describing Events
`temp_threshold` and `session_buffer_threshold` are repeated on every event. Spark evaluates risk on a single row with no external state or stream joins.
