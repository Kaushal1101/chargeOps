Phase 9A — Simulator and Data Model

Objective

Migrate the simulator from truck telemetry to EV charger telemetry. Replace all domain-specific vocabulary in the data model and simulator while keeping the threading model, scheduling logic, Kafka producer configuration, and state machine structure completely unchanged.

The output of this phase is a simulator that emits realistic EV charging session telemetry into Kafka on the charger-telemetry topic.

⸻

Scope

This phase touches:
- simulator/models.py — TelemetryEvent field names
- simulator/simulator.py — domain object names, lifecycle state names, schema values, charger network definition
- docker-compose.yml — kafka-init topic names

This phase does not touch:
- Kafka producer configuration
- Threading model or heapq scheduler
- Spark
- Redis consumer
- Dashboard

⸻

Data Model Changes (models.py)

Rename TelemetryEvent fields as follows. All types are preserved exactly.

Fields renamed directly:
  vehicle_id              → charger_id
  trip_state              → session_state
  trip_id                 → session_id
  cargo_temperature       → charger_temperature
  cargo_temp_threshold    → temp_threshold
  time_left_to_destination → estimated_completion_minutes
  sla_time_remaining      → session_time_remaining
  sla_buffer_threshold    → session_buffer_threshold
  cargo_type              → connector_type
  cargo_value             → energy_requested_kwh
  customer_priority       → user_tier
  service_level           → charging_speed
  destination_region      → site_region
  route_progress          → session_progress
  estimated_arrival_minutes → (removed — merged into estimated_completion_minutes above)
  remaining_stops         → power_output_kw
  driver_hours_remaining  → energy_delivered_kwh

Note: estimated_arrival_minutes and time_left_to_destination were redundant in the truck schema.
In the EV schema, estimated_completion_minutes is a single clean field that serves both roles.

Fields retained unchanged:
  event_id
  event_ts
  scenario_state

New fields added:
  charger_lat      float   — WGS84 latitude (Singapore range: ~1.25 to ~1.47)
  charger_lng      float   — WGS84 longitude (Singapore range: ~103.6 to ~104.0)
  rated_power_kw   float   — charger's nameplate capacity in kW
  site_id          str     — e.g. "Orchard_Central", "Changi_Airport"

These four fields are static per-charger (set at construction time, never change).

⸻

Simulator Domain Object Changes (simulator.py)

Rename the following classes. Internal logic is preserved.

  Vehicle      → Charger
  Fleet        → Network
  TripContext  → SessionContext

Lifecycle state names:
  IDLE              → AVAILABLE
  LOADING           → INITIALIZING
  IN_TRANSIT        → CHARGING
  DELIVERY_COMPLETE → SESSION_COMPLETE

The state machine structure, durations, and transitions are unchanged:
  AVAILABLE:       30–90 seconds
  INITIALIZING:    60–180 seconds (session handshake)
  CHARGING:        300–900 seconds (active session)
  SESSION_COMPLETE: 1 tick, then AVAILABLE

⸻

SessionContext Fields

Assign at the moment INITIALIZING begins. Immutable for the duration of the session.

  session_id          — UUID, new per session
  connector_type      — one of: CCS2, CHAdeMO, Type2, HPC
  energy_requested_kwh — float, 10.0–80.0 kWh
  user_tier           — one of: Standard, Priority, Fleet
  charging_speed      — one of: Standard, Fast, Ultra-Fast
  site_region         — one of: North, South, East, West, Central

During AVAILABLE, these fields emit as empty strings or 0.0 equivalents.

⸻

Dynamic Field Changes

session_progress (was route_progress)
  Advances 0.0 → 1.0 over the CHARGING duration. Unchanged logic.

estimated_completion_minutes (was both time_left_to_destination and estimated_arrival_minutes)
  Time remaining until session completes. Decrements as session_progress increases.
  Formula: round((1.0 - session_progress) * session_duration_minutes)

power_output_kw (was remaining_stops)
  Simulated real-time power delivery in kW.
  During CHARGING: rated_power_kw * random.uniform(0.7, 1.0) — slight variance per tick.
  During AVAILABLE / INITIALIZING / SESSION_COMPLETE: 0.0

energy_delivered_kwh (was driver_hours_remaining)
  Cumulative energy delivered this session.
  During CHARGING: session_progress * energy_requested_kwh
  During other states: 0.0

session_time_remaining (was sla_time_remaining)
  Simulated session window remaining in minutes. Decrements independently of session_progress.
  Assign at INITIALIZING start: session_duration_minutes + session_buffer_threshold + random jitter.
  Decrement each tick by interval_seconds / 60.

⸻

Scenario Value Ranges

The Scenario class generates GREEN / YELLOW / RED values for charger_temperature and session timing.
Update generate_values() for EV-appropriate ranges.

Charger temperature ranges by scenario:
  GREEN:  temp_threshold - 15 to temp_threshold - 5  (e.g. 35–45°C for a 50°C threshold)
  YELLOW: temp_threshold * 0.90 to temp_threshold * 0.95
  RED:    temp_threshold + 2 to temp_threshold + 10

Session timing (session_time_remaining vs estimated_completion_minutes):
  GREEN:  session completes well within window (large positive buffer)
  YELLOW: session buffer below threshold (tight but not over)
  RED:    session projected to overrun (negative buffer)

These ranges produce the same GREEN / YELLOW / RED distribution in Spark as before.

⸻

Charger Network Definition

Replace Fleet.default() and Fleet.scaled() with a Singapore charger network.

Network.default() should hardcode a small set of named chargers:
  SG-ORC-01   Orchard Central         lat=1.3048  lng=103.8318  rated=150.0 kW  CCS2
  SG-CBD-01   Raffles Place           lat=1.2830  lng=103.8513  rated=50.0 kW   CHAdeMO
  SG-CHG-01   Changi Airport T3       lat=1.3644  lng=103.9915  rated=350.0 kW  HPC

Network.scaled(n) should generate n chargers distributed across Singapore sites.
Use a predefined list of (site_id, site_region, lat, lng) tuples and cycle through them.

Suggested site pool:
  ("Orchard_Central",    "Central", 1.3048, 103.8318)
  ("Raffles_Place",      "Central", 1.2830, 103.8513)
  ("Bishan_MRT",         "Central", 1.3526, 103.8352)
  ("Changi_Airport",     "East",    1.3644, 103.9915)
  ("Tampines_Hub",       "East",    1.3496, 103.9568)
  ("Woodlands_Civic",    "North",   1.4382, 103.7890)
  ("Yishun_Mall",        "North",   1.4304, 103.8354)
  ("Jurong_East",        "West",    1.3329, 103.7436)
  ("Buona_Vista",        "West",    1.3067, 103.7904)
  ("HarbourFront",       "South",   1.2654, 103.8200)

Connector types cycle or are assigned randomly: CCS2, CHAdeMO, Type2, HPC
Rated power by connector type:
  Type2:   7.0–22.0 kW
  CCS2:    50.0–150.0 kW
  CHAdeMO: 50.0–100.0 kW
  HPC:     150.0–350.0 kW

⸻

Kafka Topic Change

Update the TOPIC constant:
  fleet-telemetry  →  charger-telemetry

Update docker-compose.yml kafka-init command:
  Replace both topic creation lines:
    fleet-telemetry  →  charger-telemetry
  risk-alerts remains unchanged.

⸻

Diagnostic Log Line

Update the worker() print statement:
  [TRUCK_101] IN_TRANSIT | RED | interval=1.0s | event_ts=...
  becomes:
  [SG-ORC-01] CHARGING | RED | interval=1.0s | event_ts=...

⸻

CLI Argument

  --fleet-size  →  --network-size

⸻

Validation Checklist

- Chargers cycle through all four session lifecycle states
- State durations match the specified ranges
- Each session receives a unique session_id at INITIALIZING
- SessionContext fields are consistent across all events within a session
- session_state appears on every event
- charger_lat, charger_lng, rated_power_kw, site_id are present on every event and never change per charger
- power_output_kw is non-zero only during CHARGING
- energy_delivered_kwh increases monotonically during CHARGING and resets at SESSION_COMPLETE
- Kafka topic is charger-telemetry
- Events are consumable from localhost:9093

⸻

Exit Criteria

Phase 9A is complete when:
- The simulator emits EV charger telemetry with the full new schema on charger-telemetry
- All charger session lifecycle states are present in the stream
- session_progress, power_output_kw, energy_delivered_kwh evolve correctly during CHARGING
- Singapore coordinates and site metadata are present on every event
- The pipeline is ready for Spark schema updates in Phase 9B
