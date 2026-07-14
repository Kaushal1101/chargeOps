Phase 9 — EV Charging Network Operations: Domain Pivot

Objective

Migrate LogiShield from truck fleet monitoring to EV charging network operations while preserving the entire distributed systems architecture built across Phases 1–8.

The simulator, Kafka pipeline, Spark streaming engine, Redis state layer, and Streamlit dashboard all remain structurally unchanged. Only the domain vocabulary and telemetry schema change. The distributed systems work is the portfolio asset — this phase reframes the story around a more operationally compelling problem.

⸻

Motivation

The current "truck monitoring" narrative is functional but generic. EV charging network operations is a cleaner problem with a clearer operational purpose:

- Operators need to know which chargers are degraded, faulted, or overloaded in real time
- The risk classification model (GREEN / YELLOW / RED) maps naturally to charger health
- The session lifecycle (AVAILABLE → INITIALIZING → CHARGING → SESSION_COMPLETE) is a direct structural equivalent of the delivery lifecycle
- The schema supports future capabilities: geographic charger maps, best-charger recommendations, utilisation analytics

The architecture does not change. The pipeline becomes:

Charger Simulator (digital twin of Singapore charging network)
    ↓
Kafka (charger-telemetry)
    ↓
Spark Structured Streaming (session-scoped windowed risk classification)
    ↓
Redis (per-charger operational state)
    ↓
Streamlit Dashboard (network operations console)

⸻

What Changes vs What Stays

What changes:
- Telemetry schema (field names and values)
- Simulator domain objects (Vehicle → Charger, Fleet → Network, TripContext → SessionContext)
- Lifecycle state names (IN_TRANSIT → CHARGING, etc.)
- Kafka topic names (fleet-telemetry → charger-telemetry)
- Redis key prefixes (truck: → charger:, fleet: → network:)
- Risk signal interpretation (cargo temperature → charger temperature, delivery buffer → session buffer)
- Dashboard labels and column names

What does not change:
- Docker Compose infrastructure
- Kafka producer configuration (batching, lz4, retries)
- Heapq-based per-charger emission scheduler
- Threading model (4 worker threads, sharded)
- SparkSession setup (local[*], shuffle partitions)
- Watermarking (1 minute)
- Sliding window (5 minutes, 30-second slide)
- foreachBatch transition deduplication pattern
- Risk tier names (GREEN / YELLOW / RED)
- Redis TTL and hash pattern
- Dashboard auto-refresh loop

⸻

Sub-Phase Structure

Phase 9A — Simulator and Data Model

Migrate the simulator from truck telemetry to charger telemetry.
- Rename domain objects
- Introduce EV-specific schema fields
- Add Singapore coordinates and site metadata
- Update lifecycle state names

Phase 9B — Spark

Update the stream processor schema, field references, and risk logic.
- Rename all schema fields
- Update IN_TRANSIT filter to CHARGING
- Rename delivery_buffer → session_buffer
- Update alert reason strings
- Update topic subscription

Phase 9C — Redis Consumer and Dashboard

Update the Redis consumer field mappings and the dashboard display layer.
- Rename key prefixes and field names
- Update CHARGER_COLUMNS
- Update dashboard section headers

⸻

Singapore Charger Network Design

The simulator will model a network of EV chargers distributed across Singapore. Each charger is assigned a fixed location at construction time and emits telemetry continuously.

Charger ID format: SG-{SITE_CODE}-{NUMBER}
Example: SG-ORC-01, SG-CBD-03, SG-CHG-02

Site regions (maps to existing destination_region field pattern):
- North: Woodlands, Yishun, Sembawang
- South: HarbourFront, Telok Blangah
- East: Changi, Tampines, Pasir Ris
- West: Jurong East, Buona Vista, Clementi
- Central: Orchard, Bishan, Toa Payoh, CBD

Connector types (maps to cargo_type):
- CCS2 (DC fast, 50–150 kW)
- CHAdeMO (DC fast, 50–100 kW)
- Type2 (AC, 7–22 kW)
- HPC (DC ultra-fast, 150–350 kW)

User tiers (maps to customer_priority):
- Standard
- Priority
- Fleet

Charging speeds (maps to service_level):
- Standard (Type2 AC)
- Fast (DC 50–100 kW)
- Ultra-Fast (DC 150+ kW)

⸻

Risk Logic Mapping

The Spark risk classification logic is structurally unchanged. Only field names differ.

Signal 1: Charger Temperature (replaces cargo temperature)
- GREEN: operating below threshold
- YELLOW: approaching threshold (>90% of threshold value)
- RED: threshold exceeded

Signal 2: Session Buffer (replaces delivery buffer)
session_buffer = session_time_remaining - estimated_completion_minutes
- GREEN: session will complete comfortably within the window
- YELLOW: session buffer below threshold (session at risk of running over)
- RED: buffer negative (session already projected to overrun)

Risk tier logic (same structure as before):
RED if session_buffer < 0 OR charger_temperature > temp_threshold
YELLOW if session_buffer < session_buffer_threshold OR charger_temperature > temp_threshold * 0.9
GREEN otherwise

⸻

Future Capabilities Enabled by This Design

The schema introduced in Phase 9 is designed to support future phases without further data model changes:

Geographic map: charger_lat and charger_lng are present from Phase 9A. A Streamlit map component can be added to the dashboard later without touching the pipeline.

Best-charger query: site_id, connector_type, user_tier, session_progress, and power_output_kw provide the inputs needed for a simple recommendation engine. This is deferred — the schema supports it.

Utilisation analytics: session_energy_kwh and power_output_kw accumulate across sessions and can feed a utilisation report.

⸻

Exit Criteria

Phase 9 is complete when:
- The simulator emits EV charger telemetry with the full new schema
- Charger session lifecycle cycles correctly through all four states
- Spark processes charger-telemetry and produces risk-alerts scoped to CHARGING sessions
- Redis stores per-charger state with the new field names
- The dashboard displays a network operations view with charger health, session progress, and site context
- All existing distributed systems properties are preserved: watermarking, windowed aggregation, transition deduplication, TTL-based eviction
