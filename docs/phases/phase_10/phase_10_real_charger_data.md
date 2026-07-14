Phase 10 — Real Singapore Charger Data

Objective

Replace the procedurally generated charger network with a real Singapore EV charger inventory. Use official public datasets as the source of truth for static charger identity and location. Simulate all live operational state on top of the real static data. The streaming architecture, risk logic, Redis layer, and dashboard are unchanged.

The key principle: real statics, simulated dynamics.

⸻

Motivation

The current simulator generates chargers from a hardcoded pool of 10 Singapore sites. The charger IDs, coordinates, and site names are plausible but invented. Replacing them with real charger data makes the project feel like a genuine network operations platform rather than a demo. It also enables the map-based dashboard view (Phase 10D) since the coordinates now reflect actual charger distribution across Singapore.

⸻

What Changes vs What Stays

What changes:
- Charger inventory source: procedural generation → loaded from a local dataset snapshot
- Charger IDs, site names, coordinates, operator names: derived from real data
- simulator/simulator.py: Network.scaled() replaced with Network.from_dataset()
- A new data/ directory holds the charger snapshot (JSON or CSV)
- A new script to fetch and normalise the raw dataset into the internal schema

What does not change:
- Session lifecycle state machine (AVAILABLE → INITIALIZING → CHARGING → SESSION_COMPLETE)
- All dynamic telemetry fields (charger_temperature, session_progress, power_output_kw, etc.)
- Kafka, Spark, Redis, dashboard pipeline
- Risk tiering logic
- The TelemetryEvent schema (fields already exist)

⸻

Sub-Phase Structure

Phase 10A — Data Source Evaluation

Identify and test candidate Singapore government datasets for EV charger inventory.
Assess field completeness: charger ID or unique identifier, latitude, longitude, site name, charger type, connector type, operator, cable count.
Decision: proceed with the best available source, or fall back to a curated hand-built dataset of real locations.

Candidate sources to evaluate:
- data.gov.sg — Singapore government open data portal
- LTA DataMall — Land Transport Authority API (covers transport infrastructure)
- OneMap API — Singapore's official geocoding and mapping service
- EMA (Energy Market Authority) — may have EV infrastructure datasets
- SP Mobility / Greenlots / Shell Recharge — operator-published data where available

Acceptance criteria for a usable dataset:
- At least 50 charger records
- Latitude and longitude present and accurate for the majority of records
- Some form of site name or address resolvable to a site name
- Charger type or power level present for at least a subset of records

Fallback: if no public dataset meets the criteria, build a curated snapshot of ~50 real Singapore charger locations sourced from public maps and operator websites. Still geographically authentic, fully controllable.

Phase 10B — Data Normalisation

Write a one-time script (scripts/fetch_chargers.py or scripts/build_charger_snapshot.py) that:
1. Fetches or reads the raw dataset
2. Normalises each record into the internal charger schema
3. Derives missing fields from available data (e.g. region from lat/lng bounding boxes, connector type from charger type label)
4. Writes a clean snapshot to data/chargers.json

Internal charger schema (normalised):
  charger_id        — derived from source ID or generated as SG-{i:04d}
  site_name         — from dataset or derived from address
  site_id           — slugified site_name (e.g. "Orchard_Central")
  site_region       — North / South / East / West / Central, derived from coordinates
  latitude          — float, WGS84
  longitude         — float, WGS84
  charger_type      — AC / DC / HPC where available, otherwise inferred from power level
  connector_type    — CCS2 / CHAdeMO / Type2 / HPC where available
  rated_power_kw    — float where available, otherwise assigned by connector type range
  operator          — string where available

Region derivation from coordinates (Singapore bounding box):
  Central: lat 1.28–1.33, lng 103.81–103.87
  North:   lat > 1.38
  South:   lat < 1.28
  East:    lng > 103.87
  West:    lng < 103.81

Phase 10C — Simulator Integration

Update simulator/simulator.py to load from data/chargers.json instead of generating chargers procedurally.

Replace Network.scaled(n) with Network.from_dataset(path, n=None):
- Loads data/chargers.json
- If n is provided, takes a random sample of n chargers
- If n is None, loads the full dataset
- Assigns temp_threshold and session_buffer_threshold per-charger (randomly within sensible ranges, as before)
- Assigns interval_seconds via _cadence() as before

Update CLI: --network-size becomes optional. Default behaviour loads the full dataset.

Phase 10D — Map View

Add a map section to the Streamlit dashboard showing all active alerted chargers at their real coordinates.

Use st.pydeck_chart with a ScatterplotLayer:
- Green dots for YELLOW alerts (amber is hard to distinguish at small sizes)
- Red dots for RED alerts
- Tooltip showing charger_id, site_id, tier, reason

Data source: charger_lat and charger_lng are already in every Redis charger hash. No pipeline changes required.

Add as Section 2.5 between Network Summary and Active Chargers table, so operators see the geographic view before the tabular detail.

Phase 10E — Documentation

Update project_log.md with Phase 10 entries.
Update README to describe the real charger inventory.
Update docs/architecture.md if the data layer warrants a new component description.

⸻

Data Directory

data/chargers.json — local snapshot of normalised charger inventory. Committed to the repository so the simulator runs without any API calls or internet access. Regenerated by running scripts/build_charger_snapshot.py.

⸻

Exit Criteria

Phase 10 is complete when:
- The simulator loads real Singapore charger locations from data/chargers.json
- Charger IDs and site names reflect real network assets
- The dashboard map shows charger alert locations on a Singapore map
- The pipeline runs end-to-end with real charger data without any changes to Spark, Redis, or risk logic
- data/chargers.json is committed and the simulator works offline without any API dependency

⸻

Future Phases

Phase 11 — AI Remediation Agent
Consume risk-alerts and generate operational briefs using Claude. Deferred from Phase 10 to keep this phase focused on data authenticity.

Phase 12 — Best Charger Recommendation
Query Redis state to recommend the optimal charger given a user location, connector type, and urgency. Builds on the real charger inventory and geographic coordinates established in Phase 10.
