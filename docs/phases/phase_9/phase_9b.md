Phase 9B — Spark Stream Processor

Objective

Update stream_processor.py to reflect the new EV charging schema produced by Phase 9A. All distributed systems logic — watermarking, windowed aggregation, foreachBatch deduplication, Kafka output — is structurally unchanged. This phase is exclusively field renames and topic/filter string updates.

⸻

Scope

This phase touches:
- spark_streaming/stream_processor.py — schema, field references, filter condition, risk reason strings, topic subscription

This phase does not touch:
- SparkSession configuration
- Watermark duration
- Window size or slide interval
- foreachBatch deduplication logic
- Checkpoint location
- Kafka output configuration

Clear the checkpoint before restarting Spark after this phase:
  rm -rf /tmp/logishield-checkpoints/risk-alerts

⸻

Schema Changes (TELEMETRY_SCHEMA)

Replace the existing StructType definition with the new EV field names.

Fields renamed:
  vehicle_id              → charger_id              StringType
  trip_state              → session_state            StringType
  trip_id                 → session_id               StringType
  cargo_temperature       → charger_temperature      DoubleType
  cargo_temp_threshold    → temp_threshold           DoubleType
  time_left_to_destination → estimated_completion_minutes  IntegerType
  sla_time_remaining      → session_time_remaining   IntegerType
  sla_buffer_threshold    → session_buffer_threshold IntegerType
  cargo_type              → connector_type           StringType
  cargo_value             → energy_requested_kwh     DoubleType
  customer_priority       → user_tier                StringType
  service_level           → charging_speed           StringType
  destination_region      → site_region              StringType
  route_progress          → session_progress         DoubleType
  estimated_arrival_minutes  (removed — merged into estimated_completion_minutes)
  remaining_stops         → power_output_kw          DoubleType
  driver_hours_remaining  → energy_delivered_kwh     DoubleType

Fields retained unchanged:
  event_id               StringType
  event_ts               StringType
  scenario_state         StringType

New fields added:
  charger_lat            DoubleType
  charger_lng            DoubleType
  rated_power_kw         DoubleType
  site_id                StringType

⸻

Structured Select Changes

Update the parsed.select() call to reference the new field names.
Every col("data.old_name") becomes col("data.new_name").

vehicle_id              → charger_id
cargo_temperature       → charger_temperature
time_left_to_destination → estimated_completion_minutes
sla_time_remaining      → session_time_remaining
sla_buffer_threshold    → session_buffer_threshold
trip_state              → session_state
trip_id                 → session_id
cargo_type              → connector_type
cargo_value             → energy_requested_kwh
customer_priority       → user_tier
service_level           → charging_speed
destination_region      → site_region
route_progress          → session_progress
estimated_arrival_minutes → (removed)
remaining_stops         → power_output_kw
driver_hours_remaining  → energy_delivered_kwh

Add the four new fields:
  col("data.charger_lat")
  col("data.charger_lng")
  col("data.rated_power_kw")
  col("data.site_id")

⸻

Derived Column Change

Rename the computed column:

  delivery_buffer = sla_time_remaining - time_left_to_destination
  becomes:
  session_buffer = session_time_remaining - estimated_completion_minutes

Formula is identical. Only the column name and input field names change.

⸻

Filter Change

  in_transit = with_metrics.filter(col("trip_state") == "IN_TRANSIT")
  becomes:
  charging = with_metrics.filter(col("session_state") == "CHARGING")

⸻

Windowed Aggregation Changes

groupBy key:
  col("vehicle_id")  →  col("charger_id")

Aggregation column renames:
  avg("delivery_buffer")        → avg("session_buffer"),       alias "avg_session_buffer"
  avg("cargo_temperature")      → avg("charger_temperature"),  alias "avg_charger_temperature"
  first("sla_buffer_threshold") → first("session_buffer_threshold"), alias "session_buffer_threshold"
  first("cargo_temp_threshold") → first("temp_threshold"),     alias "temp_threshold"
  first("trip_id")              → first("session_id"),         alias "session_id"
  first("cargo_type")           → first("connector_type"),     alias "connector_type"
  first("cargo_value")          → first("energy_requested_kwh"), alias "energy_requested_kwh"
  first("customer_priority")    → first("user_tier"),          alias "user_tier"
  first("service_level")        → first("charging_speed"),     alias "charging_speed"
  first("destination_region")   → first("site_region"),        alias "site_region"
  max("route_progress")         → max("session_progress"),     alias "max_session_progress"
  min("remaining_stops")        → (removed — power_output_kw is not min-aggregated)
  min("driver_hours_remaining") → (removed — energy_delivered_kwh is not min-aggregated)
  min("estimated_arrival_minutes") → (removed — merged above)

New aggregations:
  max("power_output_kw")        alias "max_power_output_kw"
  max("energy_delivered_kwh")   alias "max_energy_delivered_kwh"
  min("estimated_completion_minutes") alias "min_estimated_completion_minutes"
  first("charger_lat")          alias "charger_lat"
  first("charger_lng")          alias "charger_lng"
  first("rated_power_kw")       alias "rated_power_kw"
  first("site_id")              alias "site_id"

⸻

Risk Tiering Changes

Rename column references in the withColumn("risk_tier", ...) expression:

  avg_delivery_buffer    → avg_session_buffer
  avg_cargo_temperature  → avg_charger_temperature
  cargo_temp_threshold   → temp_threshold
  sla_buffer_threshold   → session_buffer_threshold

The tier logic is structurally unchanged:
  RED    if avg_session_buffer < 0 OR avg_charger_temperature > temp_threshold
  YELLOW if avg_session_buffer < session_buffer_threshold OR avg_charger_temperature > temp_threshold * 0.9
  GREEN  otherwise

⸻

Alert Records Select Changes

Rename all column references in the alert_records select:

  vehicle_id             → charger_id
  avg_delivery_buffer    → avg_session_buffer,      alias "session_buffer"
  avg_cargo_temperature  → avg_charger_temperature, alias "avg_temperature"
  lit("IN_TRANSIT")      → lit("CHARGING"),         alias "session_state"
  trip_id                → session_id
  cargo_type             → connector_type
  cargo_value            → energy_requested_kwh
  customer_priority      → user_tier
  service_level          → charging_speed
  destination_region     → site_region
  max_route_progress     → max_session_progress,    alias "session_progress"
  min_remaining_stops    → (removed)
  min_driver_hours_remaining → (removed)
  min_estimated_arrival_minutes → min_estimated_completion_minutes, alias "estimated_completion_minutes"

Add new fields to the select:
  col("max_power_output_kw").alias("power_output_kw")
  col("max_energy_delivered_kwh").alias("energy_delivered_kwh")
  col("charger_lat")
  col("charger_lng")
  col("rated_power_kw")
  col("site_id")

⸻

Alert Reason Strings

Update the reason column concat expressions to use EV language:

RED reasons:
  "Delivery buffer breached SLA: Xmin"
  → "Session projected to overrun by Xmin"

  "Cargo temperature exceeded threshold: X°C"
  → "Charger temperature exceeded threshold: X°C"

YELLOW reasons:
  "Delivery buffer below threshold: Xmin"
  → "Session buffer below threshold: Xmin"

  "Cargo temperature approaching threshold: X°C"
  → "Charger temperature approaching threshold: X°C"

⸻

Deduplication Key Change

The last_tiers dict uses (vehicle_id, trip_id) as the key.
Update to:
  key = (charger_id, session_id)

The deduplication logic (only emit on tier transition) is unchanged.

⸻

Topic Subscription Change

  .option("subscribe", "fleet-telemetry")
  becomes:
  .option("subscribe", "charger-telemetry")

⸻

Validation Checklist

- Spark parses charger-telemetry events without schema errors
- CHARGING filter correctly excludes AVAILABLE, INITIALIZING, SESSION_COMPLETE events
- Windowed aggregation groups by charger_id
- session_buffer is computed correctly (session_time_remaining - estimated_completion_minutes)
- risk_tier is assigned correctly for RED, YELLOW, and GREEN
- Only YELLOW and RED alerts reach risk-alerts
- alert_records contains charger_id, session_id, connector_type, user_tier, site_id, charger_lat, charger_lng
- Reason strings use EV language
- Transition deduplication fires correctly on tier change
- Watermark and window parameters are unchanged

⸻

Exit Criteria

Phase 9B is complete when:
- Spark consumes charger-telemetry and produces risk-alerts with the new EV schema
- Session-scoped risk classification works correctly for CHARGING events
- The full alert payload — including charger location, session context, and health metrics — flows into risk-alerts
- The pipeline is ready for Redis and dashboard updates in Phase 9C
