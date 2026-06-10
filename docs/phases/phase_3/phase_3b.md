# Phase 3B — Schema Parsing & Structured DataFrames

## Objective

Convert raw Kafka telemetry messages into structured Spark DataFrames using the canonical event contract already defined in the project documentation.

By the end of this phase, LogiShield should be able to read telemetry from Kafka, parse the payload into typed columns, and display structured rows in Spark without performing any risk logic yet.

---

# Scope

This phase focuses exclusively on:

- Parsing raw Kafka message payloads
- Applying the canonical telemetry schema from event_schema.md
- Converting streaming JSON into structured Spark DataFrames
- Casting `event_ts` to `TimestampType` for downstream event-time processing
- Dropping the raw `value` column after parsing
- Preserving Kafka metadata for debugging and downstream processing
- Verifying that parsed rows display correctly in the console

This phase does not include:

- Derived metrics
- Risk tiering
- Windowing
- Watermarking
- Alert generation
- AI remediation

---

# Deliverables

## File to Update

Update the existing file:

```
spark_streaming/stream_processor.py
```

Do not create any new files.

The updated job should:

- Continue consuming from `fleet-telemetry`
- Parse the Kafka `value` field as JSON using `from_json()` with an explicit `StructType`
- Apply the canonical schema already defined in the project docs
- Cast `event_ts` from string to `TimestampType`
- Drop the raw `value` column after parsing
- Retain Kafka metadata: `topic`, `partition`, `offset`, `timestamp`
- Print parsed rows to the console sink for inspection

---

# Schema Alignment

Use the telemetry contract already defined in `event_schema.md` as the source of truth.

The `StructType` defined in code must match these fields exactly:

| Field | Spark Type |
|-------|-----------|
| event_id | StringType |
| event_ts | StringType (parsed from JSON, then cast to TimestampType) |
| vehicle_id | StringType |
| cargo_temperature | DoubleType |
| time_left_to_destination | IntegerType |
| sla_time_remaining | IntegerType |
| scenario_state | StringType |
| sla_buffer_threshold | IntegerType |
| cargo_temp_threshold | DoubleType |

Do not redefine field names, types, or meanings in code unless the project docs are updated first.

---

# Metadata Preservation

Retain these Kafka metadata columns alongside the parsed telemetry fields:

- `topic`
- `partition`
- `offset`
- `timestamp`

The raw `value` column (unparsed JSON string) must be dropped after parsing — it is redundant once structured columns exist.

---

# Technical Decisions

## `event_ts` Cast to TimestampType

`event_ts` arrives as an ISO-8601 string. It must be cast to `TimestampType` during parsing in this phase. This avoids rework in Phase 3C, where `event_ts` will be used as the event-time column for windowing and watermarking.

## Raw `value` Column Dropped After Parsing

Once `from_json()` has extracted structured columns, the original raw JSON string `value` column is redundant and adds noise. It is dropped as part of this phase.

## Minimal Surface Area

Keep this phase small. The goal is only to prove that Kafka telemetry can be converted into usable structured fields.

---

# How to Run

Same command as Phase 3A:

```bash
source venv/bin/activate
spark-submit \
  --master 'local[*]' \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
  spark_streaming/stream_processor.py
```

Prerequisites:
- Docker stack running (`docker-compose up -d`)
- Simulator running (`python -m simulator.simulator`)
- Spark UI available at `http://localhost:4040` while job is running

---

# Validation Checklist

- [ ] Spark still connects to Kafka successfully
- [ ] Kafka messages are read from `fleet-telemetry`
- [ ] Raw JSON payloads are parsed into structured columns
- [ ] `event_ts` appears as a timestamp column, not a string
- [ ] Raw `value` column is absent from output
- [ ] Kafka metadata columns (`topic`, `partition`, `offset`, `timestamp`) are present
- [ ] Parsed telemetry rows appear clearly in the console
- [ ] Event contract matches `event_schema.md`
- [ ] No parsing errors or null rows in output

---

# Exit Criteria

Phase 3B is complete when:

- The streaming job consumes telemetry from Kafka
- The payload is parsed using the canonical schema from the project docs
- `event_ts` is a `TimestampType` column
- The raw `value` column is not present in the output
- The output is a structured DataFrame rather than raw JSON text
- No parsing errors or schema mismatches occur

---

# Phase 3B Outcome

At the conclusion of this phase, LogiShield will have a structured streaming foundation inside Spark.

This sets up Phase 3C, where the project can begin deriving metrics and computing delivery buffer signals from the parsed telemetry data.
