Phase 11 — Real-Time Network Analytics

Objective

Derive operational statistics from the live telemetry stream and surface them on the dashboard.
No historical store is introduced — all stats reflect the current state of the stream within each
Spark micro-batch. The goal is to answer questions about regional load, connector performance,
and overall network health that the per-charger alert view cannot answer.

⸻

Questions Being Answered

Regional health:
  Which regions have the most active charging sessions right now?
  Which regions are running hottest on average?
  Are sessions in certain regions cutting closer to their buffer limit?

Connector performance:
  How efficiently are different connector types delivering power (actual vs rated)?
  What is the average energy delivered per session by connector type?

Network-wide:
  How many chargers are actively charging across the full network?
  What is the average temperature and session buffer fleet-wide?

⸻

Architecture

Data source: the `charging` DataFrame in stream_processor.py — CHARGING-filtered events
with `session_buffer` already derived. This is tapped before the windowed risk aggregation.

A second streaming query runs in parallel with the existing risk alert query:
  - Uses foreachBatch (no windowing — each micro-batch is a snapshot of recent CHARGING events)
  - Computes groupBy aggregations inside the batch function
  - Writes results directly to Redis

No new Kafka topics, no new infrastructure. Redis is the only sink.

⸻

Redis Key Design

stats:region:{region}     HASH
  active_sessions         int    — number of CHARGING events in this batch
  avg_temperature         float  — average charger temperature
  avg_session_progress    float  — average session progress (0.0–1.0)
  avg_session_buffer      float  — average session buffer (minutes)
  avg_power_kw            float  — average real-time power output
  last_update             string — ISO-8601 timestamp

stats:connector:{type}    HASH
  active_sessions         int
  avg_power_kw            float  — average actual power output
  avg_rated_power_kw      float  — average rated capacity
  avg_utilization_pct     float  — avg_power_kw / avg_rated_power_kw × 100
  avg_energy_kwh          float  — average cumulative energy delivered
  last_update             string

stats:network             HASH
  total_active_sessions   int
  avg_temperature         float
  avg_session_buffer      float
  avg_power_kw            float
  last_update             string

⸻

Sub-Phase Structure

Phase 11A — Spark Stats Sink

Add a second streaming query to spark_streaming/stream_processor.py:
- Taps the `charging` DataFrame (after CHARGING filter, before watermark)
- Uses foreachBatch with a `write_stats` function
- Inside write_stats: three groupBy aggregations (by region, by connector, network-wide)
- Writes to Redis stats:* keys on every batch
- Runs in parallel with the existing risk alert query
- Uses its own checkpoint: /tmp/logishield-checkpoints/charger-stats
- query.awaitTermination() replaced with spark.streams.awaitAnyTermination()

Phase 11B — Dashboard Statistics Section

Add a Statistics section to dashboard/app.py, inserted after System Health (Section 1)
and before Network Summary (Section 2):

  Network-wide row: total active sessions, avg temperature, avg session buffer, avg power
  Regional breakdown: st.dataframe with one row per region
  Connector breakdown: st.dataframe with one row per connector type

Data reads: r.hgetall("stats:network"), r.scan_iter("stats:region:*"),
            r.scan_iter("stats:connector:*")

If no stats keys exist yet (Spark not running), show a caption and skip the section.

Phase 11C — Limitations Documentation

Create docs/limitations.md acknowledging that alert patterns are simulator-driven.

⸻

Exit Criteria

Phase 11 is complete when:
- stats:region:*, stats:connector:*, stats:network keys are visible in Redis while the pipeline runs
- Dashboard Statistics section populates correctly from Redis
- Section degrades gracefully (caption only) when Spark is not running
- docs/limitations.md committed

⸻

Simulator Acknowledgement

The statistics derived in this phase reflect the simulator's scripted injection pattern
(GREEN → YELLOW → RED cycling every 15 steps), not organic operational variance.
Regional and connector distributions will be stable and deterministic rather than emergent.
This is documented in docs/limitations.md.
