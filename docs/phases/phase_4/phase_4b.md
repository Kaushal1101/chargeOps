# Phase 4B — Delayed Burst Replay

## Objective

Validate how LogiShield behaves when telemetry is withheld for a period of time and then replayed in a burst.

This phase is not about out-of-order randomness. It is about testing the boundary between acceptable delay and watermark rejection so that Spark's event-time processing can be validated under a connectivity outage style failure mode.

---

# Scope

This phase focuses exclusively on:

- Delayed telemetry replay
- Watermark boundary validation
- Event-time correctness under burst delivery
- Observation of accepted vs dropped late events
- Documentation of Spark behavior under delay

This phase does not include:

- Fleet scaling
- Packet loss
- New business logic
- New risk classification logic
- AI remediation

---

# Architecture Being Tested

```
Telemetry Simulator
      │
      ▼
Apache Kafka (fleet-telemetry)
      │
      ▼
Spark Structured Streaming
      │
      ├── Event-Time Processing
      ├── Sliding Windows
      ├── Watermarking
      └── Risk Tiering
      │
      ▼
Apache Kafka (risk-alerts)
```

The key assumption under test is that Spark accepts delayed events only while they remain inside the configured watermark boundary.

---

# Chaos Injector Location

Add a `delayed_burst` mode to the existing file:

```
chaos/chaos_injector.py
```

Add `"delayed_burst"` to the `--mode` choices in `main()`. No other files need to be created or modified.

---

# Burst Mechanism: Timestamp Backdating

The delayed burst is implemented via **timestamp backdating** — a fixed batch of events is generated with `event_ts` set to `now - delay_seconds` for all events in the burst, and all events are sent to Kafka immediately without any actual waiting period.

This is the correct mechanism for testing Spark's watermark boundary. Spark assigns events to windows based solely on `event_ts` — a burst of events all stamped with the same past timestamp exercises the watermark exactly as a real connectivity outage would.

**How it differs from Phase 4A:**
- Phase 4A applied a *random* offset per event (disorder simulation)
- Phase 4B applies a *fixed* offset to all events in the burst (outage simulation)

**Burst size:** 30 events per run — 30 steps for the target vehicle. Only one vehicle is burst; the other two trucks continue sending live telemetry via the normal simulator, which keeps Spark's watermark advancing throughout the test.

---

# Delay Scenarios

All four scenarios use the same 30-event burst targeting TRUCK_101. Only `--delay` changes.

| Scenario | `--delay` | Expected Behavior |
|----------|-----------|-------------------|
| Small | 120s (2 min) | All events accepted — well inside 5-min watermark |
| Near-boundary | 240s (4 min) | Most events accepted — close enough to observe edge behavior |
| Over-boundary | 420s (7 min) | Events begin falling outside watermark, some dropped |
| Extreme | 1200s (20 min) | Most events rejected, stream remains healthy |

---

# How to Run

**Step 1 — Start the normal simulator** (Terminal 1):
```bash
source venv/bin/activate
python -m simulator.simulator
```

The normal simulator keeps publishing fresh events, which advances Spark's watermark throughout the test. TRUCK_101 also publishes normal telemetry while the burst events arrive in parallel via the chaos injector.

**Step 2 — Clear checkpoint and start Spark** (Terminal 2):
```bash
rm -rf /tmp/logishield-checkpoints/risk-alerts
source venv/bin/activate
spark-submit \
  --master 'local[*]' \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
  spark_streaming/stream_processor.py
```

**Step 3 — Run each burst scenario in a separate terminal. Clear the checkpoint and restart Spark between scenarios.**

```bash
source venv/bin/activate

# Small delay (2 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 120 --vehicle TRUCK_101

# Near-boundary (4 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 240 --vehicle TRUCK_101

# Over-boundary (7 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 420 --vehicle TRUCK_101

# Extreme (20 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 1200 --vehicle TRUCK_101
```

**Between each scenario:**
```bash
# Kill Spark, then clear checkpoint
rm -rf /tmp/logishield-checkpoints/risk-alerts
```

---

# Observation Method

Same as Phase 4A — no console sink. Observe via:

**Kafka UI** (`http://localhost:8080` → `risk-alerts`):
- Small/near-boundary delays should produce alerts
- Over-boundary and extreme delays should produce fewer or no alerts

**Spark UI** (`http://localhost:4040` → Streaming tab):
- The watermark advances continuously throughout the test, driven by TRUCK_102 and TRUCK_103's live events
- When the burst arrives, watch whether TRUCK_101's backdated events are accepted or dropped relative to the current watermark
- Input rate will spike briefly during the burst then return to baseline

---

# Expected vs Unexpected Behavior

| Scenario | Expected | Unexpected |
|----------|----------|------------|
| Small (2 min) | Alerts appear in `risk-alerts` | No alerts, Spark error |
| Near-boundary (4 min) | Alerts appear, possibly fewer | Spark crashes |
| Over-boundary (7 min) | Some events dropped, stream stable | Corrupted metrics |
| Extreme (20 min) | Most events dropped, stream healthy | Spark crashes or hangs |

---

# Validation Checklist

- [ ] `delayed_burst` mode added to `chaos/chaos_injector.py`
- [ ] Normal simulator is running during each burst scenario
- [ ] Small delay (120s): alerts appear in `risk-alerts`
- [ ] Near-boundary (240s): alerts appear, edge behavior observed
- [ ] Over-boundary (420s): watermark drops events, stream remains stable
- [ ] Extreme (1200s): most events dropped, Spark stays healthy
- [ ] Spark UI shows input rate spike during burst
- [ ] Behavior documented in project log

---

# Exit Criteria

Phase 4B is complete when:

- `delayed_burst` mode exists in the chaos injector
- All four delay scenarios have been run
- Watermark boundary behavior is clearly understood
- Accepted and rejected delayed events can be distinguished
- No crashes or stream failures occurred

---

# Phase 4B Outcome

At the end of Phase 4B, LogiShield will have validated how the pipeline behaves when telemetry is delayed and then replayed.

This confirms whether watermarking is correctly separating acceptable lateness from events that are too stale to trust, and establishes the pipeline's tolerance for real-world connectivity outage scenarios.
