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

**Burst size:** 90 events per run — 30 steps × 3 trucks. This represents approximately 30 seconds of telemetry from all three trucks, which is enough for Spark to see meaningful window data from the burst.

---

# Pre-Run Requirement: Stop the Normal Simulator

**The normal simulator must NOT be running during Phase 4B tests.**

If the normal simulator is running, it will continuously publish fresh events to `fleet-telemetry`. Spark's watermark will keep advancing with those fresh events, making the backdated burst events look progressively older relative to the watermark. This contaminates the test.

For a clean watermark boundary test:
1. Ensure the simulator is stopped
2. Run the Spark job alone (its watermark will stall without incoming events)
3. Then fire the delayed burst — Spark's watermark reflects only the burst timestamps

---

# Delay Scenarios

All four scenarios use the same 90-event burst. Only `--delay` changes.

| Scenario | `--delay` | Expected Behavior |
|----------|-----------|-------------------|
| Small | 120s (2 min) | All events accepted — well inside 5-min watermark |
| Near-boundary | 240s (4 min) | Most events accepted — close enough to observe edge behavior |
| Over-boundary | 420s (7 min) | Events begin falling outside watermark, some dropped |
| Extreme | 1200s (20 min) | Most events rejected, stream remains healthy |

---

# How to Run

**Step 1 — Ensure simulator is stopped.** No `python -m simulator.simulator` should be running.

**Step 2 — Clear checkpoint and start Spark:**
```bash
rm -rf /tmp/logishield-checkpoints/risk-alerts
source venv/bin/activate
spark-submit \
  --master 'local[*]' \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
  spark_streaming/stream_processor.py
```

**Step 3 — Run each scenario in a separate terminal. Clear the checkpoint and restart Spark between scenarios.**

```bash
source venv/bin/activate

# Small delay (2 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 120

# Near-boundary (4 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 240

# Over-boundary (7 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 420

# Extreme (20 min)
python -m chaos.chaos_injector --mode delayed_burst --delay 1200
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
- Watch the watermark timestamp — with no normal simulator running, it will stall
- When the burst arrives, watch whether the watermark advances or the events are dropped
- Input rate will spike during the burst then drop to zero after it completes

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
- [ ] Normal simulator is stopped before each run
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
