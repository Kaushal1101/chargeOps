# Phase 4C — Packet Loss Simulation

## Objective

Validate that LogiShield remains stable and produces sensible stream outputs when some telemetry events are intentionally dropped before they reach Kafka.

This phase is about incomplete telemetry, not delayed telemetry or out-of-order replay. The goal is to understand how the pipeline behaves when a portion of the expected data never arrives at all.

---

## Scope

This phase focuses exclusively on:

* Controlled packet loss
* Stability under missing telemetry
* Effect of reduced input rate on Spark
* Observation of rolling metric behavior with incomplete data
* Documentation of degradation tolerance

This phase does **not** include:

* Fleet scaling
* Delayed burst replay
* Out-of-order events
* New business logic
* New risk classification logic
* AI remediation

---

## Architecture Being Tested

```text
Telemetry Simulator
      │
      ├── Some events dropped intentionally
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

The key assumption under test is that the pipeline remains healthy and interpretable even when the telemetry stream is partially missing.

---

## Chaos Injector Location

All Phase 4 chaos modes live in:

```text
chaos/
```

For this sub-phase, implement packet loss in:

```text
chaos/chaos_injector.py
```

### Responsibilities

* Reuse the existing telemetry event model from the simulator
* Reuse the existing Kafka producer setup where possible
* Randomly drop a configured percentage of generated events before they are sent to Kafka
* Keep the normal simulator unchanged

The chaos injector is a separate entry point. It should not modify `simulator/simulator.py` directly.

---

## Packet Loss Scenarios To Test

Use a gradual progression so the effect of missing data is easy to interpret.

### Scenario 1 — Light Loss

* Loss rate: **2–3%**
* Expected: mostly a sanity check; stream should look nearly normal

### Scenario 2 — Moderate Loss

* Loss rate: **5%**
* Expected: missing samples begin to show up, but the stream should still behave normally

### Scenario 3 — Heavy Loss

* Loss rate: **10–15%**
* Expected: meaningful degradation, but the stream should remain operational

---

## Deliverables

### Packet Loss Mode

Add a chaos mode that drops some events before publishing them to Kafka.

### Success Criteria

Kafka receives fewer events than the simulator generated, according to the configured loss rate.

---

### Stability Validation

Run the Spark pipeline against each packet-loss level and observe whether the stream remains healthy.

### Success Criteria

Spark continues running and processing data even when a portion of telemetry never arrives.

---

### Behavioral Notes

Document:

* the configured loss rate
* the observed input rate
* any visible change in batch duration
* whether the rolling metrics remain understandable
* whether the system appears stable under the loss scenario

### Success Criteria

You can explain how packet loss affected the stream without confusing it with watermark behavior.

---

## Important Design Rule

Packet loss should **not** automatically map to a RED truck state.

Packet loss is a telemetry reliability problem, not an operational delivery failure.

The goal is to test degradation tolerance in the pipeline, not to conflate missing data with cargo or SLA failure.

---

## Validation Checklist

* [ ] `chaos/chaos_injector.py` contains a packet-loss mode
* [ ] Events are randomly dropped before Kafka publication
* [ ] Loss rates can be configured and tested
* [ ] Spark continues processing successfully
* [ ] Input rate decreases roughly in line with loss rate
* [ ] The output remains interpretable
* [ ] Packet loss is not treated as RED by default
* [ ] The behavior is documented clearly

---

## Exit Criteria

Phase 4C is complete when:

* Controlled packet loss has been introduced into the telemetry stream
* Spark remains stable under multiple loss rates
* The effect on input rate and stream output has been observed
* The system behavior has been documented clearly enough to compare against the other resilience tests

---

## Phase 4C Outcome

At the end of Phase 4C, LogiShield will have validated that the pipeline can tolerate incomplete telemetry without collapsing or misclassifying normal operational states as delivery failures.

This creates the baseline for Phase 4D, where the fleet size is increased and throughput behavior is measured under load.
