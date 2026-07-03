Phase 5D — State Transition Alerting & Temperature Signal Simplification

Objective

Improve the quality of risk-alerts by removing noisy duplicate emissions and simplifying the temperature signal used for risk classification.

At this point, LogiShield already produces structured alerts, but two issues remain:

* The same truck can emit repeated alerts for the same state across multiple windows.
* max_cargo_temperature makes the alert stream overly sensitive to one-off spikes and effectively overwhelms the Yellow state.

Phase 5D addresses both issues by introducing state transition alerting and simplifying temperature logic to use average temperature only.

⸻

Scope

This phase focuses exclusively on:

* Replacing max temperature with average temperature in risk logic
* Simplifying the temperature signal used by Spark
* Emitting alerts only when a truck changes risk state
* Reducing duplicate risk-alerts messages
* Preserving the existing telemetry schema and streaming architecture

This phase does not include:

* Per-vehicle cadence work
* Packet loss profiles
* Delayed replay scenarios
* Malformed input handling
* AI remediation
* New topics or dashboards

⸻

Architecture Being Modified

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
      ├── Average Temperature Logic
      └── Risk Tiering with State Transitions
      │
      ▼
Apache Kafka (risk-alerts)

The key assumptions being tested are:

* Average temperature is sufficient to represent sustained thermal risk.
* Risk alerts should represent state changes, not repeated identical conditions.
* The downstream alert stream becomes more meaningful when duplicate emissions are suppressed.

⸻

Design Decisions

1. Remove Max Temperature from Risk Logic

Use average cargo temperature rather than maximum cargo temperature in the rolling window logic.

This keeps the alert stream focused on sustained thermal drift rather than one-off spikes.

The intent is to make temperature contribute to risk tiering in a stable, interpretable way.

2. Emit Alerts Only on State Change

Track each vehicle’s previous emitted tier and only publish an alert when the tier changes.

Examples:

* GREEN → YELLOW → emit alert
* YELLOW → RED → emit alert
* RED → RED → suppress alert
* GREEN → GREEN → suppress alert

This prevents risk-alerts from being flooded with repeated identical windows.

⸻

Deliverables

Average-Temperature Risk Logic

Update the Spark pipeline so risk classification uses average temperature rather than maximum temperature.

Success Criteria

Temperature remains part of the risk decision, but the classification is no longer dominated by a single worst-case window value.

⸻

Transition-Based Alert Emission

Add logic so alerts are emitted only when a vehicle changes state.

Success Criteria

Repeated windows with the same tier no longer produce repeated identical alerts.

⸻

Alert-State Memory

Introduce a lightweight memory of each vehicle’s last emitted risk state.

This can live inside the Spark streaming logic or a small supporting state structure, depending on implementation style.

Success Criteria

The pipeline knows whether the current tier is new or simply a repeat of the previous tier.

⸻

Risk-Alert Quality Validation

Compare the alert stream before and after the refactor.

Success Criteria

The alert stream becomes smaller, cleaner, and more meaningful.

⸻

Validation Checklist

* max_cargo_temperature is removed from risk classification logic
* Average cargo temperature is used instead
* Alerts only fire on tier changes
* Duplicate alerts are suppressed
* risk-alerts becomes less noisy
* Spark continues to process the stream successfully
* The resulting alert stream is easier to interpret

⸻

Exit Criteria

Phase 5D is complete when:

* Risk tiering no longer depends on maximum temperature
* Average temperature is the sole temperature signal used in classification
* Alerts are emitted only on state transitions
* Duplicate alerts are materially reduced
* The risk-alerts topic reflects meaningful changes rather than repeated unchanged windows

⸻

Phase 5D Outcome

At the end of Phase 5D, LogiShield will have a much cleaner alert stream.

This makes the downstream risk-alerts topic far more useful for any future dashboard, operations view, or remediation layer, because alerts will now represent actual changes in truck status rather than repeated copies of the same state.