# 🚚 LogiShield Pipeline

A real-time logistics risk detection platform designed to ingest fleet telemetry, identify emerging operational risks, and generate AI-powered remediation recommendations through a distributed event-driven architecture.

## 🏗️ Architecture & Tech Stack

This project uses a streaming-first architecture that separates telemetry generation, real-time analytics, and intelligent incident response into independent components:

- Telemetry Simulation (Python): Fleet simulators in the /simulator directory generate realistic truck telemetry streams including delivery timelines, cargo temperature readings, and operational anomalies.

- Message Broker (Apache Kafka): Kafka serves as the central event bus, decoupling telemetry producers from downstream processing services.

- Stream Processing (Apache Spark Structured Streaming): PySpark jobs continuously consume telemetry events, perform event-time aggregations, calculate rolling metrics, and classify operational risk levels.

- Risk Tiering Engine: A deterministic rules engine evaluates delivery buffer degradation and cargo health to assign Green, Yellow, or Red risk classifications.

- AI Remediation Agent: Alert consumers transform high-risk operational events into concise business-impact assessments and recommended mitigation actions using a configurable LLM integration.

- Fault Tolerance Testing: Chaos engineering utilities inject packet loss, network delays, and throughput spikes to validate pipeline resilience under adverse conditions.

## 🔄 System Flow

```text
Telemetry Simulator
        │
        ▼
Apache Kafka (fleet-telemetry)
        │
        ▼
Spark Structured Streaming
        │
        ▼
Risk Tiering Engine (Green / Yellow / Red)
        │
        ▼
Apache Kafka (risk-alerts)
        │
        ▼
AI Remediation Agent
        │
        ▼
Operational Briefs
```

## Prerequisites

- Docker Desktop — runs Kafka, Zookeeper, and supporting infrastructure
- Python 3.12
- Java 11 or higher — required by PySpark

## ⚡ Core Capabilities

### Real-Time Fleet Monitoring
Continuously processes streaming telemetry from simulated logistics fleets to detect delivery delays and cargo condition violations before SLA breaches occur.

### Event-Time Analytics
Uses Spark Structured Streaming with sliding windows and watermarking to maintain accurate calculations even when telemetry packets arrive late or out of order.

### Risk Classification
Evaluates operational health using rolling delivery buffers and cargo temperature trends to proactively identify at-risk shipments.

### AI-Powered Incident Response
Automatically translates technical anomaly signals into concise operational recommendations and business-impact summaries.

### Chaos Engineering Validation
Stress-tests the platform against network degradation, packet loss, and throughput spikes to verify reliability under real-world failure scenarios.

## 🛠️ Technology Stack

### Data Streaming
- Apache Kafka
- Apache Zookeeper

### Stream Processing
- Apache Spark Structured Streaming
- PySpark

### AI Layer
- Python
- Configurable LLM integration

### Infrastructure
- Docker Compose

### Data Format
- JSON Event Streams

## 📂 Repository Structure

```text
logishield-pipeline/

├── simulator/
│   └── Fleet telemetry generation
│
├── spark_streaming/
│   └── Stream processing and risk tiering
│
├── agent/
│   └── AI remediation workflows
│
├── chaos/
│   └── Fault injection and resilience testing
│
├── benchmarks/
│   └── Performance measurement utilities
│
├── configs/
│   └── Shared configuration
│
└── docs/
    └── Architecture and design documentation
```
