Phase 5A — Threaded Load Model Refactor

Objective

Refactor the simulator so LogiShield can generate telemetry with a realistic concurrent load model instead of a single sequential loop.

The goal of this phase is to increase simulator throughput and make the traffic pattern less artificial by allowing different subsets of the fleet to emit in parallel.

This phase is not about benchmarking Spark yet.
It is about making the simulator capable of producing a heavier, more realistic workload so later Spark measurements are meaningful.

⸻

Scope

This phase focuses exclusively on:

* Threaded telemetry generation
* Fleet sharding across worker threads
* Per-vehicle cadence control
* Preserving the canonical telemetry schema
* Maintaining compatibility with the existing Kafka topic and Spark pipeline

This phase does not include:

* Packet loss
* Out-of-order timestamp backdating
* Delayed burst replay
* Malformed input
* New business logic
* New Spark features
* Final benchmarking reports

⸻

Architecture Being Tested

Fleet
  │
  ├── Shard 1 → Worker Thread 1
  ├── Shard 2 → Worker Thread 2
  ├── Shard 3 → Worker Thread 3
  └── Shard N → Worker Thread N
           │
           ▼
        Kafka Producer
           │
           ▼
   Apache Kafka (fleet-telemetry)

Key assumptions being tested:

* Kafka publishing is I/O-bound enough that threads can improve throughput.
* Vehicle state remains thread-confined to avoid race conditions.
* The simulator can scale beyond the current sequential Python loop.
* The output still matches the canonical event contract.

⸻

Design Decision

Use threading rather than multiprocessing or asyncio for the first load-model refactor.

Why threading

* Simpler to implement and explain
* Good fit for Kafka send latency and other I/O waits
* Easier to integrate with the current simulator structure
* Enough to push past the existing sequential bottleneck for this project

Why not multiprocessing yet

* More coordination overhead
* Harder to manage shared simulation state
* More complex debugging for a first pass

Why not asyncio yet

* The simulator still does non-trivial event construction work
* Threading is a more natural fit for the current codebase and mental model

⸻

Refactor Targets

Vehicle Ownership

Each vehicle should own its own state and event generation logic.

Expected responsibilities:

* vehicle identity
* thresholds
* current scenario state
* current step counter
* event generation
* step advancement

Fleet Ownership

The fleet should manage a collection of vehicles and support sharding them across workers.

Expected responsibilities:

* create the fleet
* hold the vehicle collection
* split vehicles into shards
* support configurable fleet sizes

Worker Threads

Worker threads should own their assigned shard of vehicles and repeatedly publish their telemetry.

Expected responsibilities:

* iterate only over assigned vehicles
* generate events from thread-owned vehicles
* send events to Kafka
* keep vehicle state local to the worker thread

Simulator Orchestrator

The main simulator entry point should coordinate startup and shutdown, but not own the low-level per-vehicle publish logic.

Expected responsibilities:

* construct the fleet
* create the Kafka producer(s)
* start worker threads
* handle shutdown cleanly
* preserve the current topic and schema behavior

⸻

Execution Model

Use 4 worker threads and shard the fleet evenly across them.

Do not create one thread per vehicle.

* 4 worker threads — enough parallelism to push past the sequential bottleneck without over-engineering
* Each thread owns a deterministic, non-overlapping subset of vehicles
* Vehicle state is thread-confined — no locks or shared mutable state needed
* One shared KafkaProducer across all threads — kafka-python-ng's KafkaProducer is thread-safe and uses an internal background I/O thread. Sharing one producer gives better message batching and fewer broker connections than one producer per thread.
* threading.Event stop flag — the main thread sets it on KeyboardInterrupt, each worker checks it at the top of its loop and exits cleanly. Without this, Ctrl+C kills the main thread but leaves workers hanging.
* Per-shard cadence — each worker loops over its N/4 vehicles and sleeps the remainder of the second using max(0.0, 1.0 - elapsed). True per-vehicle independent cadence requires a priority queue scheduler and is deferred to Phase 5B.

⸻

Deliverables

Threaded Simulator Refactor

Update the simulator so it can publish telemetry concurrently using worker threads.

Success Criteria

The simulator can produce more events per second than the current single-loop implementation.

⸻

Fleet Sharding

Add a mechanism for splitting vehicles across worker threads.

Success Criteria

Each worker thread is responsible for only a subset of the fleet.

⸻

Preserved Event Contract

The threaded simulator must still emit telemetry that matches the canonical schema already defined in the project docs.

Success Criteria

Spark and Kafka continue to consume the output without schema changes.

⸻

Throughput Observation

Measure the approximate throughput improvement compared with the sequential simulator.

Success Criteria

The simulator ceiling increases enough that Spark becomes a more meaningful benchmark target.

⸻

Validation Checklist

* Threaded publishing mode exists
* Fleet is sharded across worker threads
* Vehicle state remains thread-confined
* Kafka output still conforms to the canonical schema
* Simulator throughput increases compared with the sequential version
* Spark still consumes telemetry successfully
* No obvious race conditions or duplicate state mutation appear

⸻

Exit Criteria

Phase 5A is complete when:

* The simulator uses worker threads instead of a single sequential publish loop
* Fleet vehicles are distributed across shards
* The generator can sustain higher throughput than before
* The output still matches the LogiShield telemetry contract
* The new structure is stable enough to support later load profiles

⸻

Phase 5A Outcome

At the end of Phase 5A, LogiShield will have a more realistic, higher-throughput telemetry generator.

This becomes the foundation for later Phase 5 sub-phases, where the simulator will introduce jitter, irregular traffic, and other load characteristics needed to benchmark Spark fairly.