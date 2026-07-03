import argparse
import heapq
import random
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from kafka import KafkaProducer
from kafka.errors import KafkaError

from simulator.models import TelemetryEvent

TOPIC = "fleet-telemetry"

_CARGO_TYPES = [
    "Pharmaceuticals",
    "Fresh Food",
    "Frozen Goods",
    "Electronics",
    "General Freight",
]
_CUSTOMER_PRIORITIES = ["Standard", "Priority", "Critical"]
_SERVICE_LEVELS = ["Standard", "Express", "Same-Day"]
_DESTINATION_REGIONS = ["North", "South", "East", "West"]


class Scenario:
    def __init__(self, state: Literal["GREEN", "YELLOW", "RED"]) -> None:
        self.state = state

    @classmethod
    def from_step(cls, step: int) -> "Scenario":
        phase = step % 15
        if phase < 5:
            return cls("GREEN")
        if phase < 10:
            return cls("YELLOW")
        return cls("RED")

    def generate_values(self, temp_threshold: float, sla_threshold: int) -> dict:
        if self.state == "GREEN":
            cargo_temperature = random.uniform(temp_threshold - 3.0, temp_threshold - 1.0)
            time_left_to_destination = random.randint(10, 30)
            sla_time_remaining = time_left_to_destination + sla_threshold + random.randint(10, 30)
        elif self.state == "YELLOW":
            cargo_temperature = random.uniform(temp_threshold * 0.9, temp_threshold * 0.95)
            time_left_to_destination = random.randint(10, 30)
            sla_time_remaining = time_left_to_destination + random.randint(0, sla_threshold)
        else:  # RED
            cargo_temperature = random.uniform(temp_threshold + 0.5, temp_threshold + 3.0)
            time_left_to_destination = random.randint(10, 30)
            sla_time_remaining = time_left_to_destination - random.randint(1, 20)

        return {
            "cargo_temperature": round(cargo_temperature, 2),
            "time_left_to_destination": time_left_to_destination,
            "sla_time_remaining": sla_time_remaining,
        }


@dataclass
class TripContext:
    trip_id: str
    cargo_type: str
    cargo_value: float
    customer_priority: str
    service_level: str
    destination_region: str
    remaining_stops_initial: int
    shift_hours: float
    trip_start_time: float

    @classmethod
    def generate(cls) -> "TripContext":
        return cls(
            trip_id=str(uuid.uuid4()),
            cargo_type=random.choice(_CARGO_TYPES),
            cargo_value=round(random.uniform(5000.0, 500000.0), 2),
            customer_priority=random.choice(_CUSTOMER_PRIORITIES),
            service_level=random.choice(_SERVICE_LEVELS),
            destination_region=random.choice(_DESTINATION_REGIONS),
            remaining_stops_initial=max(1, random.randint(1, 5)),
            shift_hours=round(random.uniform(8.0, 11.0), 4),
            trip_start_time=0.0,
        )


class Vehicle:
    def __init__(
        self,
        vehicle_id: str,
        cargo_temp_threshold: float,
        sla_buffer_threshold: int,
        interval_seconds: float = 1.0,
    ) -> None:
        self.vehicle_id = vehicle_id
        self.cargo_temp_threshold = cargo_temp_threshold
        self.sla_buffer_threshold = sla_buffer_threshold
        self.interval_seconds = interval_seconds
        self._step: int = 0
        self.trip_state: str = "IDLE"
        self._state_deadline: float = time.time() + random.uniform(30, 90)
        self._trip_context: TripContext | None = None

    def _maybe_advance_lifecycle(self) -> None:
        now = time.time()
        if now < self._state_deadline:
            return
        if self.trip_state == "IDLE":
            self.trip_state = "LOADING"
            self._trip_context = TripContext.generate()
            self._state_deadline = now + random.uniform(60, 180)
        elif self.trip_state == "LOADING":
            self.trip_state = "IN_TRANSIT"
            self._trip_context.trip_start_time = now
            self._state_deadline = now + random.uniform(300, 900)
        elif self.trip_state == "IN_TRANSIT":
            self.trip_state = "DELIVERY_COMPLETE"
            self._state_deadline = now
        elif self.trip_state == "DELIVERY_COMPLETE":
            self.trip_state = "IDLE"
            self._trip_context = None
            self._state_deadline = now + random.uniform(30, 90)

    def _compute_dynamic_fields(self) -> dict:
        if self.trip_state != "IN_TRANSIT" or self._trip_context is None:
            return {
                "route_progress": 0.0,
                "estimated_arrival_minutes": 0,
                "remaining_stops": 0,
                "driver_hours_remaining": 0.0,
            }

        ctx = self._trip_context
        elapsed = time.time() - ctx.trip_start_time
        trip_duration = self._state_deadline - ctx.trip_start_time
        trip_duration = max(trip_duration, 1.0)

        route_progress = min(elapsed / trip_duration, 1.0)

        trip_duration_minutes = trip_duration / 60.0
        estimated_arrival_minutes = max(0, round((1.0 - route_progress) * trip_duration_minutes))

        threshold_interval = 1.0 / ctx.remaining_stops_initial
        stops_completed = int(route_progress / threshold_interval)
        remaining_stops = max(0, ctx.remaining_stops_initial - stops_completed)

        driver_hours_remaining = max(0.0, ctx.shift_hours - (elapsed / 3600.0))

        return {
            "route_progress": round(route_progress, 4),
            "estimated_arrival_minutes": estimated_arrival_minutes,
            "remaining_stops": remaining_stops,
            "driver_hours_remaining": round(driver_hours_remaining, 4),
        }

    def current_scenario(self) -> Scenario:
        return Scenario.from_step(self._step)

    def generate_event(self, event_ts: datetime | None = None) -> TelemetryEvent:
        self._maybe_advance_lifecycle()
        dynamic = self._compute_dynamic_fields()

        if self.trip_state == "IN_TRANSIT":
            scenario = self.current_scenario()
            values = scenario.generate_values(
                self.cargo_temp_threshold, self.sla_buffer_threshold
            )
        else:
            scenario = None
            values = {
                "cargo_temperature": 0.0,
                "time_left_to_destination": 0,
                "sla_time_remaining": 0,
            }

        if event_ts is None:
            event_ts = datetime.now(timezone.utc)
        event_ts_str = event_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        ctx = self._trip_context
        return TelemetryEvent(
            event_id=str(uuid.uuid4()),
            event_ts=event_ts_str,
            vehicle_id=self.vehicle_id,
            cargo_temperature=values["cargo_temperature"],
            time_left_to_destination=values["time_left_to_destination"],
            sla_time_remaining=values["sla_time_remaining"],
            scenario_state=scenario.state if self.trip_state == "IN_TRANSIT" else "GREEN",
            sla_buffer_threshold=self.sla_buffer_threshold,
            cargo_temp_threshold=self.cargo_temp_threshold,
            trip_state=self.trip_state,
            trip_id=ctx.trip_id if ctx else "",
            cargo_type=ctx.cargo_type if ctx else "",
            cargo_value=ctx.cargo_value if ctx else 0.0,
            customer_priority=ctx.customer_priority if ctx else "",
            service_level=ctx.service_level if ctx else "",
            destination_region=ctx.destination_region if ctx else "",
            route_progress=dynamic["route_progress"],
            estimated_arrival_minutes=dynamic["estimated_arrival_minutes"],
            remaining_stops=dynamic["remaining_stops"],
            driver_hours_remaining=dynamic["driver_hours_remaining"],
        )

    def advance(self) -> None:
        self._step += 1

    def __lt__(self, other: "Vehicle") -> bool:
        return self.vehicle_id < other.vehicle_id


class Fleet:
    def __init__(self, vehicles: list[Vehicle]) -> None:
        self.vehicles = vehicles

    @classmethod
    def default(cls) -> "Fleet":
        return cls(
            [
                Vehicle("TRUCK_101", 5.0, 30),
                Vehicle("TRUCK_102", 6.0, 25),
                Vehicle("TRUCK_103", 4.5, 35),
            ]
        )

    @classmethod
    def scaled(cls, n: int) -> "Fleet":
        def _cadence() -> float:
            r = random.random()
            if r < 0.20:
                return 0.5
            if r < 0.80:
                return 1.0
            return 2.0

        return cls(
            [
                Vehicle(
                    f"TRUCK_{i:04d}",
                    round(random.uniform(4.5, 6.0), 1),
                    random.randint(25, 40),
                    interval_seconds=_cadence(),
                )
                for i in range(1, n + 1)
            ]
        )

    def __iter__(self):
        return iter(self.vehicles)


def create_producer() -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers="localhost:9093",
            value_serializer=lambda v: v,
            retries=3,
            linger_ms=10,
            batch_size=65536,
            buffer_memory=67108864,
            compression_type="lz4",
        )
    except KafkaError as e:
        print(f"Failed to connect to Kafka at localhost:9093: {e}")
        raise


# === DIAGNOSTIC: event-generation rate counter (temporary; remove this block to disable) ===
_event_count = 0
_event_count_lock = threading.Lock()


def _record_event() -> None:
    global _event_count
    with _event_count_lock:
        _event_count += 1


def _rate_reporter(stop: threading.Event, interval: float = 5.0) -> None:
    global _event_count
    while not stop.wait(interval):
        with _event_count_lock:
            count = _event_count
            _event_count = 0
        print(
            f"[DIAGNOSTIC] generated {count} events in {interval:.1f}s "
            f"({count / interval:.1f} ev/s)"
        )
# === END DIAGNOSTIC ===


def worker(shard: list[Vehicle], producer: KafkaProducer, stop: threading.Event) -> None:
    now = time.time()
    heap = [(now + vehicle.interval_seconds * i / len(shard), vehicle) for i, vehicle in enumerate(shard)]
    heapq.heapify(heap)

    while not stop.is_set():
        next_time, vehicle = heapq.heappop(heap)
        wait = next_time - time.time()
        if wait > 0:
            stop.wait(wait)
        if stop.is_set():
            break
        event = vehicle.generate_event()
        producer.send(
            TOPIC,
            key=vehicle.vehicle_id.encode(),
            value=event.to_json_bytes(),
        )
        print(
            f"[{vehicle.vehicle_id}] {vehicle.trip_state} | "
            f"{vehicle.current_scenario().state} | interval={vehicle.interval_seconds}s | "
            f"event_ts={event.event_ts}"
        )
        _record_event()  # DIAGNOSTIC
        vehicle.advance()
        heapq.heappush(heap, (time.time() + vehicle.interval_seconds * random.uniform(0.7, 1.3), vehicle))


def run(fleet_size: int) -> None:
    num_threads = 4
    fleet = Fleet.scaled(fleet_size)
    producer = create_producer()
    stop = threading.Event()

    vehicles = list(fleet)
    shards = [vehicles[i::num_threads] for i in range(num_threads)]

    threads = [
        threading.Thread(target=worker, args=(shard, producer, stop), daemon=True)
        for shard in shards
        if shard
    ]

    # DIAGNOSTIC: start event-rate reporter
    threading.Thread(target=_rate_reporter, args=(stop,), daemon=True).start()

    for t in threads:
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("Shutting down simulator...")
        stop.set()
        for t in threads:
            t.join()
        producer.flush()
        producer.close()
        print("Simulator stopped.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fleet-size",
        type=int,
        default=3,
        help="Number of vehicles in fleet (default: 3)",
    )
    args = parser.parse_args()
    run(args.fleet_size)


if __name__ == "__main__":
    main()
