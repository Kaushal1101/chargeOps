import argparse
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Literal

from kafka import KafkaProducer
from kafka.errors import KafkaError

from simulator.models import TelemetryEvent

TOPIC = "fleet-telemetry"


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


class Vehicle:
    def __init__(
        self,
        vehicle_id: str,
        cargo_temp_threshold: float,
        sla_buffer_threshold: int,
    ) -> None:
        self.vehicle_id = vehicle_id
        self.cargo_temp_threshold = cargo_temp_threshold
        self.sla_buffer_threshold = sla_buffer_threshold
        self._step: int = 0

    def current_scenario(self) -> Scenario:
        return Scenario.from_step(self._step)

    def generate_event(self, event_ts: datetime | None = None) -> TelemetryEvent:
        scenario = self.current_scenario()
        values = scenario.generate_values(
            self.cargo_temp_threshold, self.sla_buffer_threshold
        )

        if event_ts is None:
            event_ts = datetime.now(timezone.utc)
        event_ts_str = event_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return TelemetryEvent(
            event_id=str(uuid.uuid4()),
            event_ts=event_ts_str,
            vehicle_id=self.vehicle_id,
            cargo_temperature=values["cargo_temperature"],
            time_left_to_destination=values["time_left_to_destination"],
            sla_time_remaining=values["sla_time_remaining"],
            scenario_state=scenario.state,
            sla_buffer_threshold=self.sla_buffer_threshold,
            cargo_temp_threshold=self.cargo_temp_threshold,
        )

    def advance(self) -> None:
        self._step += 1


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
        return cls(
            [
                Vehicle(
                    f"TRUCK_{i:04d}",
                    round(random.uniform(4.5, 6.0), 1),
                    random.randint(25, 40),
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
        )
    except KafkaError as e:
        print(f"Failed to connect to Kafka at localhost:9093: {e}")
        raise


def worker(shard: list[Vehicle], producer: KafkaProducer, stop: threading.Event) -> None:
    while not stop.is_set():
        start = time.time()
        for vehicle in shard:
            event = vehicle.generate_event()
            producer.send(
                TOPIC,
                key=vehicle.vehicle_id.encode(),
                value=event.to_json_bytes(),
            )
            print(
                f"[{vehicle.vehicle_id}] {vehicle.current_scenario().state} | event_ts={event.event_ts}"
            )
            vehicle.advance()
        elapsed = time.time() - start
        stop.wait(max(0.0, 1.0 - elapsed))


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
