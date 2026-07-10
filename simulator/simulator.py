import argparse
import heapq
import json
import random
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from kafka import KafkaProducer
from kafka.errors import KafkaError

from simulator.models import TelemetryEvent

TOPIC = "charger-telemetry"
CHARGER_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "chargers.json"

_USER_TIERS = ["Standard", "Priority", "Corporate"]
_CHARGING_SPEEDS = ["Standard", "Fast", "Ultra-Fast"]


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

    def generate_values(self, temp_threshold: float, session_buffer_threshold: int) -> dict:
        if self.state == "GREEN":
            charger_temperature = temp_threshold - random.uniform(5, 15)
            estimated_completion_minutes = random.randint(5, 30)
            session_time_remaining = (
                estimated_completion_minutes + session_buffer_threshold + random.randint(10, 30)
            )
        elif self.state == "YELLOW":
            charger_temperature = temp_threshold * random.uniform(0.90, 0.95)
            estimated_completion_minutes = random.randint(5, 30)
            session_time_remaining = (
                estimated_completion_minutes + random.randint(0, session_buffer_threshold)
            )
        else:  # RED
            charger_temperature = temp_threshold + random.uniform(2, 10)
            estimated_completion_minutes = random.randint(5, 30)
            session_time_remaining = estimated_completion_minutes - random.randint(1, 20)

        return {
            "charger_temperature": round(charger_temperature, 2),
            "estimated_completion_minutes": estimated_completion_minutes,
            "session_time_remaining": session_time_remaining,
        }


@dataclass
class SessionContext:
    session_id: str
    connector_type: str
    energy_requested_kwh: float
    user_tier: str
    charging_speed: str
    session_start_time: float

    @classmethod
    def generate(cls, connector_type: str) -> "SessionContext":
        return cls(
            session_id=str(uuid.uuid4()),
            connector_type=connector_type,
            energy_requested_kwh=round(random.uniform(10.0, 80.0), 2),
            user_tier=random.choice(_USER_TIERS),
            charging_speed=random.choice(_CHARGING_SPEEDS),
            session_start_time=0.0,
        )


class Charger:
    def __init__(
        self,
        charger_id: str,
        temp_threshold: float,
        session_buffer_threshold: int,
        charger_lat: float,
        charger_lng: float,
        rated_power_kw: float,
        site_id: str,
        site_region: str,
        connector_type: str,
        interval_seconds: float = 1.0,
    ) -> None:
        self.charger_id = charger_id
        self.temp_threshold = temp_threshold
        self.session_buffer_threshold = session_buffer_threshold
        self.charger_lat = charger_lat
        self.charger_lng = charger_lng
        self.rated_power_kw = rated_power_kw
        self.site_id = site_id
        self.site_region = site_region
        self.connector_type = connector_type
        self.interval_seconds = interval_seconds
        self._step: int = 0
        self.session_state: str = "AVAILABLE"
        self._state_deadline: float = time.time() + random.uniform(30, 90)
        self._session_context: SessionContext | None = None

    def _maybe_advance_lifecycle(self) -> None:
        now = time.time()
        if now < self._state_deadline:
            return
        if self.session_state == "AVAILABLE":
            self.session_state = "INITIALIZING"
            self._session_context = SessionContext.generate(connector_type=self.connector_type)
            self._state_deadline = now + random.uniform(60, 180)
        elif self.session_state == "INITIALIZING":
            self.session_state = "CHARGING"
            self._session_context.session_start_time = now
            self._state_deadline = now + random.uniform(300, 900)
        elif self.session_state == "CHARGING":
            self.session_state = "SESSION_COMPLETE"
            self._state_deadline = now
        elif self.session_state == "SESSION_COMPLETE":
            self.session_state = "AVAILABLE"
            self._session_context = None
            self._state_deadline = now + random.uniform(30, 90)

    def _compute_dynamic_fields(self) -> dict:
        if self.session_state != "CHARGING" or self._session_context is None:
            return {
                "session_progress": 0.0,
                "estimated_completion_minutes": 0,
                "power_output_kw": 0.0,
                "energy_delivered_kwh": 0.0,
            }

        ctx = self._session_context
        elapsed = time.time() - ctx.session_start_time
        trip_duration = max(self._state_deadline - ctx.session_start_time, 1.0)

        session_progress = min(elapsed / trip_duration, 1.0)
        estimated_completion_minutes = max(
            0, round((1.0 - session_progress) * (trip_duration / 60.0))
        )
        power_output_kw = round(self.rated_power_kw * random.uniform(0.7, 1.0), 2)
        energy_delivered_kwh = round(session_progress * ctx.energy_requested_kwh, 3)

        return {
            "session_progress": round(session_progress, 4),
            "estimated_completion_minutes": estimated_completion_minutes,
            "power_output_kw": power_output_kw,
            "energy_delivered_kwh": energy_delivered_kwh,
        }

    def current_scenario(self) -> Scenario:
        return Scenario.from_step(self._step)

    def generate_event(self, event_ts: datetime | None = None) -> TelemetryEvent:
        self._maybe_advance_lifecycle()
        dynamic = self._compute_dynamic_fields()

        if self.session_state == "CHARGING":
            scenario = self.current_scenario()
            values = scenario.generate_values(
                self.temp_threshold, self.session_buffer_threshold
            )
        else:
            scenario = None
            values = {
                "charger_temperature": 0.0,
                "estimated_completion_minutes": 0,
                "session_time_remaining": 0,
            }

        if event_ts is None:
            event_ts = datetime.now(timezone.utc)
        event_ts_str = event_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        ctx = self._session_context
        return TelemetryEvent(
            event_id=str(uuid.uuid4()),
            event_ts=event_ts_str,
            charger_id=self.charger_id,
            charger_temperature=values["charger_temperature"],
            estimated_completion_minutes=values["estimated_completion_minutes"],
            session_time_remaining=values["session_time_remaining"],
            scenario_state=scenario.state if self.session_state == "CHARGING" else "GREEN",
            session_buffer_threshold=self.session_buffer_threshold,
            temp_threshold=self.temp_threshold,
            session_state=self.session_state,
            session_id=ctx.session_id if ctx else "",
            connector_type=ctx.connector_type if ctx else "",
            energy_requested_kwh=ctx.energy_requested_kwh if ctx else 0.0,
            user_tier=ctx.user_tier if ctx else "",
            charging_speed=ctx.charging_speed if ctx else "",
            site_region=self.site_region,
            session_progress=dynamic["session_progress"],
            power_output_kw=dynamic["power_output_kw"],
            energy_delivered_kwh=dynamic["energy_delivered_kwh"],
            charger_lat=self.charger_lat,
            charger_lng=self.charger_lng,
            rated_power_kw=self.rated_power_kw,
            site_id=self.site_id,
        )

    def advance(self) -> None:
        self._step += 1

    def __lt__(self, other: "Charger") -> bool:
        return self.charger_id < other.charger_id


class Network:
    def __init__(self, chargers: list[Charger]) -> None:
        self.chargers = chargers

    @classmethod
    def default(cls) -> "Network":
        return cls([
            Charger("SG-ORC-01", 50.0, 10, 1.3048, 103.8318, 150.0, "Orchard_Central", "Central", "CCS2"),
            Charger("SG-CBD-01", 45.0, 10, 1.2830, 103.8513,  50.0, "Raffles_Place",   "Central", "CCS2"),
            Charger("SG-CHG-01", 55.0, 10, 1.3644, 103.9915, 350.0, "Changi_Airport",  "East", "CCS2"),
        ])

    @classmethod
    def from_dataset(
        cls,
        path: Path = CHARGER_DATA_PATH,
        n: int | None = None,
    ) -> "Network":
        def _cadence() -> float:
            r = random.random()
            if r < 0.20:
                return 0.5
            if r < 0.80:
                return 1.0
            return 2.0

        records = json.loads(path.read_text(encoding="utf-8"))
        selected_records = records
        if n is not None and n < len(records):
            selected_records = random.sample(records, n)

        chargers = []
        for record in selected_records:
            chargers.append(Charger(
                charger_id=record["charger_id"],
                temp_threshold=round(random.uniform(45.0, 60.0), 1),
                session_buffer_threshold=random.randint(5, 15),
                charger_lat=float(record["latitude"]),
                charger_lng=float(record["longitude"]),
                rated_power_kw=float(record["rated_power_kw"]),
                site_id=record["site_id"],
                site_region=record["site_region"],
                connector_type=record["connector_type"],
                interval_seconds=_cadence(),
            ))
        return cls(chargers)

    def __iter__(self):
        return iter(self.chargers)


def create_producer() -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: v,
            retries=3,
            linger_ms=10,
            batch_size=65536,
            buffer_memory=67108864,
            compression_type="lz4",
        )
    except KafkaError as e:
        print(f"Failed to connect to Kafka at kafka:9092: {e}")
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


def worker(shard: list[Charger], producer: KafkaProducer, stop: threading.Event) -> None:
    now = time.time()
    heap = [(now + charger.interval_seconds * i / len(shard), charger) for i, charger in enumerate(shard)]
    heapq.heapify(heap)

    while not stop.is_set():
        next_time, charger = heapq.heappop(heap)
        wait = next_time - time.time()
        if wait > 0:
            stop.wait(wait)
        if stop.is_set():
            break
        event = charger.generate_event()
        producer.send(
            TOPIC,
            key=charger.charger_id.encode(),
            value=event.to_json_bytes(),
        )
        print(
            f"[{charger.charger_id}] {charger.session_state} | "
            f"{charger.current_scenario().state} | interval={charger.interval_seconds}s | "
            f"event_ts={event.event_ts}"
        )
        _record_event()  # DIAGNOSTIC
        charger.advance()
        heapq.heappush(heap, (time.time() + charger.interval_seconds * random.uniform(0.7, 1.3), charger))


def run(network_size: int) -> None:
    num_threads = 4
    network = Network.from_dataset(n=network_size)
    producer = create_producer()
    stop = threading.Event()

    chargers = list(network)
    shards = [chargers[i::num_threads] for i in range(num_threads)]

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
        "--network-size",
        type=int,
        default=3,
        help="Number of real chargers to load from data/chargers.json (default: 3)",
    )
    args = parser.parse_args()
    run(args.network_size)


if __name__ == "__main__":
    main()
