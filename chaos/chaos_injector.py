import argparse
import random
import time
from datetime import datetime, timedelta, timezone

from simulator.models import TelemetryEvent
from simulator.simulator import (
    TOPIC,
    Fleet,
    create_producer,
)


class ChaosInjector:
    def apply_out_of_order(
        self, event: TelemetryEvent, max_delay: int
    ) -> TelemetryEvent:
        offset = random.randint(0, max_delay)
        original_ts = datetime.strptime(
            event.event_ts, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=timezone.utc)
        backdated = original_ts - timedelta(seconds=offset)
        backdated_str = backdated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return event.model_copy(update={"event_ts": backdated_str})

    def apply_delayed_burst(
        self, event: TelemetryEvent, delay: int
    ) -> TelemetryEvent:
        original_ts = datetime.strptime(
            event.event_ts, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=timezone.utc)
        backdated = original_ts - timedelta(seconds=delay)
        backdated_str = backdated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return event.model_copy(update={"event_ts": backdated_str})


def run_out_of_order(delay_seconds: int) -> None:
    fleet = Fleet.default()
    producer = create_producer()
    injector = ChaosInjector()

    try:
        while True:
            for vehicle in fleet:
                event = vehicle.generate_event()
                chaos_event = injector.apply_out_of_order(event, delay_seconds)
                producer.send(
                    TOPIC,
                    key=vehicle.vehicle_id.encode(),
                    value=chaos_event.to_json_bytes(),
                )
                print(
                    f"[{vehicle.vehicle_id}] {vehicle.current_scenario().state} | event_ts={chaos_event.event_ts}"
                )
                vehicle.advance()

            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Chaos injector stopped.")
        producer.flush()
        producer.close()


def run_delayed_burst(delay_seconds: int) -> None:
    fleet = Fleet.default()
    producer = create_producer()
    injector = ChaosInjector()
    burst_steps = 30  # 30 steps x 3 trucks = 90 events

    for _ in range(burst_steps):
        for vehicle in fleet:
            event = vehicle.generate_event()
            chaos_event = injector.apply_delayed_burst(event, delay_seconds)
            producer.send(
                TOPIC,
                key=vehicle.vehicle_id.encode(),
                value=chaos_event.to_json_bytes(),
            )
            print(
                f"[{vehicle.vehicle_id}] {vehicle.current_scenario().state} | delay=-{delay_seconds}s | event_ts={chaos_event.event_ts}"
            )
            vehicle.advance()

    producer.flush()
    producer.close()
    print(f"Delayed burst complete. {burst_steps * 3} events sent.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["out_of_order", "delayed_burst"],
    )
    parser.add_argument(
        "--delay",
        type=int,
        required=True,
        help="Max backdate offset in seconds",
    )
    args = parser.parse_args()

    if args.mode == "out_of_order":
        run_out_of_order(args.delay)
    elif args.mode == "delayed_burst":
        run_delayed_burst(args.delay)


if __name__ == "__main__":
    main()
