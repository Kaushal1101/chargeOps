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


def run_delayed_burst(
    delay_seconds: int, burst_vehicle_id: str = "TRUCK_101"
) -> None:
    fleet = Fleet.default()
    producer = create_producer()
    injector = ChaosInjector()
    burst_steps = 30

    burst_vehicle = next(v for v in fleet if v.vehicle_id == burst_vehicle_id)

    for _ in range(burst_steps):
        event = burst_vehicle.generate_event()
        chaos_event = injector.apply_delayed_burst(event, delay_seconds)
        producer.send(
            TOPIC,
            key=burst_vehicle.vehicle_id.encode(),
            value=chaos_event.to_json_bytes(),
        )
        print(
            f"[{burst_vehicle.vehicle_id}] {burst_vehicle.current_scenario().state} | delay=-{delay_seconds}s | event_ts={chaos_event.event_ts}"
        )
        burst_vehicle.advance()

    producer.flush()
    producer.close()
    print(
        f"Delayed burst complete. {burst_steps} events sent for {burst_vehicle_id}."
    )


def run_packet_loss(loss_rate: float) -> None:
    fleet = Fleet.default()
    producer = create_producer()
    generated = 0
    sent = 0

    try:
        while True:
            for vehicle in fleet:
                event = vehicle.generate_event()
                generated += 1

                if random.random() < loss_rate:
                    print(
                        f"[{vehicle.vehicle_id}] DROPPED | {vehicle.current_scenario().state}"
                    )
                else:
                    producer.send(
                        TOPIC,
                        key=vehicle.vehicle_id.encode(),
                        value=event.to_json_bytes(),
                    )
                    sent += 1
                    print(
                        f"[{vehicle.vehicle_id}] {vehicle.current_scenario().state} | event_ts={event.event_ts}"
                    )

                vehicle.advance()

            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        dropped = generated - sent
        actual_loss_rate = (dropped / generated * 100) if generated > 0 else 0.0
        print("\nPacket loss summary")
        print(f"Events generated : {generated}")
        print(f"Events sent      : {sent}")
        print(f"Events dropped   : {dropped}")
        print(f"Actual loss rate : {actual_loss_rate:.1f}%")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["out_of_order", "delayed_burst", "packet_loss"],
    )
    parser.add_argument(
        "--delay",
        type=int,
        help="Max backdate offset in seconds",
    )
    parser.add_argument(
        "--vehicle",
        type=str,
        default="TRUCK_101",
        help="Vehicle ID to target for delayed burst (default: TRUCK_101)",
    )
    parser.add_argument(
        "--loss-rate",
        type=float,
        help="Fraction of events to drop (0.0-1.0)",
    )
    args = parser.parse_args()

    if args.mode == "out_of_order":
        if args.delay is None:
            parser.error("--delay is required for out_of_order mode")
        run_out_of_order(args.delay)
    elif args.mode == "delayed_burst":
        if args.delay is None:
            parser.error("--delay is required for delayed_burst mode")
        run_delayed_burst(args.delay, args.vehicle)
    elif args.mode == "packet_loss":
        if args.loss_rate is None:
            parser.error("--loss-rate is required for packet_loss mode")
        run_packet_loss(args.loss_rate)


if __name__ == "__main__":
    main()
