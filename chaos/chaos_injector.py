import argparse
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from simulator.models import TelemetryEvent
from simulator.simulator import (
    FLEET,
    TOPIC,
    create_producer,
    generate_values,
    next_scenario,
)


def run_out_of_order(delay_seconds: int) -> None:
    producer = create_producer()
    steps = {truck["vehicle_id"]: 0 for truck in FLEET}

    try:
        while True:
            for truck in FLEET:
                vehicle_id = truck["vehicle_id"]
                temp_threshold = truck["cargo_temp_threshold"]
                sla_threshold = truck["sla_buffer_threshold"]

                step = steps[vehicle_id]
                scenario = next_scenario(step)
                values = generate_values(scenario, temp_threshold, sla_threshold)

                offset = random.randint(0, delay_seconds)
                now = datetime.now(timezone.utc)
                backdated = now - timedelta(seconds=offset)
                event_ts = backdated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                event = TelemetryEvent(
                    event_id=str(uuid.uuid4()),
                    event_ts=event_ts,
                    vehicle_id=vehicle_id,
                    cargo_temperature=values["cargo_temperature"],
                    time_left_to_destination=values["time_left_to_destination"],
                    sla_time_remaining=values["sla_time_remaining"],
                    scenario_state=scenario,
                    sla_buffer_threshold=truck["sla_buffer_threshold"],
                    cargo_temp_threshold=truck["cargo_temp_threshold"],
                )

                producer.send(
                    TOPIC,
                    key=vehicle_id.encode(),
                    value=event.to_json_bytes(),
                )

                print(
                    f"[{vehicle_id}] {scenario} | offset=-{offset}s | event_ts={event_ts}"
                )

                steps[vehicle_id] += 1

            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Chaos injector stopped.")
        producer.flush()
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, required=True, choices=["out_of_order"])
    parser.add_argument(
        "--delay",
        type=int,
        required=True,
        help="Max backdate offset in seconds",
    )
    args = parser.parse_args()

    if args.mode == "out_of_order":
        run_out_of_order(args.delay)


if __name__ == "__main__":
    main()
