import random
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from simulator.models import TelemetryEvent

TOPIC = "fleet-telemetry"

FLEET = [
    {"vehicle_id": "TRUCK_101", "cargo_temp_threshold": 5.0, "sla_buffer_threshold": 30},
    {"vehicle_id": "TRUCK_102", "cargo_temp_threshold": 6.0, "sla_buffer_threshold": 25},
    {"vehicle_id": "TRUCK_103", "cargo_temp_threshold": 4.5, "sla_buffer_threshold": 35},
]


def next_scenario(step: int) -> str:
    phase = step % 15
    if phase < 5:
        return "GREEN"
    if phase < 10:
        return "YELLOW"
    return "RED"


def generate_values(scenario: str, temp_threshold: float, sla_threshold: int) -> dict:
    if scenario == "GREEN":
        cargo_temperature = temp_threshold - random.uniform(1.5, 3.0)
        sla_time_remaining = sla_threshold + random.randint(60, 120)
        time_left_to_destination = random.randint(30, 60)
    elif scenario == "YELLOW":
        cargo_temperature = temp_threshold - random.uniform(0.1, 0.5)
        sla_time_remaining = sla_threshold + random.randint(5, 20)
        time_left_to_destination = random.randint(10, 30)
    else:  # RED
        cargo_temperature = temp_threshold + random.uniform(0.1, 2.0)
        sla_time_remaining = sla_threshold - random.randint(5, 20)
        time_left_to_destination = random.randint(5, 15)

    return {
        "cargo_temperature": round(cargo_temperature, 2),
        "time_left_to_destination": time_left_to_destination,
        "sla_time_remaining": sla_time_remaining,
    }


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


def run() -> None:
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

                event = TelemetryEvent.create(
                    vehicle_id=vehicle_id,
                    cargo_temperature=values["cargo_temperature"],
                    time_left_to_destination=values["time_left_to_destination"],
                    sla_time_remaining=values["sla_time_remaining"],
                    scenario_state=scenario,
                    sla_buffer_threshold=sla_threshold,
                    cargo_temp_threshold=temp_threshold,
                )

                producer.send(
                    TOPIC,
                    key=vehicle_id.encode(),
                    value=event.to_json_bytes(),
                )

                buffer = values["sla_time_remaining"] - values["time_left_to_destination"]
                print(
                    f"[{vehicle_id}] {scenario} | temp={values['cargo_temperature']} | buffer={buffer}min"
                )

                steps[vehicle_id] += 1

            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Simulator stopped.")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()
