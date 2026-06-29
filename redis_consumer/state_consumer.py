"""Redis fleet state consumer for LogiShield.

Reads risk-alerts from Kafka and materializes the latest per-truck state
into Redis. Spark and Kafka must already be running externally.

Run with:
    python -m redis_consumer.state_consumer
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

import redis
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9093"
RISK_ALERTS_TOPIC = "risk-alerts"
CONSUMER_GROUP = "logishield-redis-state"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
TRUCK_KEY_TTL_SECONDS = 420


def _recount_fleet(r: redis.Redis) -> None:
    counts: dict[str, int] = {}
    for key in r.scan_iter("truck:*"):
        tier = r.hget(key, "tier")
        if tier:
            counts[tier] = counts.get(tier, 0) + 1
    r.delete("fleet:counts")
    if counts:
        r.hset("fleet:counts", mapping=counts)


def run(verbose: bool = False) -> None:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        r.ping()
    except redis.RedisError:
        print(
            f"[state_consumer] Redis not reachable at {REDIS_HOST}:{REDIS_PORT}",
            file=sys.stderr,
        )
        raise

    _recount_fleet(r)
    print("[state_consumer] fleet:counts rebuilt from active truck keys")

    consumer = KafkaConsumer(
        RISK_ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print(f"[state_consumer] consuming risk-alerts (group={CONSUMER_GROUP})")

    for msg in consumer:
        try:
            alert = msg.value
            vehicle_id = alert.get("vehicle_id")
            tier = alert.get("risk_tier")
            if not vehicle_id or not tier:
                continue

            truck_key = f"truck:{vehicle_id}"
            prev_tier = r.hget(truck_key, "tier")

            r.hset(
                truck_key,
                mapping={
                    "vehicle_id": vehicle_id,
                    "tier": tier,
                    "delivery_buffer": str(alert.get("delivery_buffer", "")),
                    "avg_temperature": str(alert.get("avg_temperature", "")),
                    "window_start": alert.get("window_start", ""),
                    "window_end": alert.get("window_end", ""),
                    "alert_ts": alert.get("alert_ts", ""),
                    "reason": alert.get("reason", ""),
                },
            )
            r.expire(truck_key, TRUCK_KEY_TTL_SECONDS)

            if prev_tier != tier:
                if prev_tier:
                    r.hincrby("fleet:counts", prev_tier, -1)
                r.hincrby("fleet:counts", tier, 1)

            r.hset(
                "fleet:last_update",
                mapping={
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "vehicle_id": vehicle_id,
                    "tier": tier,
                },
            )

            if verbose:
                print(f"[state_consumer] {vehicle_id} → {tier} (prev: {prev_tier or 'new'})")
        except Exception as exc:
            print(f"[state_consumer] skipping malformed message: {exc}", file=sys.stderr)
            continue


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Materialize risk-alerts into Redis fleet state."
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print each state update."
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    run(verbose=args.verbose)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
