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

KAFKA_BOOTSTRAP = "kafka:9092"
RISK_ALERTS_TOPIC = "risk-alerts"
CONSUMER_GROUP = "logishield-redis-state"
REDIS_HOST = "redis"
REDIS_PORT = 6379
CHARGER_KEY_TTL_SECONDS = 420


def _recount_network(r: redis.Redis) -> None:
    counts: dict[str, int] = {}
    for key in r.scan_iter("charger:*"):
        tier = r.hget(key, "tier")
        if tier:
            counts[tier] = counts.get(tier, 0) + 1
    r.delete("network:counts")
    if counts:
        r.hset("network:counts", mapping=counts)


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

    _recount_network(r)
    print("[state_consumer] network:counts rebuilt from active charger keys")

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
            charger_id = alert.get("charger_id")
            tier = alert.get("risk_tier")
            if not charger_id or not tier:
                continue

            charger_key = f"charger:{charger_id}"
            prev_tier = r.hget(charger_key, "tier")

            r.hset(
                charger_key,
                mapping={
                    "charger_id": charger_id,
                    "tier": tier,
                    "session_buffer": str(alert.get("session_buffer", "")),
                    "avg_temperature": str(alert.get("avg_temperature", "")),
                    "window_start": alert.get("window_start", ""),
                    "window_end": alert.get("window_end", ""),
                    "alert_ts": alert.get("alert_ts", ""),
                    "reason": alert.get("reason", ""),
                    "session_state": alert.get("session_state", ""),
                    "session_id": alert.get("session_id", ""),
                    "connector_type": alert.get("connector_type", ""),
                    "energy_requested_kwh": str(alert.get("energy_requested_kwh", "")),
                    "user_tier": alert.get("user_tier", ""),
                    "charging_speed": alert.get("charging_speed", ""),
                    "site_region": alert.get("site_region", ""),
                    "session_progress": str(alert.get("session_progress", "")),
                    "estimated_completion_minutes": str(alert.get("estimated_completion_minutes", "")),
                    "power_output_kw": str(alert.get("power_output_kw", "")),
                    "energy_delivered_kwh": str(alert.get("energy_delivered_kwh", "")),
                    "charger_lat": str(alert.get("charger_lat", "")),
                    "charger_lng": str(alert.get("charger_lng", "")),
                    "rated_power_kw": str(alert.get("rated_power_kw", "")),
                    "site_id": alert.get("site_id", ""),
                },
            )
            r.expire(charger_key, CHARGER_KEY_TTL_SECONDS)

            if prev_tier != tier:
                if prev_tier:
                    r.hincrby("network:counts", prev_tier, -1)
                r.hincrby("network:counts", tier, 1)

            r.hset(
                "network:last_update",
                mapping={
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "charger_id": charger_id,
                    "tier": tier,
                },
            )

            if verbose:
                print(f"[state_consumer] {charger_id} → {tier} (prev: {prev_tier or 'new'})")
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
