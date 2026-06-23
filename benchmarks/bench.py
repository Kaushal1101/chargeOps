"""LogiShield benchmark runner.

Observes a live LogiShield pipeline by consuming from the risk-alerts Kafka
topic. Spark and the simulator must already be running externally.

Run with:
    python -m benchmarks.bench --duration 60 --fleet-size 100
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from kafka import KafkaConsumer

SPARK_REST_BASE = "http://localhost:4040/api/v1"
KAFKA_BOOTSTRAP = "localhost:9093"
RISK_ALERTS_TOPIC = "risk-alerts"
CONSUMER_POLL_TIMEOUT_MS = 1000
REQUEST_TIMEOUT_SECONDS = 5
SPARK_POLL_INTERVAL_SECONDS = 30


def _get_application_id() -> str | None:
    try:
        resp = requests.get(f"{SPARK_REST_BASE}/applications", timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        apps = resp.json()
    except (requests.RequestException, ValueError):
        return None
    return apps[0].get("id") if apps else None


def _get_spark_jobs(app_id: str) -> list[dict]:
    try:
        resp = requests.get(
            f"{SPARK_REST_BASE}/applications/{app_id}/jobs",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"[bench] failed to fetch Spark jobs: {exc}", file=sys.stderr)
        return []
    return data if isinstance(data, list) else []


def _parse_ts(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(ts_str.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    try:
        cut = max(2, min(100, len(values)))
        qs = statistics.quantiles(values, n=cut, method="inclusive")
        idx = max(0, min(len(qs) - 1, int(round((pct / 100.0) * (cut - 1))) - 1))
        return float(qs[idx])
    except statistics.StatisticsError:
        sv = sorted(values)
        idx = max(0, min(len(sv) - 1, int(round((pct / 100.0) * (len(sv) - 1)))))
        return float(sv[idx])


def _safe_mean(values: list[float]) -> float | None:
    return float(statistics.fmean(values)) if values else None


def run_benchmark(duration: int, fleet_size: int, output_path: Path) -> dict:
    run_started_at = datetime.now(timezone.utc).isoformat()
    deadline = time.monotonic() + duration

    lags_seconds: list[float] = []
    tier_counts: dict[str, int] = {}
    alert_wall_times: list[float] = []
    _missing_fields = 0
    _parse_failures = 0
    _negative_lags = 0
    _debug_printed = 0

    jobs_seen: set[int] = set()
    job_durations_ms: list[float] = []
    last_spark_poll = 0.0

    consumer = KafkaConsumer(
        RISK_ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
    )

    print(
        f"[bench] starting observation for {duration}s "
        f"(fleet_size={fleet_size}, topic={RISK_ALERTS_TOPIC})"
    )

    while time.monotonic() < deadline:
        for msg in consumer:
            if time.monotonic() >= deadline:
                break

            alert = msg.value
            alert_wall_times.append(time.time())

            tier = alert.get("risk_tier")
            if tier:
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

            raw_we = alert.get("window_end")
            raw_at = alert.get("alert_ts")
            window_end = _parse_ts(raw_we)
            alert_ts = _parse_ts(raw_at)

            if _debug_printed < 3:
                _debug_printed += 1
                print(f"[debug] window_end={raw_we!r} → {window_end}", file=sys.stderr)
                print(f"[debug] alert_ts  ={raw_at!r} → {alert_ts}", file=sys.stderr)

            if raw_we is None or raw_at is None:
                _missing_fields += 1
            elif window_end is None or alert_ts is None:
                _parse_failures += 1
            else:
                lag = (alert_ts - window_end).total_seconds()
                if lag >= 0:
                    lags_seconds.append(lag)
                else:
                    _negative_lags += 1
                    if _debug_printed <= 3:
                        print(f"[debug] lag={lag:.1f}s (negative, discarded)", file=sys.stderr)

        now = time.monotonic()
        if now - last_spark_poll >= SPARK_POLL_INTERVAL_SECONDS:
            last_spark_poll = now
            app_id = _get_application_id()
            if app_id:
                for job in _get_spark_jobs(app_id):
                    jid = job.get("jobId")
                    if jid is None or jid in jobs_seen:
                        continue
                    jobs_seen.add(jid)
                    dur = job.get("duration")
                    if isinstance(dur, (int, float)):
                        job_durations_ms.append(float(dur))

    consumer.close()

    total_alerts = len(alert_wall_times)
    alerts_per_sec = total_alerts / duration if duration > 0 else 0.0

    summary = {
        "run_ts": run_started_at,
        "fleet_size": fleet_size,
        "duration_seconds": duration,
        "alerts": {
            "total": total_alerts,
            "per_second": round(alerts_per_sec, 3),
            "by_tier": tier_counts,
        },
        "processing_lag_seconds": {
            "avg": _safe_mean(lags_seconds),
            "p50": _percentile(lags_seconds, 50),
            "p95": _percentile(lags_seconds, 95),
            "p99": _percentile(lags_seconds, 99),
            "samples": len(lags_seconds),
            "skipped_missing_fields": _missing_fields,
            "skipped_parse_failures": _parse_failures,
            "skipped_negative_lag": _negative_lags,
        },
        "spark_jobs": {
            "avg_duration_ms": _safe_mean(job_durations_ms),
            "p95_duration_ms": _percentile(job_durations_ms, 95),
            "p99_duration_ms": _percentile(job_durations_ms, 99),
            "samples": len(job_durations_ms),
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def _print_summary(summary: dict, output_path: Path) -> None:
    def fmt(value: float | None, unit: str = "") -> str:
        return f"{value:.2f}{unit}" if value is not None else "n/a"

    alerts = summary.get("alerts", {})
    lag = summary.get("processing_lag_seconds", {})
    spark = summary.get("spark_jobs", {})

    print()
    print("=" * 56)
    print(" LogiShield Benchmark Summary")
    print("=" * 56)
    print(f" run_ts             : {summary.get('run_ts')}")
    print(f" fleet_size         : {summary.get('fleet_size')}")
    print(f" duration_seconds   : {summary.get('duration_seconds')}")
    print("-" * 56)
    print(f" alerts_total       : {alerts.get('total', 0)}")
    print(f" alerts_per_second  : {fmt(alerts.get('per_second'), ' alerts/s')}")
    print(f" by_tier            : {alerts.get('by_tier', {})}")
    print("-" * 56)
    print(f" lag_avg            : {fmt(lag.get('avg'), 's')}")
    print(f" lag_p50            : {fmt(lag.get('p50'), 's')}")
    print(f" lag_p95            : {fmt(lag.get('p95'), 's')}")
    print(f" lag_p99            : {fmt(lag.get('p99'), 's')}")
    print(f" lag_samples        : {lag.get('samples', 0)}")
    print(f" lag_skip_missing   : {lag.get('skipped_missing_fields', 0)}")
    print(f" lag_skip_parse     : {lag.get('skipped_parse_failures', 0)}")
    print(f" lag_skip_negative  : {lag.get('skipped_negative_lag', 0)}")
    print("-" * 56)
    print(f" spark_avg_job_ms   : {fmt(spark.get('avg_duration_ms'), ' ms')}")
    print(f" spark_p95_job_ms   : {fmt(spark.get('p95_duration_ms'), ' ms')}")
    print("-" * 56)
    print(f" results written to : {output_path}")
    print("=" * 56)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Observe a live LogiShield pipeline and record metrics."
    )
    parser.add_argument("--duration", type=int, default=60,
                        help="Seconds to observe (default: 60).")
    parser.add_argument("--fleet-size", type=int, default=0,
                        help="Fleet size for this run (recorded in output).")
    parser.add_argument("--output", type=str,
                        default="benchmarks/results/benchmark_results.json",
                        help="Path to write JSON result file.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    output_path = Path(args.output)
    summary = run_benchmark(args.duration, args.fleet_size, output_path)
    _print_summary(summary, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
