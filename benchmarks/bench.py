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
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

SPARK_PROGRESS_FILE = "/tmp/logishield-spark-progress.jsonl"
KAFKA_BOOTSTRAP = "localhost:9093"
FLEET_TELEMETRY_TOPIC = "fleet-telemetry"
RISK_ALERTS_TOPIC = "risk-alerts"
CONSUMER_POLL_TIMEOUT_MS = 1000


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
    telemetry_event_count = 0

    input_rates: list[float] = []
    processing_rates: list[float] = []
    trigger_durations_ms: list[float] = []
    _progress_path = Path(SPARK_PROGRESS_FILE)
    last_progress_line = len(_progress_path.read_text(encoding="utf-8").splitlines()) if _progress_path.exists() else 0

    consumer = KafkaConsumer(
        RISK_ALERTS_TOPIC,
        FLEET_TELEMETRY_TOPIC,
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

            if msg.topic == FLEET_TELEMETRY_TOPIC:
                telemetry_event_count += 1
                continue

            alert = msg.value
            alert_wall_times.append(time.time())

            tier = alert.get("risk_tier")
            if tier:
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

            window_end = _parse_ts(alert.get("window_end"))
            alert_ts = _parse_ts(alert.get("alert_ts"))

            if window_end is not None and alert_ts is not None:
                lag = (alert_ts - window_end).total_seconds()
                if lag >= 0:
                    lags_seconds.append(lag)

        if _progress_path.exists():
            lines = [ln for ln in _progress_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
            for line in lines[last_progress_line:]:
                rec = json.loads(line)
                ir = rec.get("inputRowsPerSecond")
                pr = rec.get("processedRowsPerSecond")
                if isinstance(ir, (int, float)):
                    input_rates.append(float(ir))
                if isinstance(pr, (int, float)):
                    processing_rates.append(float(pr))
                dur = rec.get("durationMs") or {}
                te = dur.get("triggerExecution")
                if isinstance(te, (int, float)):
                    trigger_durations_ms.append(float(te))
            last_progress_line = len(lines)

    consumer.close()

    total_alerts = len(alert_wall_times)
    alerts_per_sec = total_alerts / duration if duration > 0 else 0.0

    summary = {
        "run_ts": run_started_at,
        "fleet_size": fleet_size,
        "duration_seconds": duration,
        "telemetry_ingress": {
            "total_events": telemetry_event_count,
            "events_per_second": round(telemetry_event_count / duration, 3) if duration > 0 else 0.0,
        },
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
        },
        "spark": {
            "input_rows_per_second_avg": _safe_mean(input_rates),
            "processed_rows_per_second_avg": _safe_mean(processing_rates),
            "trigger_duration_ms_avg": _safe_mean(trigger_durations_ms),
            "trigger_duration_ms_p95": _percentile(trigger_durations_ms, 95),
            "batch_samples": len(trigger_durations_ms),
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def _print_summary(summary: dict, output_path: Path) -> None:
    def fmt(value: float | None, unit: str = "") -> str:
        return f"{value:.2f}{unit}" if value is not None else "n/a"

    ingress = summary.get("telemetry_ingress", {})
    alerts = summary.get("alerts", {})
    lag = summary.get("processing_lag_seconds", {})
    spark = summary.get("spark", {})

    print()
    print("=" * 56)
    print(" LogiShield Benchmark Summary")
    print("=" * 56)
    print(f" run_ts             : {summary.get('run_ts')}")
    print(f" fleet_size         : {summary.get('fleet_size')}")
    print(f" duration_seconds   : {summary.get('duration_seconds')}")
    print("-" * 56)
    print(f" telemetry_events   : {ingress.get('total_events', 0)}")
    print(f" events_per_second  : {fmt(ingress.get('events_per_second'), ' ev/s')}")
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
    print("-" * 56)
    print(f" spark_input_rps    : {fmt(spark.get('input_rows_per_second_avg'), ' rows/s')}")
    print(f" spark_process_rps  : {fmt(spark.get('processed_rows_per_second_avg'), ' rows/s')}")
    print(f" spark_trigger_ms   : {fmt(spark.get('trigger_duration_ms_avg'), ' ms')}")
    print(f" spark_trigger_p95  : {fmt(spark.get('trigger_duration_ms_p95'), ' ms')}")
    print(f" spark_batch_samples: {spark.get('batch_samples', 0)}")
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
