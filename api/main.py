"""ChargeOps operational API — read-only Redis state over HTTP.

Run with:
    uvicorn api.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import NoReturn

import redis
from fastapi import FastAPI, HTTPException

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STALENESS_THRESHOLD_SECONDS = 600

app = FastAPI(title="ChargeOps Operational API")
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def _redis_unavailable() -> NoReturn:
    raise HTTPException(status_code=503, detail="Redis unavailable")


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


@app.get("/health")
def health():
    try:
        data = r.hgetall("network:last_update")
    except redis.RedisError:
        _redis_unavailable()
    if not data:
        return {
            "status": "stale",
            "pipeline_active": False,
            "last_update_ts": None,
            "last_charger_id": None,
            "last_tier": None,
        }
    ts = data.get("ts")
    parsed = _parse_ts(ts)
    active = parsed is not None and (
        datetime.now(timezone.utc) - parsed
    ).total_seconds() < STALENESS_THRESHOLD_SECONDS
    return {
        "status": "ok" if active else "stale",
        "pipeline_active": active,
        "last_update_ts": ts,
        "last_charger_id": data.get("charger_id"),
        "last_tier": data.get("tier"),
    }


@app.get("/summary")
def summary():
    try:
        counts = r.hgetall("network:counts")
    except redis.RedisError:
        _redis_unavailable()
    return {
        "red_alerts": int(counts.get("RED") or 0),
        "yellow_alerts": int(counts.get("YELLOW") or 0),
    }


@app.get("/chargers")
def chargers():
    try:
        return [rec for key in r.scan_iter("charger:*") if (rec := r.hgetall(key))]
    except redis.RedisError:
        _redis_unavailable()


@app.get("/chargers/{charger_id}")
def charger(charger_id: str):
    cid = charger_id.upper()
    try:
        data = r.hgetall(f"charger:{cid}")
        if not data:
            raise HTTPException(status_code=404, detail="Charger not found")
        return data
    except redis.RedisError:
        _redis_unavailable()


@app.get("/stats/network")
def stats_network():
    try:
        return r.hgetall("stats:network")
    except redis.RedisError:
        _redis_unavailable()


@app.get("/stats/regions")
def stats_regions():
    try:
        result = []
        for key in r.scan_iter("stats:region:*"):
            rec = r.hgetall(key)
            rec["region"] = key.split("stats:region:")[-1]
            result.append(rec)
        return result
    except redis.RedisError:
        _redis_unavailable()


@app.get("/stats/connectors")
def stats_connectors():
    try:
        result = []
        for key in r.scan_iter("stats:connector:*"):
            rec = r.hgetall(key)
            rec["connector_type"] = key.split("stats:connector:")[-1]
            result.append(rec)
        return result
    except redis.RedisError:
        _redis_unavailable()
