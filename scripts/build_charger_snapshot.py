"""
Build a normalised EV charger snapshot from the LTA DataMall raw dataset.

Input:  data/chargers_raw.json  (fetched via LTA DataMall EVCBatch endpoint)
Output: data/chargers.json      (committed snapshot used by the simulator)

Run with:
    python scripts/build_charger_snapshot.py

To refresh the raw data first:
    python scripts/fetch_chargers.py
"""

import json
import re
from collections import Counter
from pathlib import Path

RAW_PATH = Path("data/chargers_raw.json")
OUT_PATH = Path("data/chargers.json")

# Map LTA plug type labels to our internal connector_type vocabulary
PLUG_TYPE_MAP = {
    "Type 2":   "Type2",
    "Combo 2":  "CCS2",
    "CHAdeMO":  "CHAdeMO",
}


def _region(lat: float, lng: float) -> str:
    if lat < 1.27:
        return "South"
    if lat > 1.40:
        return "North"
    if lng > 103.87:
        return "East"
    if lng < 103.78:
        return "West"
    return "Central"


def _slugify(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9\s]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    return name[:50]


def _best_plug(plug_types: list) -> dict:
    """Return the highest-power plug type from a charging point."""
    if not plug_types:
        return {}
    return max(plug_types, key=lambda p: float(p.get("powerRating") or 0))


def normalise(locations: list) -> list:
    chargers = []
    seen_ids: set[str] = set()

    for loc in locations:
        lat = loc.get("latitude")
        lng = loc.get("longtitude")  # LTA dataset has this typo
        if not lat or not lng:
            continue
        if not (1.20 <= lat <= 1.50 and 103.5 <= lng <= 104.1):
            continue

        site_name = (loc.get("name") or "").strip().title()
        site_id = _slugify(site_name)
        site_region = _region(lat, lng)

        for cp in loc.get("chargingPoints", []):
            plug_types = cp.get("plugTypes", [])
            if not plug_types:
                continue

            best = _best_plug(plug_types)
            ev_ids = best.get("evIds", [])
            if not ev_ids:
                continue

            charger_id = ev_ids[0].get("evCpId", "")
            if not charger_id or charger_id in seen_ids:
                continue
            seen_ids.add(charger_id)

            connector_type = PLUG_TYPE_MAP.get(best.get("plugType", ""), "Type2")

            try:
                rated_power_kw = float(best.get("powerRating") or 7.4)
            except (ValueError, TypeError):
                rated_power_kw = 7.4

            operator = (cp.get("operator") or "").strip().title()

            chargers.append({
                "charger_id":    charger_id,
                "site_name":     site_name,
                "site_id":       site_id,
                "site_region":   site_region,
                "latitude":      lat,
                "longitude":     lng,
                "connector_type": connector_type,
                "rated_power_kw": rated_power_kw,
                "operator":      operator,
            })

    return chargers


def print_stats(chargers: list) -> None:
    regions    = Counter(c["site_region"]    for c in chargers)
    connectors = Counter(c["connector_type"] for c in chargers)
    operators  = Counter(c["operator"]       for c in chargers)

    print(f"\nTotal chargers: {len(chargers)}")

    print("\nBy region:")
    for r, n in sorted(regions.items()):
        print(f"  {r}: {n}")

    print("\nBy connector type:")
    for k, n in connectors.most_common():
        print(f"  {k}: {n}")

    print("\nTop 10 operators:")
    for k, n in operators.most_common(10):
        print(f"  {k}: {n}")

    power_vals = [c["rated_power_kw"] for c in chargers]
    print(f"\nPower range: {min(power_vals):.1f} – {max(power_vals):.1f} kW")
    dc = [c for c in chargers if c["connector_type"] in ("CCS2", "CHAdeMO")]
    print(f"DC fast chargers (CCS2/CHAdeMO): {len(dc)}")


def main() -> None:
    if not RAW_PATH.exists():
        print(f"Raw data not found at {RAW_PATH}.")
        print("Run scripts/fetch_chargers.py first.")
        raise SystemExit(1)

    raw = json.loads(RAW_PATH.read_text())
    locations = raw["evLocationsData"]
    print(f"Raw locations loaded: {len(locations)}")
    print(f"Last updated: {raw.get('LastUpdatedTime', 'unknown')}")

    chargers = normalise(locations)
    print_stats(chargers)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(chargers, indent=2))
    print(f"\nSnapshot written to {OUT_PATH}")


if __name__ == "__main__":
    main()
