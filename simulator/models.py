import json
from typing import Literal

from pydantic import BaseModel


class TelemetryEvent(BaseModel):
    event_id: str
    event_ts: str
    vehicle_id: str
    cargo_temperature: float
    time_left_to_destination: int
    sla_time_remaining: int
    scenario_state: Literal["GREEN", "YELLOW", "RED"]
    sla_buffer_threshold: int
    cargo_temp_threshold: float
    trip_state: str
    trip_id: str
    cargo_type: str
    cargo_value: float
    customer_priority: str
    service_level: str
    destination_region: str
    route_progress: float
    estimated_arrival_minutes: int
    remaining_stops: int
    driver_hours_remaining: float

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.model_dump()).encode("utf-8")
