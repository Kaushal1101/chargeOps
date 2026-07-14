import json
from typing import Literal

from pydantic import BaseModel


class TelemetryEvent(BaseModel):
    event_id: str
    event_ts: str
    charger_id: str
    charger_temperature: float
    estimated_completion_minutes: int
    session_time_remaining: int
    scenario_state: Literal["GREEN", "YELLOW", "RED"]
    session_buffer_threshold: int
    temp_threshold: float
    session_state: str
    session_id: str
    connector_type: str
    energy_requested_kwh: float
    user_tier: str
    charging_speed: str
    site_region: str
    session_progress: float
    power_output_kw: float
    energy_delivered_kwh: float
    charger_lat: float
    charger_lng: float
    rated_power_kw: float
    site_id: str

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.model_dump()).encode("utf-8")
