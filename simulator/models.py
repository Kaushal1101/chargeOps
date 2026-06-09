import json
import uuid
from datetime import datetime, timezone
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

    @classmethod
    def create(
        cls,
        vehicle_id: str,
        cargo_temperature: float,
        time_left_to_destination: int,
        sla_time_remaining: int,
        scenario_state: str,
        sla_buffer_threshold: int,
        cargo_temp_threshold: float,
    ) -> "TelemetryEvent":
        return cls(
            event_id=str(uuid.uuid4()),
            event_ts=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            vehicle_id=vehicle_id,
            cargo_temperature=cargo_temperature,
            time_left_to_destination=time_left_to_destination,
            sla_time_remaining=sla_time_remaining,
            scenario_state=scenario_state,
            sla_buffer_threshold=sla_buffer_threshold,
            cargo_temp_threshold=cargo_temp_threshold,
        )

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.model_dump()).encode("utf-8")
