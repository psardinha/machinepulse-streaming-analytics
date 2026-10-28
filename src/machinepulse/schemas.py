from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field


class TelemetryEvent(BaseModel):
    """One timestamped reading from an industrial machine."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    device_id: str
    event_time: datetime
    temperature: float = Field(ge=-10, le=70)
    power_kw: float = Field(ge=0, le=100_000)

    @classmethod
    def create_example(cls, device_id: str = "machine-001", temperature: float = 20.0) -> "TelemetryEvent":
        return cls(
            event_id="evt-example-001",
            device_id=device_id,
            event_time=datetime.now(UTC),
            temperature=temperature,
            power_kw=50,
        )
