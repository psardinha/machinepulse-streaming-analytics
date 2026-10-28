from dataclasses import dataclass, field

from machinepulse.schemas import TelemetryEvent


@dataclass
class DataQualityChecker:
    _seen_event_ids: set[str] = field(default_factory=set)
    _seen_device_timestamps: set[tuple[str, object]] = field(default_factory=set)

    def evaluate(self, event: TelemetryEvent) -> list[str]:
        """Return data quality issues found in an event."""
        issues: list[str] = []

        if not event.event_id.strip():
            issues.append("missing_event_id")
        elif event.event_id in self._seen_event_ids:
            issues.append("duplicate_event_id")
        else:
            self._seen_event_ids.add(event.event_id)

        device_timestamp = (event.device_id, event.event_time)

        if device_timestamp in self._seen_device_timestamps:
            issues.append("duplicate_device_timestamp")
        else:
            self._seen_device_timestamps.add(device_timestamp)

        return issues
