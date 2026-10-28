from collections import deque

from machinepulse.schemas import TelemetryEvent


class LocalStream:
    """A minimal in-memory stream for local development."""

    def __init__(self) -> None:
        self._events: deque[TelemetryEvent] = deque()

    def publish(self, event: TelemetryEvent) -> None:
        self._events.append(event)

    def consume_batch(self, max_records: int = 100) -> list[TelemetryEvent]:
        if max_records < 1:
            raise ValueError("max_records must be positive")

        batch = []
        while self._events and len(batch) < max_records:
            batch.append(self._events.popleft())
        return batch

    def __len__(self) -> int:
        return len(self._events)
