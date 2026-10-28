import random
from datetime import UTC, datetime, timedelta

from machinepulse.schemas import TelemetryEvent


def generate_events(count: int = 100, device_count: int = 10, seed: int = 42) -> list[TelemetryEvent]:
    """Generate reproducible telemetry for simulated machines."""
    if count < 0:
        raise ValueError("count must be non-negative")
    if device_count < 1:
        raise ValueError("device_count must be positive")

    rng = random.Random(seed)
    start = datetime.now(UTC)
    events = []

    for i in range(count):
        device_number = i % device_count + 1
        device_id = f"machine-{device_number:03d}"

        temperature = rng.gauss(25, 8)
        power_kw = max(0.0, rng.gauss(50, 10))

        # Inject an abnormal period for devices # 3 and 7
        if device_number in (3, 7) and i // device_count in (device_number+3, device_number+4, device_number+5, device_number+6):
            temperature = min(max(41, temperature + 30), 68)
            power_kw += 60

        event = TelemetryEvent(event_id=f"evt-{i + 1:06d}",
                               device_id=device_id,
                               event_time=start + timedelta(seconds=i),
                               temperature=round(temperature, 2),
                               power_kw=power_kw)
        events.append(event)

    return events
