from datetime import UTC, datetime, timedelta

from machinepulse.anomaly_detection import AnomalyDetector
from machinepulse.schemas import TelemetryEvent


def make_event(event_id: str, device_id: str, temperature: float, power_kw: float, second: int) -> TelemetryEvent:
    return TelemetryEvent(event_id=event_id,
                          device_id=device_id,
                          event_time=datetime(2026, 10, 23, tzinfo=UTC) + timedelta(seconds=second),
                          temperature=temperature,
                          power_kw=power_kw)


def test_alert_requires_three_consecutive_high_readings():
    detector = AnomalyDetector(required_consecutive_readings=3)

    assert detector.evaluate(make_event("1", "machine-1", 41, 50, 0)) == []
    assert detector.evaluate(make_event("2", "machine-1", 42, 50, 1)) == []
    assert detector.evaluate(make_event("3", "machine-1", 43, 50, 2)) == ["sustained_high_temperature"]

    assert detector.evaluate(make_event("1", "machine-1", 21, 71, 0)) == []
    assert detector.evaluate(make_event("2", "machine-1", 22, 72, 1)) == []
    assert detector.evaluate(make_event("3", "machine-1", 43, 73, 2)) == ["sustained_high_power_consumption"]

def test_normal_reading_resets_the_counter():
    detector = AnomalyDetector(required_consecutive_readings=3)

    detector.evaluate(make_event("1", "machine-1", 70, 71, 0))
    detector.evaluate(make_event("2", "machine-1", 70, 72, 1))
    detector.evaluate(make_event("3", "machine-1", 30, 50, 2))

    assert detector.evaluate(make_event("4", "machine-1", 70, 50, 3)) == []
    assert detector.evaluate(make_event("5", "machine-1", 70, 50, 4)) == []


def test_alert_is_not_repeated_while_condition_persists():
    detector = AnomalyDetector(required_consecutive_readings=3)

    results = [detector.evaluate(make_event(str(i), "machine-1", 70, 75, i)) for i in range(5)]

    assert set(results[2]) == {"sustained_high_power_consumption", "sustained_high_temperature"}
    assert results[3] == []
    assert results[4] == []


def test_devices_have_independent_state():
    detector = AnomalyDetector(required_consecutive_readings=3)

    detector.evaluate(make_event("1", "machine-1", 70, 70, 0))
    detector.evaluate(make_event("2", "machine-1", 70, 71, 1))

    assert detector.evaluate(make_event("3", "machine-2", 70, 72, 2)) == []
