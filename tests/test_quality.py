
from machinepulse.quality import DataQualityChecker
from machinepulse.schemas import TelemetryEvent


def make_event(event_id: str = "evt-001", device_id: str = "machine-001", temperature: float = 20.0) -> TelemetryEvent:
    event = TelemetryEvent.create_example(device_id=device_id, temperature=temperature)
    return event.model_copy(update={"event_id": event_id})


def test_first_event_has_no_quality_issues() -> None:
    checker = DataQualityChecker()
    issues = checker.evaluate(make_event())
    assert issues == []


def test_duplicate_event_id_is_detected() -> None:
    checker = DataQualityChecker()
    checker.evaluate(make_event(event_id="evt-001"))
    issues = checker.evaluate(make_event(event_id="evt-001"))
    assert "duplicate_event_id" in issues


def test_duplicate_device_timestamp_is_detected() -> None:
    checker = DataQualityChecker()
    event = make_event()
    checker.evaluate(event)
    issues = checker.evaluate(event)
    assert "duplicate_device_timestamp" in issues


def test_same_timestamp_on_different_devices_is_allowed() -> None:
    checker = DataQualityChecker()
    event_a = make_event(device_id="machine-001")
    event_b = make_event(event_id="another-id",device_id="machine-002")
    # Give both events exactly the same timestamp.
    event_b = event_b.model_copy(update={"event_time": event_a.event_time})
    assert checker.evaluate(event_a) == []
    assert checker.evaluate(event_b) == []


def test_blank_event_id_is_detected() -> None:
    checker = DataQualityChecker()
    event = make_event(event_id="   ")
    issues = checker.evaluate(event)
    assert "missing_event_id" in issues
