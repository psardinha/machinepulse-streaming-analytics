
from machinepulse.anomaly_detection import AnomalyDetector
from machinepulse.processor import process_event, process_stream
from machinepulse.quality import DataQualityChecker
from machinepulse.schemas import TelemetryEvent
from machinepulse.streaming import LocalStream


def test_normal_event_has_no_anomalies():
    event = TelemetryEvent.create_example(temperature=23)
    detector = AnomalyDetector(required_consecutive_readings=1)
    quality = DataQualityChecker()
    result = process_event(event, detector, quality)
    assert result.status == "ok"
    assert result.anomalies == []


def test_high_temperature_is_detected():
    event = TelemetryEvent.create_example(temperature=43,)
    detector = AnomalyDetector(required_consecutive_readings=1)
    quality = DataQualityChecker()
    result = process_event(event, detector, quality)
    assert result.status == "anomaly"
    assert "sustained_high_temperature" in result.anomalies


def test_stream_is_consumed_and_processed():
    stream = LocalStream()
    stream.publish(TelemetryEvent.create_example())
    results = process_stream(stream, batch_size=10)
    assert len(results) == 1
    assert len(stream) == 0


def make_event(event_id: str, temperature: float) -> TelemetryEvent:
    event = TelemetryEvent.create_example(temperature=temperature)
    return event.model_copy(update={"event_id": event_id})


def test_processor_reports_quality_issues() -> None:
    detector = AnomalyDetector(required_consecutive_readings=1)
    checker = DataQualityChecker()
    event = make_event("evt-001", 70)
    process_event(event, detector, checker)
    result = process_event(event, detector, checker)
    assert result.status == "quality_issue"
    assert "duplicate_event_id" in result.quality_issues
    assert result.anomalies == []


def test_processor_still_detects_anomalies() -> None:
    detector = AnomalyDetector(temperature_threshold=65.0, required_consecutive_readings=1)
    checker = DataQualityChecker()
    event = make_event("evt-001", 70.0)
    result = process_event(event, detector, checker)
    assert result.status == "anomaly"
    assert result.anomalies == ["sustained_high_temperature"]
    assert result.quality_issues == []
