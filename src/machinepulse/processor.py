
from dataclasses import dataclass, field

from machinepulse.anomaly_detection import AnomalyDetector
from machinepulse.quality import DataQualityChecker
from machinepulse.schemas import TelemetryEvent
from machinepulse.streaming import LocalStream


@dataclass
class ProcessingResult:
    event_id: str
    device_id: str
    status: str
    anomalies: list[str] = field(default_factory=list)
    quality_issues: list[str] = field(default_factory=list)


def process_event(event: TelemetryEvent, detector: AnomalyDetector, quality_checker: DataQualityChecker) -> ProcessingResult:
    quality_issues = quality_checker.evaluate(event)

    # Do not let questionable events change detector state.
    if quality_issues:
        anomalies: list[str] = []
    else:
        anomalies = detector.evaluate(event)
    if quality_issues:
        status = "quality_issue"
    elif anomalies:
        status = "anomaly"
    else:
        status = "ok"

    return ProcessingResult(event_id=event.event_id,
                            device_id=event.device_id,
                            status=status,
                            anomalies=anomalies,
                            quality_issues=quality_issues)


def process_stream(stream: LocalStream, 
                   batch_size: int = 20, 
                   detector: AnomalyDetector | None = None,
                   quality_checker: DataQualityChecker | None = None) -> list[ProcessingResult]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    # One detector instance must be retained across batches.
    if detector is None:
        detector = AnomalyDetector()

    if quality_checker is None:
        quality_checker = DataQualityChecker()

    results: list[ProcessingResult] = []

    while len(stream) > 0:
        batch = stream.consume_batch(max_records=batch_size)

        for event in batch:
            results.append(process_event(event, detector, quality_checker))

    return results
