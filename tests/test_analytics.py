import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from machinepulse.analytics import (
    get_anomalies_by_device,
    get_quality_issue_summary,
    get_status_summary,
)


def create_results_file(tmp_path):
    path = tmp_path / "results.parquet"
    table = pa.table({
            "event_id": ["evt-001", "evt-002", "evt-003", "evt-004"],
            "device_id": ["machine-001", "machine-002", "machine-001", "machine-002"],
            "status": ["ok", "anomaly", "anomaly", "quality_issue"],
            "anomalies": [[], ["sustained_high_temperature"], ["sustained_high_temperature"], []],
            "quality_issues": [[], [], [], ["duplicate_event_id", "duplicate_device_timestamp"]]})
    pq.write_table(table, path)
    return path


def test_status_summary(tmp_path) -> None:
    path = create_results_file(tmp_path)
    summary = get_status_summary(path)
    assert summary == [{"status": "anomaly", "event_count": 2},
                       {"status": "ok", "event_count": 1},
                       {"status": "quality_issue", "event_count": 1}]


def test_anomalies_by_device(tmp_path) -> None:
    path = create_results_file(tmp_path)
    summary = get_anomalies_by_device(path)
    assert summary == [{"device_id": "machine-001", "anomaly_count": 1},
                       {"device_id": "machine-002", "anomaly_count": 1}]


def test_quality_issue_summary(tmp_path) -> None:
    path = create_results_file(tmp_path)
    summary = get_quality_issue_summary(path)
    assert summary == [{"issue": "duplicate_device_timestamp", "issue_count": 1},
                       {"issue": "duplicate_event_id", "issue_count": 1}]


def test_missing_parquet_file_raises_error(tmp_path) -> None:
    missing_path = tmp_path / "missing.parquet"
    with pytest.raises(FileNotFoundError):
        get_status_summary(missing_path)
