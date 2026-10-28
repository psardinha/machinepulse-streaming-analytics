from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from machinepulse.analytics import get_averages_by_device, get_hourly_anomaly_counts


def test_average_temperature_by_device(tmp_path) -> None:
    path = tmp_path / "telemetry.parquet"

    table = pa.table({"device_id": ["machine-001", "machine-001", "machine-002"],
                      "temperature": [40.0, 60.0, 50.0],
                      "power_kw": [20, 30, 40]})
    pq.write_table(table, path)
    result = get_averages_by_device(path)
    assert sorted(result, key=lambda x: x["device_id"]) == sorted([{"device_id": "machine-002",
                                                                    "average_temperature": 50.0,
                                                                    "average_power_kw": 40,
                                                                    "event_count": 1},
                                                                   {"device_id": "machine-001",
                                                                    "average_temperature": 50.0,
                                                                    "average_power_kw": 25,
                                                                    "event_count": 2}], 
                                                                  key=lambda x: x["device_id"])


def test_hourly_anomaly_counts(tmp_path) -> None:
    telemetry_path = tmp_path / "telemetry.parquet"
    results_path = tmp_path / "results.parquet"

    hour_10 = datetime(2026, 10, 22, 10, 5, tzinfo=UTC)
    hour_11 = datetime(2026, 10, 22, 11, 15, tzinfo=UTC)
    telemetry = pa.table({"event_id": ["evt-001", "evt-002", "evt-003"],
                          "event_time": pa.array([hour_10, hour_10.replace(minute=45), hour_11],
                                                 type=pa.timestamp("us", tz="UTC"))})
    results = pa.table({"event_id": ["evt-001", "evt-002", "evt-003"],
                        "status": ["anomaly", "anomaly", "ok"]})
    pq.write_table(telemetry, telemetry_path)
    pq.write_table(results, results_path)

    summary = get_hourly_anomaly_counts(telemetry_path, results_path)

    assert len(summary) == 1
    assert summary[0]["hour"] == hour_10.replace(minute=0, second=0, microsecond=0)
    assert summary[0]["anomaly_count"] == 2
