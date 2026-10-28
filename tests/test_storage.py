
import pyarrow.parquet as pq

from machinepulse.processor import ProcessingResult
from machinepulse.schemas import TelemetryEvent
from machinepulse.storage import write_processing_results, write_telemetry


def test_write_and_read_telemetry(tmp_path) -> None:
    event = TelemetryEvent.create_example()
    path = tmp_path / "telemetry.parquet"
    write_telemetry([event], path)

    table = pq.read_table(path)
    assert table.num_rows == 1
    assert table.column("event_id")[0].as_py() == event.event_id
    assert table.column("device_id")[0].as_py() == event.device_id
    assert table.column("temperature")[0].as_py() == event.temperature


def test_write_and_read_processing_results(tmp_path) -> None:
    result = ProcessingResult(event_id="evt-001",
                              device_id="machine-001",
                              status="anomaly",
                              anomalies=["sustained_high_temperature"],
                              quality_issues=[])
    path = tmp_path / "results.parquet"
    write_processing_results([result], path)

    table = pq.read_table(path)
    assert table.num_rows == 1
    assert table.column("status")[0].as_py() == "anomaly"
    assert table.column("anomalies")[0].as_py() == ["sustained_high_temperature"]
    assert table.column("quality_issues")[0].as_py() == []


def test_empty_telemetry_batch_preserves_schema(tmp_path) -> None:
    path = tmp_path / "empty.parquet"
    write_telemetry([], path)

    table = pq.read_table(path)
    assert table.num_rows == 0
    assert "event_id" in table.column_names
    assert "event_time" in table.column_names
