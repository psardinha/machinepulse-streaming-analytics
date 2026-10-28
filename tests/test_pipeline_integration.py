from machinepulse.analytics import get_averages_by_device, get_status_summary
from machinepulse.processor import process_stream
from machinepulse.simulator import generate_events
from machinepulse.storage import write_processing_results, write_telemetry
from machinepulse.streaming import LocalStream


def test_pipeline_end_to_end(tmp_path) -> None:
    # 1. Generate simulated telemetry.
    events = generate_events(count=100, device_count=10, seed=42)

    # 2. Publish every event to the local stream.
    stream = LocalStream()
    for event in events:
        stream.publish(event)

    assert len(stream) == 100

    # 3. Process the stream.
    results = process_stream(stream, batch_size=20)

    assert len(results) == 100
    assert len(stream) == 0

    # 4. Save telemetry and results to temporary Parquet files.
    telemetry_path = tmp_path / "telemetry.parquet"
    results_path = tmp_path / "processing_results.parquet"

    write_telemetry(events, telemetry_path)
    write_processing_results(results, results_path)

    assert telemetry_path.is_file()
    assert results_path.is_file()

    # 5. Query the saved results with DuckDB.
    status_summary = get_status_summary(results_path)
    averages = get_averages_by_device(telemetry_path)

    # Every processed event should have exactly one status.
    assert sum(row["event_count"] for row in status_summary) == 100

    # Every simulated device should appear in the temperature analysis.
    assert len(averages) == 10
    assert all(row["event_count"] == 10 for row in averages)
