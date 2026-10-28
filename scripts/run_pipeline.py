

from machinepulse.analytics import (
    get_anomalies_by_device,
    get_averages_by_device,
    get_hourly_anomaly_counts,
    get_quality_issue_summary,
    get_status_summary,
)
from machinepulse.processor import process_stream
from machinepulse.simulator import generate_events
from machinepulse.storage import write_processing_results, write_telemetry
from machinepulse.streaming import LocalStream


def main() -> None:
    event_count = 100
    batch_size = 20

    print("MachinePulse local pipeline")
    print("-" * 35)

    # 1. Generate telemetry.
    events = generate_events(count=event_count, seed=42)
    print(f"Generated events: {len(events)}")

    # 2. Publish telemetry.
    stream = LocalStream()

    for event in events:
        stream.publish(event)

    print(f"Published events: {len(events)}")

    # 3. Consume and process telemetry.
    results = process_stream(stream, batch_size=batch_size)

    telemetry_path = write_telemetry(events, "data/telemetry.parquet")
    results_path = write_processing_results(results, "data/processing_results.parquet")
    print(f"Telemetry saved to: {telemetry_path}")
    print(f"Processing results saved to: {results_path}")

    # 4. Summarize processing results.
    print("\nHourly anomaly counts:")
    for row in get_hourly_anomaly_counts("data/telemetry.parquet", "data/processing_results.parquet"):
        print(f"  {row['hour']}: {row['anomaly_count']}")

    print("\nAverage temperature by device:")
    for row in get_averages_by_device("data/telemetry.parquet"):
        print(f"  {row['device_id']}: {row['average_temperature']:.1f} °C/{row['average_power_kw']:.0f} kw," 
              f"({row['event_count']} events)")

    print("\nEvent status summary:")
    for row in get_status_summary(results_path):
        print(f"  {row['status']}: {row['event_count']}")

    print("\nAnomalies by device:")
    for row in get_anomalies_by_device(results_path):
        print(f"  {row['device_id']}: {row['anomaly_count']}")

    print("\nData quality issues:")
    for row in get_quality_issue_summary(results_path):
        print(f"  {row['issue']}: {row['issue_count']}")


if __name__ == "__main__":
    main()
