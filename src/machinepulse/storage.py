from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from machinepulse.processor import ProcessingResult
from machinepulse.schemas import TelemetryEvent

TELEMETRY_SCHEMA = pa.schema([pa.field("event_id", pa.string()),
                              pa.field("device_id", pa.string()),
                              pa.field("event_time", pa.timestamp("us", tz="UTC")),
                              pa.field("temperature", pa.float64()),
                              pa.field("power_kw", pa.float64())])

RESULT_SCHEMA = pa.schema([pa.field("event_id", pa.string()),
                           pa.field("device_id", pa.string()),
                           pa.field("status", pa.string()),
                           pa.field("anomalies", pa.list_(pa.string())),
                           pa.field("quality_issues", pa.list_(pa.string()))])


def write_telemetry(events: list[TelemetryEvent], output_path: str | Path) -> Path:
    """Write telemetry events to a Parquet file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = [event.model_dump(mode="python") for event in events]
    table = pa.Table.from_pylist(rows, schema=TELEMETRY_SCHEMA)
    pq.write_table(table, path)
    return path


def write_processing_results(results: list[ProcessingResult], output_path: str | Path) -> Path:
    """Write processing results to a Parquet file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = [{"event_id": result.event_id,
             "device_id": result.device_id,
             "status": result.status,
             "anomalies": result.anomalies,
             "quality_issues": result.quality_issues} for result in results]
    table = pa.Table.from_pylist(rows, schema=RESULT_SCHEMA)
    pq.write_table(table, path)
    return path