from pathlib import Path

import duckdb


def _query_parquet(query: str, parquet_path: str | Path) -> list[dict[str, object]]:
    """Run a SQL query against a Parquet file and return dictionaries."""
    path = Path(parquet_path)
    if not path.is_file():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    with duckdb.connect() as connection:
        cursor = connection.execute(query, [str(path)])
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(columns, row)) for row in rows]


def get_status_summary(parquet_path: str | Path) -> list[dict[str, object]]:
    """Count processed events by status."""
    query = """
        SELECT
            status,
            COUNT(*) AS event_count
        FROM read_parquet(?)
        GROUP BY status
        ORDER BY status
    """
    return _query_parquet(query, parquet_path)


def get_anomalies_by_device(parquet_path: str | Path) -> list[dict[str, object]]:
    """Count anomalous events for each device."""
    query = """
        SELECT
            device_id,
            COUNT(*) AS anomaly_count
        FROM read_parquet(?)
        WHERE status = 'anomaly'
        GROUP BY device_id
        ORDER BY anomaly_count DESC, device_id
    """
    return _query_parquet(query, parquet_path)


def get_quality_issue_summary(parquet_path: str | Path) -> list[dict[str, object]]:
    """Count occurrences of each data quality issue."""
    query = """
        SELECT
            issue,
            COUNT(*) AS issue_count
        FROM read_parquet(?)
        CROSS JOIN UNNEST(quality_issues) AS issues(issue)
        GROUP BY issue
        ORDER BY issue
    """
    return _query_parquet(query, parquet_path)


def get_hourly_anomaly_counts(telemetry_path: str | Path, results_path: str | Path) -> list[dict[str, object]]:
    """Count anomalous events by hour, using each event's timestamp."""
    telemetry_file = Path(telemetry_path)
    results_file = Path(results_path)

    for path in (telemetry_file, results_file):
        if not path.is_file():
            raise FileNotFoundError(f"Parquet file not found: {path}")

    query = """
        SELECT
            date_trunc('hour', t.event_time) AS hour,
            COUNT(*) AS anomaly_count
        FROM read_parquet(?) AS t
        INNER JOIN read_parquet(?) AS r
            ON t.event_id = r.event_id
        WHERE r.status = 'anomaly'
        GROUP BY date_trunc('hour', t.event_time)
        ORDER BY hour
    """

    with duckdb.connect() as connection:
        cursor = connection.execute(query, [str(telemetry_file), str(results_file)])
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def get_averages_by_device(telemetry_path: str | Path) -> list[dict[str, object]]:
    """Calculate average temperature and event count for each device."""
    query = """
        SELECT
            device_id,
            AVG(temperature) AS average_temperature,
            AVG(power_kw) AS average_power_kw,
            COUNT(*) AS event_count
        FROM read_parquet(?)
        GROUP BY device_id
        ORDER BY average_temperature DESC, average_power_kw DESC, device_id
    """

    return _query_parquet(query, telemetry_path)
