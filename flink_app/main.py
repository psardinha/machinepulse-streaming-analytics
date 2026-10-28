from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from pyflink.common import Configuration, Row, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from kinesis_connector import KinesisStreamsSource
from pyflink.table import StreamTableEnvironment

from flink_processor import MachinePulseProcessor, TelemetryValidator


def load_properties() -> dict[str, str]:
    properties_path = "/etc/flink/application_properties.json"
    groups: dict[str, dict[str, str]] = {}

    if os.path.isfile(properties_path):
        with open(properties_path, encoding="utf-8") as file:
            for group in json.load(file):
                groups[group.get("PropertyGroupId", "")] = group.get("PropertyMap", {})

    config = groups.get("MachinePulseConfig", {})

    return {"input_stream_arn": config.get("INPUT_STREAM_ARN") or os.getenv("INPUT_STREAM_ARN", ""),
            "aws_region": config.get("AWS_REGION") or os.getenv("AWS_REGION", "eu-west-1"),
            "output_bucket": config.get("OUTPUT_BUCKET") or os.getenv("OUTPUT_BUCKET", ""),
            "bronze_prefix": config.get("OUTPUT_BRONZE_PREFIX", "bronze/"),
            "silver_prefix": config.get("OUTPUT_SILVER_PREFIX", "silver/"),
            "gold_prefix": config.get("OUTPUT_GOLD_PREFIX", "gold/"),
            "quality_prefix": config.get("OUTPUT_QUALITY_PREFIX", "quality/")}


def parse_json(value: str) -> dict[str, Any]:
    result = json.loads(value)
    if not isinstance(result, dict):
        raise ValueError("Expected a JSON object")
    return result


def has_output_type(value: str, output_type: str) -> bool:
    return parse_json(value)["output_type"] == output_type


def unwrap_output(value: str) -> str:
    return json.dumps(parse_json(value)["value"], separators=(",", ":"))


def bronze_row(value: str) -> Row:
    return Row(raw_payload=value, ingested_at_utc=datetime.now(timezone.utc).isoformat())


class DeviceKpiAggregate(AggregateFunction):
    """Accumulates:
        - Number of records,
        - Sum of temperatures reported
        - Maximum of temperatures reported
        - Sum of power consumptions reported
        - Maximum of power consumptions reported
    """
    def create_accumulator(self) -> tuple[int, float, float, float, float]:
        return 0, 0.0, float("-inf"), 0.0, float("-inf")

    def add(self, value: str, accumulator: tuple[int, float, float, float, float]) -> tuple[int, float, float, float, float]:
        record = parse_json(value)
        temperature = float(record["temperature"])
        power_kw = float(record["power_kw"])
        return (accumulator[0] + 1,
                accumulator[1] + temperature,
                max(accumulator[2], temperature),
                accumulator[3] + power_kw,
                max(accumulator[4], power_kw))

    def get_result(self, accumulator: tuple[int, float, float, float, float]) -> tuple[int, float, float, float, float]:
        return accumulator

    def merge(self,
              left: tuple[int, float, float, float, float],
              right: tuple[int, float, float, float, float]) -> tuple[int, float, float, float, float]:
        return (left[0] + right[0],
                left[1] + right[1],
                max(left[2], right[2]),
                left[3] + right[3],
                max(left[4], right[4]))


class DeviceKpiWindow(ProcessWindowFunction):
    """Issue window KPI measured"""
    def process(self, key: str, context, elements: Iterable[tuple[int, float, float, float, float]]):
        for count, temperature_sum, temperature_max, power_sum, power_max in elements:
            window = context.window()
            yield Row(device_id=key,
                      window_start_utc=datetime.fromtimestamp(window.start / 1000, tz=timezone.utc).isoformat(),
                      window_end_utc=datetime.fromtimestamp(window.end / 1000, tz=timezone.utc).isoformat(),
                      reading_count=count,
                      average_temperature=temperature_sum / count,
                      peak_temperature=temperature_max,
                      average_power_kw=power_sum / count,
                      peak_power_kw=power_max)


def telemetry_row(value: str) -> Row:
    record = parse_json(value)
    return Row(event_id=str(record.get("event_id", "")),
               device_id=str(record.get("device_id", "")),
               event_time=str(record.get("event_time", "")),
               temperature=float(record.get("temperature", 0)),
               power_kw=float(record.get("power_kw", 0)),
               payload_json=json.dumps(record, separators=(",", ":")))


def anomaly_row(value: str) -> Row:
    record = parse_json(value)
    return Row(event_id=str(record.get("event_id", "")),
               device_id=str(record.get("device_id", "")),
               event_time=str(record.get("event_time", "")),
               anomaly_type=str(record.get("anomaly_type", "")),
               temperature=float(record.get("temperature", 0)),
               power_kw=float(record.get("power_kw", 0)),
               payload_json=json.dumps(record, separators=(",", ":")))


def quality_row(value: str) -> Row:
    record = parse_json(value)
    raw_record = record.get("raw_record", "")
    original_record = record.get("record", {})

    if isinstance(raw_record, (dict, list)):
        raw_record = json.dumps(raw_record, separators=(",", ":"))

    if not raw_record and original_record:
        raw_record = json.dumps(original_record, separators=(",", ":"))

    issues = record.get("issues", [])
    if not isinstance(issues, list):
        issues = [str(issues)]

    return Row(event_id=str(record.get("event_id", "")),
               device_id=str(record.get("device_id", "")),
               raw_record=str(raw_record),
               issues_json=json.dumps(issues, separators=(",", ":")),
               payload_json=json.dumps(record, separators=(",", ":")))


def main() -> None:
    properties = load_properties()

    stream_arn = properties["input_stream_arn"]
    aws_region = properties["aws_region"]
    bucket = properties["output_bucket"].strip()

    if not stream_arn:
        raise ValueError("INPUT_STREAM_ARN is required.")

    if not bucket:
        raise ValueError("OUTPUT_BUCKET is required.")

    for key in ("bronze_prefix", "silver_prefix", "gold_prefix", "quality_prefix"):
        properties[key] = properties[key].strip("/") + "/"

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60_000)

    source_config = Configuration()
    source_config.set_string("aws.region", aws_region)

    kinesis_source = (KinesisStreamsSource.builder().
                                           set_stream_arn(stream_arn).
                                           set_source_config(source_config).
                                           set_deserialization_schema(SimpleStringSchema()).
                                           build())

    raw_stream = env.from_source(source=kinesis_source,
                                 watermark_strategy=WatermarkStrategy.no_watermarks(),
                                 source_name="MachinePulse Kinesis source",
                                 type_info=Types.STRING()).uid("machinepulse-kinesis-source")

    validation_outputs = raw_stream.process(TelemetryValidator(), output_type=Types.STRING()).name("validate-telemetry")

    validated_stream = (validation_outputs.filter(lambda value: has_output_type(value, "valid")).
                                           map(unwrap_output, output_type=Types.STRING()))

    # validation_quality holds records with local field errors, e.g., an invalid timestamp, a field absent...
    validation_quality = (validation_outputs.filter(lambda value: has_output_type(value, "quality")).
                                             map(unwrap_output, output_type=Types.STRING()))

    keyed_stream = validated_stream.key_by(lambda raw: json.loads(raw)["device_id"], key_type=Types.STRING())

    processor_outputs = keyed_stream.process(MachinePulseProcessor(deduplication_ttl_minutes=5), 
                                             output_type=Types.STRING()).name("detect-anomalies")

    processed_stream = (processor_outputs.filter(lambda value: has_output_type(value, "processed")).
                                          map(unwrap_output, output_type=Types.STRING()))

    anomalies = (processor_outputs.filter(lambda value: has_output_type(value, "anomaly")).
                                   map(unwrap_output, output_type=Types.STRING()))

    # processing_quality contains inter event errors, e.g., a repeated eventID issued for the same device within 1 hour interval
    processing_quality = (processor_outputs.filter(lambda value: has_output_type(value, "quality")).
                                            map(unwrap_output, output_type=Types.STRING()))

    quality_stream = validation_quality.union(processing_quality)

    telemetry_type = Types.ROW_NAMED(["event_id", "device_id", "event_time", "temperature", "power_kw",  "payload_json"],
                                     [Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING()])

    anomaly_type = Types.ROW_NAMED(["event_id", "device_id", "event_time", "anomaly_type", "temperature",
                                    "power_kw", "payload_json"],
                                   [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), 
                                    Types.DOUBLE(),  Types.STRING()])

    device_kpi_type = Types.ROW_NAMED(["device_id", "window_start_utc", "window_end_utc", "reading_count", 
                                       "average_temperature", "peak_temperature", "average_power_kw", "peak_power_kw"],
                                      [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(),
                                       Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()])

    quality_type = Types.ROW_NAMED(["event_id", "device_id", "raw_record", "issues_json", "payload_json"],
                                   [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])

    bronze_type = Types.ROW_NAMED(["raw_payload", "ingested_at_utc"], [Types.STRING(), Types.STRING()])

    table_env = StreamTableEnvironment.create(env)

    table_env.create_temporary_view("raw_records",
                                    table_env.from_data_stream(raw_stream.map(bronze_row, output_type=bronze_type)))

    table_env.create_temporary_view("processed_records",
                                    table_env.from_data_stream(processed_stream.map(telemetry_row, output_type=telemetry_type)))

    table_env.create_temporary_view("anomaly_records",
                                    table_env.from_data_stream(anomalies.map(anomaly_row, output_type=anomaly_type)))

    device_kpis = (processed_stream.key_by(lambda raw: json.loads(raw)["device_id"], key_type=Types.STRING()).
                                    window(TumblingProcessingTimeWindows.of(Time.minutes(5))).
                                    aggregate(DeviceKpiAggregate(),
                                              DeviceKpiWindow(),
                                              accumulator_type=Types.TUPLE([Types.LONG(), Types.DOUBLE(), Types.DOUBLE(),
                                                                            Types.DOUBLE(), Types.DOUBLE()]),
                                              output_type=device_kpi_type))

    table_env.create_temporary_view("device_kpis_5m", table_env.from_data_stream(device_kpis))

    table_env.create_temporary_view("quality_records",
                                    table_env.from_data_stream(quality_stream.map(quality_row, output_type=quality_type)))

    sinks = [("machinepulse_bronze",
              properties["bronze_prefix"],
              """
              raw_payload STRING,
              ingested_at_utc STRING
              """,
              "raw_records"),
             ("machinepulse_silver",
              properties["silver_prefix"],
              """
              event_id STRING,
              device_id STRING,
              event_time STRING,
              temperature DOUBLE,
              power_kw DOUBLE,
              payload_json STRING
              """,
              "processed_records"),
             ("machinepulse_gold",
              properties["gold_prefix"] + "anomalies/",
              """
              event_id STRING,
              device_id STRING,
              event_time STRING,
              anomaly_type STRING,
              temperature DOUBLE,
              power_kw DOUBLE,
              payload_json STRING
              """,
              "anomaly_records"),
             ("machinepulse_device_kpis_5m",
              properties["gold_prefix"] + "device_kpis_5m/",
              """
              device_id STRING,
              window_start_utc STRING,
              window_end_utc STRING,
              reading_count BIGINT,
              average_temperature DOUBLE,
              peak_temperature DOUBLE,
              average_power_kw DOUBLE,
              peak_power_kw DOUBLE
              """,
              "device_kpis_5m"),
             ("machinepulse_quality",
              properties["quality_prefix"],
              """
              event_id STRING,
              device_id STRING,
              raw_record STRING,
              issues_json STRING,
              payload_json STRING
              """,
              "quality_records")]

    statement_set = table_env.create_statement_set()

    for sink_name, prefix, columns, view_name in sinks:
        # One-shot DDL operation
        table_env.execute_sql(f"""CREATE TABLE {sink_name} ({columns}) WITH (
                              'connector' = 'filesystem',
                              'path' = 's3://{bucket}/{prefix}',
                              'format' = 'parquet',
                              'parquet.compression' = 'GZIP')""")
        # One-shot registration for ongoing processing
        statement_set.add_insert_sql(f"INSERT INTO {sink_name} SELECT * FROM {view_name}")

    statement_set.execute()


if __name__ == "__main__":
    main()
