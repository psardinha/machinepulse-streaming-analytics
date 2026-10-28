
from __future__ import annotations

import json
import time
from typing import Any

from processing import validate_record, detect_anomalies
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig, ValueStateDescriptor

class TelemetryValidator(ProcessFunction):
    """Tag valid telemetry and data-quality failures for downstream routing."""

    def process_element(self, value: str, ctx):
        record, issues = validate_record(value)

        if issues:
            yield json.dumps({"output_type": "quality",
                              "value": {"raw_record": value, 
                                        "issues": issues}},
                             separators=(",", ":"))
            return

        assert record is not None
        yield json.dumps({"output_type": "valid", "value": record}, separators=(",", ":"))


class MachinePulseProcessor(KeyedProcessFunction):
    """Detect anomalies with Flink-managed, per-device state.
       Input records must already have passed TelemetryValidator.
    """

    def __init__(self,
                 deduplication_ttl_minutes: int = 60,
                 temperature_threshold: float = 40.0,
                 power_kw_threshold: float = 70.0,
                 required_temperature_readings: int = 3,
                 required_power_readings: int = 3):
        if deduplication_ttl_minutes < 1:
            raise ValueError("Deduplication TTL must be positive.")

        if required_temperature_readings < 1 or required_power_readings < 1:
            raise ValueError("Required consecutive readings must be positive.")

        self.deduplication_ttl_minutes = deduplication_ttl_minutes
        self.temperature_threshold = temperature_threshold
        self.power_kw_threshold = power_kw_threshold
        self.required_temperature_readings = required_temperature_readings
        self.required_power_readings = required_power_readings

    def open(self, runtime_context: RuntimeContext) -> None:
        seen_descriptor = MapStateDescriptor(
            "seen_event_expirations",
            Types.STRING(),
            Types.LONG(),
        )

        ttl_config = (StateTtlConfig.new_builder(Time.minutes(self.deduplication_ttl_minutes)).
                                     set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite).
                                     disable_cleanup_in_background().
                                     build())
        seen_descriptor.enable_time_to_live(ttl_config)

        self.seen_event_expirations = runtime_context.get_map_state(seen_descriptor)

        self.temperature_count = runtime_context.get_state(ValueStateDescriptor("temperature_count", Types.INT()))
        self.temperature_alert = runtime_context.get_state(ValueStateDescriptor("temperature_alert", Types.BOOLEAN()))
        self.power_count = runtime_context.get_state(ValueStateDescriptor("power_count", Types.INT()))
        self.power_alert = runtime_context.get_state(ValueStateDescriptor("power_alert", Types.BOOLEAN()))

    def process_element(self, value: str, ctx):
        record: dict[str, Any] = json.loads(value)
        event_id = record["event_id"]
        now_ms = time.time_ns() // 1_000_000
        expires_at_ms = self.seen_event_expirations.get(event_id)

        # This state is scoped to the device key, not globally.
        if expires_at_ms is not None and now_ms < expires_at_ms:
            yield json.dumps({"output_type": "quality",
                              "value": {"event_id": event_id,
                                        "device_id": record["device_id"],
                                        "issues": ["duplicate_event_id"],
                                        "record": record}},
                             separators=(",", ":"))
            return

        self.seen_event_expirations.put(event_id, now_ms + self.deduplication_ttl_minutes * 60_000)
        device_id = record["device_id"]

        # Adapt Flink-managed state to the existing detect_anomalies API.
        local_state: dict[str, dict[str, int | bool]] = {device_id: {"temperature_count": self.temperature_count.value() or 0,
                                                                     "temperature_alert": self.temperature_alert.value() or False,
                                                                     "power_count": self.power_count.value() or 0,
                                                                     "power_alert": self.power_alert.value() or False}}

        anomalies = detect_anomalies(record, local_state)

        # Persist the updated state through Flink.
        new_state = local_state[device_id]
        self.temperature_count.update(new_state["temperature_count"])
        self.temperature_alert.update(new_state["temperature_alert"])
        self.power_count.update(new_state["power_count"])
        self.power_alert.update(new_state["power_alert"])

        yield json.dumps({"output_type": "processed", "value": record}, separators=(",", ":"))

        for anomaly in anomalies:
            yield json.dumps({"output_type": "anomaly",
                              "value": {"event_id": event_id,
                                        "device_id": record["device_id"],
                                        "event_time": record["event_time"],
                                        "anomaly_type": anomaly,
                                        "temperature": float(record["temperature"]),
                                        "power_kw": float(record["power_kw"])}},
                             separators=(",", ":"))
