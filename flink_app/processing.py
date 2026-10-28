from __future__ import annotations

import json
from datetime import datetime
from typing import Any

TEMPERATURE_MIN = -10.0
TEMPERATURE_MAX = 70.0
POWER_KW_MIN = 0.0
POWER_KW_MAX = 100_000.0

TEMPERATURE_THRESHOLD = 40.0
POWER_KW_THRESHOLD = 70.0
REQUIRED_CONSECUTIVE_READINGS = 3


def validate_record(raw: str) -> tuple[dict[str, Any] | None, list[str]]:
    """Parse JSON telemetry and report data-quality issues."""
    try:
        record = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None, ["invalid_json"]

    if not isinstance(record, dict):
        return None, ["record_not_object"]

    required = {"event_id",
                "device_id",
                "event_time",
                "temperature",
                "power_kw"}

    issues: list[str] = []

    missing = required - record.keys()
    extra = record.keys() - required

    issues.extend(f"missing_field:{name}" for name in sorted(missing))
    issues.extend(f"unexpected_field:{name}" for name in sorted(extra))

    if issues:
        return None, issues

    for name in ("event_id", "device_id"):
        if not isinstance(record[name], str) or not record[name].strip():
            issues.append(f"invalid_field:{name}")

    timestamp = record["event_time"]

    if not isinstance(timestamp, str):
        issues.append("invalid_field:event_time")
    else:
        try:
            parsed_time = datetime.fromisoformat(timestamp)
            if parsed_time.tzinfo is None:
                issues.append("timestamp_missing_timezone")
        except ValueError:
            issues.append("invalid_field:event_time")

    for name, minimum, maximum in (("temperature", TEMPERATURE_MIN, TEMPERATURE_MAX),
                                   ("power_kw", POWER_KW_MIN, POWER_KW_MAX)):
        value = record[name]

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            issues.append(f"invalid_field:{name}")
        elif not minimum <= value <= maximum:
            issues.append(f"out_of_range:{name}")

    if issues:
        return None, issues

    return record, []


def evaluate_anomaly_rules(record: dict[str, Any],
                           current_state: dict[str, int | bool],
                           *,
                           temperature_threshold: float = TEMPERATURE_THRESHOLD,
                           power_kw_threshold: float = POWER_KW_THRESHOLD,
                           required_temperature_readings: int = REQUIRED_CONSECUTIVE_READINGS,
                           required_power_readings: int = REQUIRED_CONSECUTIVE_READINGS) -> tuple[dict[str, int | bool], list[str]]:
    """Apply anomaly rules without mutating the supplied state."""
    if required_temperature_readings < 1 or required_power_readings < 1:
        raise ValueError("Required consecutive readings must be positive.")

    new_state: dict[str, int | bool] = {"temperature_count": int(current_state.get("temperature_count", 0)),
                                        "temperature_alert": bool(current_state.get("temperature_alert", False)),
                                        "power_count": int(current_state.get("power_count", 0)),
                                        "power_alert": bool(current_state.get("power_alert", False))}

    alerts: list[str] = []

    sensors = ((float(record["temperature"]),
                temperature_threshold,
                required_temperature_readings,
                "temperature_count",
                "temperature_alert",
                "sustained_high_temperature"),
               (float(record["power_kw"]),
                power_kw_threshold,
                required_power_readings,
                "power_count",
                "power_alert",
                "sustained_high_power_consumption"))

    for value, threshold, required_count, count_key, alert_key, alert_name  in sensors:
        if value > threshold:
            new_state[count_key] = int(new_state[count_key]) + 1
        else:
            new_state[count_key] = 0
            new_state[alert_key] = False

        if int(new_state[count_key]) >= required_count  and  not bool(new_state[alert_key]):
            alerts.append(alert_name)
            new_state[alert_key] = True

    return new_state, alerts


def detect_anomalies(record: dict[str, Any], state: dict[str, dict[str, int | bool]]) -> list[str]:
    """Maintain per-device state for the local pipeline."""
    device_id = record["device_id"]
    current_state = state.get(device_id, {})

    new_state, alerts = evaluate_anomaly_rules(record, current_state)
    state[device_id] = new_state

    return alerts
