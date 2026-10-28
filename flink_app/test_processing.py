import json
import pytest
from processing import detect_anomalies, evaluate_anomaly_rules, validate_record


def make_record(event_id: str = "evt-001", temperature: float = 20, power_kw: float = 50) -> dict:
    return {"event_id": event_id,
            "device_id": "machine-001",
            "event_time": "2026-10-23T10:00:00+00:00",
            "temperature": temperature,
            "power_kw": power_kw}


def test_valid_record_passes_quality_checks():
    record, issues = validate_record(json.dumps(make_record()))
    assert issues == []
    assert record is not None


def test_invalid_temperature_is_rejected():
    record, issues = validate_record(json.dumps(make_record(temperature=85)))
    assert record is None
    assert "out_of_range:temperature" in issues


def test_high_temperature_alerts_after_three_readings():
    state = {}
    alerts = []

    for index in range(3):
        record = make_record(event_id=f"evt-{index}", temperature=45)
        valid_record, issues = validate_record(json.dumps(record))
        assert issues == []
        assert valid_record is not None
        alerts.extend(detect_anomalies(valid_record, state))

    assert alerts == ["sustained_high_temperature"]


def test_temperature_at_threshold_does_not_trigger_alert():
    state = {}
    record = make_record(temperature=40)

    alerts = [alert for _ in range(4) for alert in detect_anomalies(record, state)]

    assert alerts == []
    assert state["machine-001"]["temperature_count"] == 0


def test_temperature_alert_fires_on_third_consecutive_reading_only():
    state = {}
    record = make_record(temperature=40.1)

    assert detect_anomalies(record, state) == []
    assert detect_anomalies(record, state) == []
    assert detect_anomalies(record, state) == ["sustained_high_temperature"]
    assert detect_anomalies(record, state) == []


def test_normal_reading_resets_consecutive_temperature_count():
    state = {}

    assert detect_anomalies(make_record(temperature=41), state) == []
    assert detect_anomalies(make_record(temperature=41), state) == []
    assert detect_anomalies(make_record(temperature=40), state) == []
    assert detect_anomalies(make_record(temperature=41), state) == []
    assert detect_anomalies(make_record(temperature=41), state) == []
    assert detect_anomalies(make_record(temperature=41), state) == ["sustained_high_temperature"]


def test_anomaly_clears_after_normal_reading():
    state = {}
    high_record = make_record(temperature=45)
    normal_record = make_record(temperature=20)

    for _ in range(3):
        detect_anomalies(high_record, state)

    assert detect_anomalies(normal_record, state) == []

    alerts = [alert  for _ in range(3)  for alert in detect_anomalies(high_record, state)]
    assert alerts == ["sustained_high_temperature"]


def test_devices_have_independent_state():
    state = {}
    first = make_record(temperature=45)
    second = {**first, "device_id": "machine-002"}
    detect_anomalies(first, state)
    detect_anomalies(first, state)

    assert detect_anomalies(second, state) == []
    assert detect_anomalies(first, state) == ["sustained_high_temperature"]



def test_shared_helper_detects_temperature_and_power_independently():
    record = make_record(temperature=45, power_kw=80)
    state = {}
    alerts = []

    for _ in range(3):
        state, new_alerts = evaluate_anomaly_rules(record, state)
        alerts.extend(new_alerts)

    assert sorted(alerts) == sorted(["sustained_high_temperature", "sustained_high_power_consumption"])


def test_shared_helper_supports_different_reading_requirements():
    record = make_record(temperature=45, power_kw=80)
    state = {}
    alerts = []

    for _ in range(3):
        state, new_alerts = evaluate_anomaly_rules(record, state, required_temperature_readings=2, required_power_readings=3)
        alerts.extend(new_alerts)

    assert sorted(alerts) == sorted(["sustained_high_temperature", "sustained_high_power_consumption"])


def test_shared_helper_does_not_mutate_input_state():
    record = make_record(temperature=45)
    original_state = {"temperature_count": 1,
                      "temperature_alert": False,
                      "power_count": 0,
                      "power_alert": False}

    new_state, alerts = evaluate_anomaly_rules(record, original_state)

    assert original_state["temperature_count"] == 1
    assert new_state["temperature_count"] == 2
    assert alerts == []


@pytest.mark.parametrize("threshold,value", [(40.0, 40.0), (70.0, 70.0)])
def test_reading_equal_to_sensor_threshold_is_not_high(threshold, value):
    record = make_record(temperature=20, power_kw=50)
    if threshold == 40.0:
        record["temperature"] = value
        state_key = "temperature_count"
    else:
        record["power_kw"] = value
        state_key = "power_count"
    state = {}

    alerts = [alert for _ in range(4) for alert in detect_anomalies(record, state)]

    assert alerts == []
    assert state["machine-001"][state_key] == 0
