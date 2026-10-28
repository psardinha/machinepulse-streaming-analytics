import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest


class FakeMapState:
    def __init__(self, runtime_context):
        self.runtime_context = runtime_context
        self.values = {}

    def get(self, key):
        device_values = self.values.get(self.runtime_context.current_key, {})
        return device_values.get(key)

    def put(self, key, value):
        device_values = self.values.setdefault(self.runtime_context.current_key, {})
        device_values[key] = value


class FakeValueState:
    def __init__(self):
        self.current_value = None

    def value(self):
        return self.current_value

    def update(self, value):
        self.current_value = value


class FakeRuntimeContext:
    def __init__(self):
        self.current_key = None
        self.map_state = FakeMapState(self)
        self.value_states = {}

    def get_map_state(self, descriptor):
        return self.map_state

    def get_state(self, descriptor):
        return self.value_states.setdefault(descriptor.name, FakeValueState())


class FakeMapStateDescriptor:
    def __init__(self, name, key_type, value_type):
        self.name = name

    def enable_time_to_live(self, ttl_config):
        self.ttl_config = ttl_config


class FakeValueStateDescriptor:
    def __init__(self, name, value_type):
        self.name = name


class FakeTtlConfigBuilder:
    def set_update_type(self, update_type):
        return self

    def disable_cleanup_in_background(self):
        return self

    def build(self):
        return object()


class FakeStateTtlConfig:
    UpdateType = type("UpdateType", (), {"OnCreateAndWrite": object()})

    @staticmethod
    def new_builder(ttl):
        return FakeTtlConfigBuilder()


class FakeTime:
    @staticmethod
    def minutes(value):
        return value * 60_000


class FakeTypes:
    STRING = staticmethod(lambda: "string")
    BOOLEAN = staticmethod(lambda: "boolean")
    LONG = staticmethod(lambda: "long")
    INT = staticmethod(lambda: "int")


class FakeProcessFunction:
    pass


class FakeKeyedProcessFunction:
    pass


def load_processor_with_pyflink_stubs(monkeypatch):
    pyflink_module = ModuleType("pyflink")
    common_module = ModuleType("pyflink.common")
    common_module.Time = FakeTime
    typeinfo_module = ModuleType("pyflink.common.typeinfo")
    typeinfo_module.Types = FakeTypes
    functions_module = ModuleType("pyflink.datastream.functions")
    functions_module.KeyedProcessFunction = FakeKeyedProcessFunction
    functions_module.ProcessFunction = FakeProcessFunction
    functions_module.RuntimeContext = object
    state_module = ModuleType("pyflink.datastream.state")
    state_module.MapStateDescriptor = FakeMapStateDescriptor
    state_module.StateTtlConfig = FakeStateTtlConfig
    state_module.ValueStateDescriptor = FakeValueStateDescriptor

    modules = {"pyflink": pyflink_module,
               "pyflink.common": common_module,
               "pyflink.common.typeinfo": typeinfo_module,
               "pyflink.datastream": ModuleType("pyflink.datastream"),
               "pyflink.datastream.functions": functions_module,
               "pyflink.datastream.state": state_module}
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    module_path = Path(__file__).with_name("flink_processor.py")
    spec = importlib.util.spec_from_file_location("flink_processor_under_test", module_path)
    assert spec is not None
    assert spec.loader is not None
    processor_module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "flink_processor_under_test", processor_module)
    spec.loader.exec_module(processor_module)
    return processor_module


def output_records(processor, record, clock_ms):
    processor_module, instance, runtime_context = processor
    runtime_context.current_key = record["device_id"]
    monkeypatch_time_ns = processor_module.time.time_ns
    processor_module.time.time_ns = lambda: clock_ms * 1_000_000
    try:
        return [json.loads(value) for value in instance.process_element(json.dumps(record), ctx=None)]
    finally:
        processor_module.time.time_ns = monkeypatch_time_ns


@pytest.fixture
def processor(monkeypatch):
    processor_module = load_processor_with_pyflink_stubs(monkeypatch)
    instance = processor_module.MachinePulseProcessor(deduplication_ttl_minutes=5)
    runtime_context = FakeRuntimeContext()
    instance.open(runtime_context)
    return processor_module, instance, runtime_context


def make_record(event_id="ev1", device_id="device-1"):
    return {"event_id": event_id,
            "device_id": device_id,
            "event_time": "2026-10-27T10:00:00+00:00",
            "temperature": 20.0,
            "power_kw": 50.0}


def test_same_event_id_is_duplicate_inside_ttl(processor):
    first = output_records(processor, make_record(), 1_000)
    duplicate = output_records(processor, make_record(), 2_000)

    assert [record["output_type"] for record in first] == ["processed"]
    assert [record["output_type"] for record in duplicate] == ["quality"]
    assert duplicate[0]["value"]["issues"] == ["duplicate_event_id"]


def test_duplicate_does_not_extend_ttl_and_id_is_accepted_after_five_minutes(processor):
    output_records(processor, make_record(), 1_000)
    duplicate = output_records(processor, make_record(), 2_000)
    after_ttl = output_records(processor, make_record(), 481_000)
    assert duplicate[0]["output_type"] == "quality"
    assert [record["output_type"] for record in after_ttl] == ["processed"]


def test_event_id_can_be_reused_exactly_at_expiration(processor):
    output_records(processor, make_record(), 1_000)
    at_expiration = output_records(processor, make_record(), 301_000)
    assert [record["output_type"] for record in at_expiration] == ["processed"]


def test_same_event_id_is_tracked_independently_per_device(processor):
    output_records(processor, make_record(device_id="device-1"), 1_000)
    other_device = output_records(processor, make_record(device_id="device-2"), 2_000)
    assert [record["output_type"] for record in other_device] == ["processed"]
