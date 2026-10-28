import pytest
from pydantic import ValidationError

from machinepulse.schemas import TelemetryEvent
from machinepulse.simulator import generate_events
from machinepulse.streaming import LocalStream


def test_simulator_generates_valid_unique_events():
    events = generate_events(count=100, seed=42)

    assert len(events) == 100
    assert len({event.event_id for event in events}) == 100
    assert all(event.temperature < 65 for event in events)


def test_invalid_temperature_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryEvent.create_example(temperature=71)


def test_local_stream_consumes_batches():
    stream = LocalStream()
    events = generate_events(count=5)

    for event in events:
        stream.publish(event)

    assert len(stream.consume_batch(3)) == 3
    assert len(stream) == 2
    assert len(stream.consume_batch(10)) == 2
