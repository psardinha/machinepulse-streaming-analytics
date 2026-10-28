
from dataclasses import dataclass, field

from machinepulse.schemas import TelemetryEvent


@dataclass
class DeviceState:
    consecutive_high_values: int = 0
    alert_active: bool = False


@dataclass
class AnomalyDetector:
    temperature_threshold: float = 40
    power_kw_threshold: float = 70
    required_consecutive_readings: int = 3
    _devices: dict[str, dict[str, DeviceState]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.required_consecutive_readings < 1:
            raise ValueError("required_consecutive_readings must be positive")

    def get_state(self, device_id: str, sensor_name: str) -> DeviceState:
        device_sensors = self._devices.setdefault(device_id, {})
        return device_sensors.setdefault(sensor_name, DeviceState())

    def evaluate(self, event: TelemetryEvent) -> list[str]:
        """Update device state and return newly detected anomalies."""
        temperature_state = self.get_state(event.device_id, "temperature")
        power_kw_state = self.get_state(event.device_id, "power_kw")
        anomalies: list[str] = []

        if event.temperature > self.temperature_threshold:
            temperature_state.consecutive_high_values += 1
        else:
            temperature_state.consecutive_high_values = 0
            temperature_state.alert_active = False

        if (temperature_state.consecutive_high_values >= self.required_consecutive_readings and
            not temperature_state.alert_active):
            anomalies.append("sustained_high_temperature")
            temperature_state.alert_active = True

        if event.power_kw > self.power_kw_threshold:
            power_kw_state.consecutive_high_values += 1
        else:
            power_kw_state.consecutive_high_values = 0
            power_kw_state.alert_active = False

        if (power_kw_state.consecutive_high_values >= self.required_consecutive_readings and
            not power_kw_state.alert_active):
            anomalies.append("sustained_high_power_consumption")
            power_kw_state.alert_active = True

        return anomalies
