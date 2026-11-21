"""Configuration, virtual asset registry, operating thresholds, and fault profiles."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Tuple


# ---------------------------------------------------------------------------
# MQTT Broker & Topic Defaults
# ---------------------------------------------------------------------------
DEFAULT_MQTT_HOST: str = os.getenv("MQTT_HOST", "localhost")
DEFAULT_MQTT_PORT: int = int(os.getenv("MQTT_PORT", "1883"))
DEFAULT_MQTT_USERNAME: str | None = os.getenv("MQTT_USERNAME") or None
DEFAULT_MQTT_PASSWORD: str | None = os.getenv("MQTT_PASSWORD") or None
DEFAULT_MQTT_KEEPALIVE: int = int(os.getenv("MQTT_KEEPALIVE", "10"))

DEFAULT_INTERVAL_SEC: float = float(os.getenv("SIM_INTERVAL_SEC", "1.0"))
DEFAULT_RAMP_SEC: float = float(os.getenv("SIM_RAMP_SEC", "50.0"))
DEFAULT_RECOVERY_SEC: float = float(os.getenv("SIM_RECOVERY_SEC", "12.0"))

TOPIC_PREFIX: str = "factory/site01/machine"


def telemetry_topic(machine_id: str) -> str:
    """Unified high-frequency telemetry topic (QoS 0, Retain=False)."""
    return f"{TOPIC_PREFIX}/{machine_id}/telemetry"


def status_topic(machine_id: str) -> str:
    """Device connectivity & metadata topic (QoS 1, Retain=True, LWT target)."""
    return f"{TOPIC_PREFIX}/{machine_id}/status"


def command_topic(machine_id: str) -> str:
    """Downlink control & maintenance action topic (QoS 1, Retain=False)."""
    return f"{TOPIC_PREFIX}/{machine_id}/command"


def broadcast_command_topic() -> str:
    """Broadcast control topic targeting all virtual machines (QoS 1, Retain=False)."""
    return f"{TOPIC_PREFIX}/all/command"


# ---------------------------------------------------------------------------
# Reference Prototype Thresholds (Aligned with Proposal Section 4)
# ---------------------------------------------------------------------------
SIGNAL_THRESHOLDS: Dict[str, Dict[str, str]] = {
    "temperature_c": {
        "normal": "<= 70.0 C",
        "warning": "70.0 - 80.0 C",
        "critical": "> 80.0 C",
    },
    "vibration_rms": {
        "normal": "<= 3.0 mm/s",
        "warning": "3.0 - 6.0 mm/s",
        "critical": "> 6.0 mm/s",
    },
    "current_a": {
        "normal": "<= 15.0 A",
        "warning": "15.0 - 20.0 A",
        "critical": "> 20.0 A",
    },
    "rpm": {
        "normal": "1400 - 1500 RPM",
        "warning": "1350 - 1400 or 1500 - 1550 RPM",
        "critical": "< 1350 or > 1550 RPM",
    },
}


# ---------------------------------------------------------------------------
# Virtual Motor-Pump Asset Definitions
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PumpAssetProfile:
    """Static identity and nominal operating baseline for one motor-pump unit."""

    machine_id: str
    asset_name: str
    asset_type: str
    location: str
    base_temperature_c: float
    base_vibration_rms: float
    base_current_a: float
    base_rpm: float


PUMP_ASSETS: Dict[str, PumpAssetProfile] = {
    "PUMP-01": PumpAssetProfile(
        machine_id="PUMP-01",
        asset_name="Centrifugal Water Pump Unit 01",
        asset_type="3-phase_induction_motor_centrifugal_pump",
        location="factory/site01/cooling-loop-A",
        base_temperature_c=62.5,
        base_vibration_rms=2.05,
        base_current_a=12.1,
        base_rpm=1480.0,
    ),
    "PUMP-02": PumpAssetProfile(
        machine_id="PUMP-02",
        asset_name="Centrifugal Water Pump Unit 02",
        asset_type="3-phase_induction_motor_centrifugal_pump",
        location="factory/site01/cooling-loop-B",
        base_temperature_c=64.0,
        base_vibration_rms=2.20,
        base_current_a=12.4,
        base_rpm=1472.0,
    ),
    "PUMP-03": PumpAssetProfile(
        machine_id="PUMP-03",
        asset_name="Centrifugal Water Pump Unit 03",
        asset_type="3-phase_induction_motor_centrifugal_pump",
        location="factory/site01/main-supply",
        base_temperature_c=63.2,
        base_vibration_rms=2.10,
        base_current_a=12.2,
        base_rpm=1476.0,
    ),
}


# ---------------------------------------------------------------------------
# Operating Scenarios & Fault Progression Profiles
# ---------------------------------------------------------------------------
VALID_SCENARIOS: Tuple[str, ...] = (
    "NORMAL",
    "BEARING_WEAR",
    "OVERHEATING",
    "OVERLOAD",
    "COMBINED_FAILURE",
)


@dataclass(frozen=True)
class ScenarioProfile:
    """Defines target degradation offsets, ramp speed multiplier, and noise."""

    name: str
    description: str
    # Target steady-state deltas added to the asset's nominal baseline at progress=1.0
    delta_temperature_c: float
    delta_vibration_rms: float
    delta_current_a: float
    delta_rpm: float
    # Delay fraction [0.0, 0.5] before secondary thermal rise begins
    temp_lag_fraction: float = 0.0
    # Multiplier applied to the default ramp duration
    ramp_multiplier: float = 1.0
    # Gaussian noise standard deviations
    noise_temp: float = 0.30
    noise_vib: float = 0.08
    noise_current: float = 0.16
    noise_rpm: float = 3.0


SCENARIO_PROFILES: Dict[str, ScenarioProfile] = {
    "NORMAL": ScenarioProfile(
        name="NORMAL",
        description="Nominal steady operation with low Gaussian sensor noise.",
        delta_temperature_c=0.0,
        delta_vibration_rms=0.0,
        delta_current_a=0.0,
        delta_rpm=0.0,
        noise_temp=0.32,
        noise_vib=0.08,
        noise_current=0.18,
        noise_rpm=3.2,
    ),
    "BEARING_WEAR": ScenarioProfile(
        name="BEARING_WEAR",
        description="Progressive bearing degradation: dominant vibration rise with secondary friction heat.",
        delta_temperature_c=+21.5,   # ~63 -> ~84.5 C (secondary friction heat)
        delta_vibration_rms=+6.70,   # ~2.1 -> ~8.8 mm/s (dominant abnormal signal)
        delta_current_a=+1.8,        # ~12.2 -> ~14.0 A (remains within normal <=15 A)
        delta_rpm=-22.0,             # ~1476 -> ~1454 RPM (remains within normal 1400-1500)
        temp_lag_fraction=0.15,      # Vibration leads temperature rise
        ramp_multiplier=1.0,
    ),
    "OVERHEATING": ScenarioProfile(
        name="OVERHEATING",
        description="Cooling/ventilation failure: steady motor temperature rise while mechanical load stays normal.",
        delta_temperature_c=+39.0,   # ~63 -> ~102 C (dominant abnormal signal)
        delta_vibration_rms=+0.30,   # ~2.1 -> ~2.4 mm/s (normal)
        delta_current_a=+0.6,        # ~12.2 -> ~12.8 A (normal)
        delta_rpm=-8.0,              # ~1476 -> ~1468 RPM (normal)
        ramp_multiplier=1.0,
    ),
    "OVERLOAD": ScenarioProfile(
        name="OVERLOAD",
        description="Excessive hydraulic/mechanical load: current rises sharply while shaft RPM drops.",
        delta_temperature_c=+11.0,   # ~63 -> ~74 C (mild delayed resistive heating)
        delta_vibration_rms=+0.45,   # ~2.1 -> ~2.55 mm/s (remains strictly <=3.0 normal)
        delta_current_a=+11.6,       # ~12.2 -> ~23.8 A (dominant abnormal signal, >20 A critical)
        delta_rpm=-170.0,            # ~1476 -> ~1306 RPM (drops below <1350 critical)
        temp_lag_fraction=0.25,
        ramp_multiplier=1.0,
    ),
    "COMBINED_FAILURE": ScenarioProfile(
        name="COMBINED_FAILURE",
        description="Severe multi-subsystem degradation across vibration, temperature, current, and RPM.",
        delta_temperature_c=+43.0,   # ~63 -> ~106 C (critical)
        delta_vibration_rms=+7.30,   # ~2.1 -> ~9.4 mm/s (critical)
        delta_current_a=+12.3,       # ~12.2 -> ~24.5 A (critical)
        delta_rpm=-186.0,            # ~1476 -> ~1290 RPM (critical)
        ramp_multiplier=0.80,        # Degrades 20% faster than single-mode faults
    ),
}
