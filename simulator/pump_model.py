"""Physical state machine and telemetry generator for a virtual motor-pump unit."""

from __future__ import annotations

from datetime import datetime, timezone
import math
import random
import threading
from typing import Any, Dict, Tuple

from .config import (
    DEFAULT_RAMP_SEC,
    DEFAULT_RECOVERY_SEC,
    PumpAssetProfile,
    SCENARIO_PROFILES,
    VALID_SCENARIOS,
)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class VirtualPump:
    """Thread-safe simulation model for one industrial motor-pump asset.

    Supports five operating scenarios (NORMAL, BEARING_WEAR, OVERHEATING,
    OVERLOAD, COMBINED_FAILURE) plus smooth closed-loop RECOVERING transition
    after maintenance is accepted.
    """

    def __init__(
        self,
        profile: PumpAssetProfile,
        ramp_sec: float = DEFAULT_RAMP_SEC,
        recovery_sec: float = DEFAULT_RECOVERY_SEC,
        seed: int | None = None,
    ) -> None:
        self.profile = profile
        self.default_ramp_sec = max(5.0, float(ramp_sec))
        self.default_recovery_sec = max(2.0, float(recovery_sec))

        self._lock = threading.Lock()
        self._rng = random.Random(seed)

        self.seq: int = 0
        self.sim_scenario: str = "NORMAL"
        self.machine_status: str = "RUNNING"  # RUNNING | RECOVERING
        self.elapsed_in_mode_sec: float = 0.0
        self.active_ramp_sec: float = self.default_ramp_sec
        self.active_recovery_sec: float = self.default_recovery_sec

        # Current underlying (noiseless) physical state for smooth transitions
        self._mean_temp: float = profile.base_temperature_c
        self._mean_vib: float = profile.base_vibration_rms
        self._mean_current: float = profile.base_current_a
        self._mean_rpm: float = profile.base_rpm

        # Snapshot captured when maintenance is accepted
        self._recovery_start: Tuple[float, float, float, float] = (
            self._mean_temp,
            self._mean_vib,
            self._mean_current,
            self._mean_rpm,
        )
        self.maintenance_count: int = 0
        self.last_action_id: str | None = None

    def set_scenario(self, scenario: str, ramp_sec: float | None = None) -> Dict[str, Any]:
        """Switch the pump into NORMAL or one of the four fault scenarios."""
        normalized = scenario.strip().upper()
        if normalized not in VALID_SCENARIOS:
            raise ValueError(
                f"Invalid scenario '{scenario}'. Valid options: {', '.join(VALID_SCENARIOS)}"
            )

        with self._lock:
            self.sim_scenario = normalized
            self.machine_status = "RUNNING"
            self.elapsed_in_mode_sec = 0.0
            spec = SCENARIO_PROFILES[normalized]
            base_ramp = float(ramp_sec) if ramp_sec is not None else self.default_ramp_sec
            self.active_ramp_sec = max(5.0, base_ramp * spec.ramp_multiplier)

            if normalized == "NORMAL":
                self._mean_temp = self.profile.base_temperature_c
                self._mean_vib = self.profile.base_vibration_rms
                self._mean_current = self.profile.base_current_a
                self._mean_rpm = self.profile.base_rpm

            return self._snapshot_locked()

    def accept_maintenance(
        self,
        action_id: str | None = None,
        recovery_sec: float | None = None,
    ) -> Tuple[Dict[str, Any], bool]:
        """Accept maintenance and begin smooth recovery back to nominal baseline.

        Returns (status_snapshot, started_recovery).
        Idempotent under QoS 1 duplicate deliveries or repeated button clicks:
        - If already RECOVERING (or same action_id), preserves ongoing recovery progress.
        - If already healthy in NORMAL mode, returns current state without false recovery.
        """
        with self._lock:
            if action_id and action_id == self.last_action_id:
                return self._snapshot_locked(), False
            if self.machine_status == "RECOVERING":
                return self._snapshot_locked(), False
            if self.sim_scenario == "NORMAL" and self.machine_status == "RUNNING":
                if action_id:
                    self.last_action_id = action_id
                return self._snapshot_locked(), False

            self._recovery_start = (
                self._mean_temp,
                self._mean_vib,
                self._mean_current,
                self._mean_rpm,
            )
            self.active_recovery_sec = (
                max(2.0, float(recovery_sec))
                if recovery_sec is not None
                else self.default_recovery_sec
            )
            self.machine_status = "RECOVERING"
            self.elapsed_in_mode_sec = 0.0
            self.maintenance_count += 1
            self.last_action_id = action_id or f"MAINT-{self.maintenance_count:03d}"
            return self._snapshot_locked(), True

    def get_status_metadata(self) -> Dict[str, Any]:
        """Return current asset identity and scenario state for status messages."""
        with self._lock:
            return self._snapshot_locked()

    def _snapshot_locked(self) -> Dict[str, Any]:
        return {
            "machine_id": self.profile.machine_id,
            "asset_name": self.profile.asset_name,
            "asset_type": self.profile.asset_type,
            "location": self.profile.location,
            "machine_status": self.machine_status,
            "sim_scenario": self.sim_scenario,
            "maintenance_count": self.maintenance_count,
            "last_action_id": self.last_action_id,
        }

    def step(self, dt_sec: float) -> Tuple[Dict[str, Any], bool]:
        """Advance simulation by dt_sec and return (telemetry_payload, status_changed).

        status_changed is True when a RECOVERING machine finishes returning to
        NORMAL so the caller can republish the retained MQTT status message.
        """
        with self._lock:
            self.seq += 1
            dt = max(0.05, float(dt_sec))
            status_changed = False

            if self.machine_status == "RECOVERING":
                self.elapsed_in_mode_sec += dt
                progress = min(1.0, self.elapsed_in_mode_sec / self.active_recovery_sec)
                # Smooth ease-out interpolation from degraded state back to baseline
                ease = 1.0 - (1.0 - progress) ** 2
                start_t, start_v, start_c, start_r = self._recovery_start

                self._mean_temp = start_t + (self.profile.base_temperature_c - start_t) * ease
                self._mean_vib = start_v + (self.profile.base_vibration_rms - start_v) * ease
                self._mean_current = start_c + (self.profile.base_current_a - start_c) * ease
                self._mean_rpm = start_r + (self.profile.base_rpm - start_r) * ease

                spec = SCENARIO_PROFILES["NORMAL"]
                temp = self._mean_temp + self._rng.gauss(0.0, spec.noise_temp)
                vib = max(0.1, self._mean_vib + self._rng.gauss(0.0, spec.noise_vib))
                cur = max(0.5, self._mean_current + self._rng.gauss(0.0, spec.noise_current))
                rpm = self._mean_rpm + self._rng.gauss(0.0, spec.noise_rpm)

                if progress >= 1.0:
                    self.machine_status = "RUNNING"
                    self.sim_scenario = "NORMAL"
                    self.elapsed_in_mode_sec = 0.0
                    self._mean_temp = self.profile.base_temperature_c
                    self._mean_vib = self.profile.base_vibration_rms
                    self._mean_current = self.profile.base_current_a
                    self._mean_rpm = self.profile.base_rpm
                    status_changed = True

            elif self.sim_scenario == "NORMAL":
                self.elapsed_in_mode_sec += dt
                spec = SCENARIO_PROFILES["NORMAL"]
                self._mean_temp = self.profile.base_temperature_c
                self._mean_vib = self.profile.base_vibration_rms
                self._mean_current = self.profile.base_current_a
                self._mean_rpm = self.profile.base_rpm

                # Clamp safely inside normal limits so random outliers never false-alarm
                temp = _clamp(
                    self._mean_temp + self._rng.gauss(0.0, spec.noise_temp),
                    55.0,
                    68.5,
                )
                vib = _clamp(
                    self._mean_vib + self._rng.gauss(0.0, spec.noise_vib),
                    1.2,
                    2.80,
                )
                cur = _clamp(
                    self._mean_current + self._rng.gauss(0.0, spec.noise_current),
                    9.5,
                    14.4,
                )
                rpm = _clamp(
                    self._mean_rpm + self._rng.gauss(0.0, spec.noise_rpm),
                    1415.0,
                    1495.0,
                )

            else:
                # Active fault progression: steady linear ramp (high R^2 for TTC) -> plateau
                self.elapsed_in_mode_sec += dt
                spec = SCENARIO_PROFILES[self.sim_scenario]
                raw_progress = self.elapsed_in_mode_sec / self.active_ramp_sec
                progress = min(1.0, raw_progress)

                # Optional lag on secondary temperature rise (e.g. friction/load heat)
                if spec.temp_lag_fraction > 0.0:
                    if progress <= spec.temp_lag_fraction:
                        temp_progress = 0.0
                    else:
                        temp_progress = min(
                            1.0,
                            (progress - spec.temp_lag_fraction) / (1.0 - spec.temp_lag_fraction),
                        )
                else:
                    temp_progress = progress

                # Subtle bounded fluctuation once saturated at plateau (raw_progress > 1.0)
                plateau_wave = (
                    math.sin(self.elapsed_in_mode_sec * 0.35) * 0.15
                    if raw_progress > 1.0
                    else 0.0
                )

                self._mean_temp = (
                    self.profile.base_temperature_c
                    + spec.delta_temperature_c * temp_progress
                    + plateau_wave * 1.2
                )
                self._mean_vib = (
                    self.profile.base_vibration_rms
                    + spec.delta_vibration_rms * progress
                    + plateau_wave * 0.25
                )
                self._mean_current = (
                    self.profile.base_current_a
                    + spec.delta_current_a * progress
                    + plateau_wave * 0.3
                )
                self._mean_rpm = (
                    self.profile.base_rpm
                    + spec.delta_rpm * progress
                    - plateau_wave * 3.0
                )

                temp = self._mean_temp + self._rng.gauss(0.0, spec.noise_temp)
                vib = max(0.2, self._mean_vib + self._rng.gauss(0.0, spec.noise_vib))
                cur = max(1.0, self._mean_current + self._rng.gauss(0.0, spec.noise_current))
                rpm = max(600.0, self._mean_rpm + self._rng.gauss(0.0, spec.noise_rpm))

            now_iso = (
                datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )

            telemetry: Dict[str, Any] = {
                "machine_id": self.profile.machine_id,
                "timestamp": now_iso,
                "seq": self.seq,
                "machine_status": self.machine_status,
                "temperature_c": round(temp, 2),
                "vibration_rms": round(vib, 2),
                "current_a": round(cur, 2),
                "rpm": int(round(rpm)),
                "sim_scenario": self.sim_scenario,
            }
            return telemetry, status_changed
