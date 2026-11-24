"""Kafka-side event schemas, validation and metadata envelopes (Member 2 contract).

Every record written to Kafka is wrapped in a common envelope carrying the
message metadata required for audit, replay and debugging:

    {
      "schema_version": "1.0",
      "event_type":     "telemetry" | "status" | "alert" | "features"
                        | "health" | "maintenance_action" | "quarantine",
      "machine_id":     "PUMP-01",            # also the Kafka record key
      "event_time":     "<ISO-8601 from the source event>",
      "ingest_time":    "<ISO-8601 bridge/consumer receipt time>",
      "source":         "kafka-bridge" | ...,
      "payload":        { ... event-specific fields ... }
    }

Telemetry validation mirrors the proposal's contract: ``machine_id``,
``timestamp`` and the four numeric sensor fields are mandatory; malformed
messages are rejected (and can be quarantined as ``quarantine`` events on the
audit topic instead of silently dropped).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .config import SCHEMA_VERSION

# Mandatory numeric sensor fields of a telemetry event and their plausible
# physical bounds for a motor-pump unit (sanity check, not alarm thresholds).
TELEMETRY_NUMERIC_FIELDS: Dict[str, tuple[float, float]] = {
    "temperature_c": (-40.0, 250.0),
    "vibration_rms": (0.0, 100.0),
    "current_a": (0.0, 200.0),
    "rpm": (0.0, 6000.0),
}

VALID_EVENT_TYPES: tuple[str, ...] = (
    "telemetry",
    "status",
    "alert",
    "features",
    "health",
    "maintenance_action",
    "quarantine",
)


def utc_now_iso() -> str:
    """Current UTC time as an ISO-8601 string with millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_iso8601(value: Any) -> Optional[str]:
    """Return a normalized ISO-8601 UTC string, or None if unparseable."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class ValidationResult:
    """Outcome of validating one incoming MQTT payload for the Kafka layer."""

    ok: bool
    machine_id: Optional[str] = None
    event_time: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def validate_telemetry(raw: Dict[str, Any]) -> ValidationResult:
    """Validate a raw telemetry payload from ``factory/site01/machine/+/telemetry``.

    Required: ``machine_id`` (non-empty str), ``timestamp`` (ISO-8601) and the
    four numeric sensor fields within plausible physical bounds. ``seq`` is
    optional but, when present, must be a non-negative integer.
    """
    errors: list[str] = []
    if not isinstance(raw, dict):
        return ValidationResult(ok=False, errors=["payload is not a JSON object"])

    machine_id = raw.get("machine_id")
    if not isinstance(machine_id, str) or not machine_id.strip():
        errors.append("missing or invalid 'machine_id'")
        machine_id = None
    else:
        machine_id = machine_id.strip()

    event_time = _parse_iso8601(raw.get("timestamp"))
    if event_time is None:
        errors.append("missing or invalid 'timestamp' (expected ISO-8601)")

    payload: Dict[str, Any] = {}
    for name, (lo, hi) in TELEMETRY_NUMERIC_FIELDS.items():
        value = raw.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"missing or non-numeric '{name}'")
            continue
        value = float(value)
        if not (lo <= value <= hi):
            errors.append(f"'{name}'={value} outside plausible range [{lo}, {hi}]")
            continue
        payload[name] = value

    seq = raw.get("seq")
    if seq is not None:
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            errors.append("invalid 'seq' (expected non-negative integer)")
        else:
            payload["seq"] = seq

    for optional in ("machine_status", "sim_scenario"):
        value = raw.get(optional)
        if isinstance(value, str):
            payload[optional] = value

    if machine_id is not None:
        payload["machine_id"] = machine_id

    return ValidationResult(
        ok=not errors,
        machine_id=machine_id,
        event_time=event_time,
        payload=payload,
        errors=errors,
    )


def validate_status(raw: Dict[str, Any]) -> ValidationResult:
    """Validate a raw device-status payload (``/status``, incl. broker LWT events).

    LWT payloads intentionally omit ``updated_at``/``sim_scenario``; a missing
    timestamp falls back to local receipt time per the consumer contract note.
    """
    if not isinstance(raw, dict):
        return ValidationResult(ok=False, errors=["payload is not a JSON object"])

    errors: list[str] = []
    machine_id = raw.get("machine_id")
    if not isinstance(machine_id, str) or not machine_id.strip():
        errors.append("missing or invalid 'machine_id'")
        machine_id = None
    else:
        machine_id = machine_id.strip()

    event_time = _parse_iso8601(raw.get("updated_at")) or utc_now_iso()

    payload: Dict[str, Any] = dict(raw)
    if machine_id is not None:
        payload["machine_id"] = machine_id
    payload.setdefault("connectivity", "UNKNOWN")

    return ValidationResult(
        ok=not errors,
        machine_id=machine_id,
        event_time=event_time,
        payload=payload,
        errors=errors,
    )


def build_envelope(
    event_type: str,
    machine_id: str,
    payload: Dict[str, Any],
    *,
    event_time: Optional[str] = None,
    source: str,
) -> Dict[str, Any]:
    """Wrap a validated payload in the common Kafka event envelope."""
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"unknown event_type: {event_type!r}")
    if not machine_id:
        raise ValueError("machine_id is required (it is the Kafka record key)")
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "machine_id": machine_id,
        "event_time": event_time or utc_now_iso(),
        "ingest_time": utc_now_iso(),
        "source": source,
        "payload": payload,
    }
