"""Broker-free unit tests for the Member 2 Kafka streaming layer.

Covers the parts that must be correct before any broker is involved:
- telemetry/status validation (accept valid, reject malformed -> quarantine)
- envelope metadata completeness
- Kafka key encoding and deterministic serialization
- audit-store append-only persistence with offset deduplication
- replay consumer-group id freshness

Run:  python -m unittest kafka_streaming.test_kafka_streaming -v
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from kafka_streaming.config import ALL_TOPICS, new_replay_group_id
from kafka_streaming.consumer_audit import AuditStore
from kafka_streaming.producer import encode_key, serialize_event
from kafka_streaming.schemas import build_envelope, validate_status, validate_telemetry

VALID_TELEMETRY = {
    "machine_id": "PUMP-03",
    "timestamp": "2026-11-14T16:55:12.401Z",
    "seq": 48,
    "machine_status": "RUNNING",
    "temperature_c": 86.42,
    "vibration_rms": 7.65,
    "current_a": 13.84,
    "rpm": 1456,
    "sim_scenario": "BEARING_WEAR",
}


class TestTelemetryValidation(unittest.TestCase):
    def test_valid_payload_accepted_and_normalized(self):
        result = validate_telemetry(VALID_TELEMETRY)
        self.assertTrue(result.ok, result.errors)
        self.assertEqual(result.machine_id, "PUMP-03")
        self.assertEqual(result.event_time, "2026-11-14T16:55:12.401Z")
        self.assertEqual(result.payload["seq"], 48)
        self.assertEqual(result.payload["sim_scenario"], "BEARING_WEAR")
        self.assertAlmostEqual(result.payload["vibration_rms"], 7.65)

    def test_missing_machine_id_rejected(self):
        bad = {k: v for k, v in VALID_TELEMETRY.items() if k != "machine_id"}
        result = validate_telemetry(bad)
        self.assertFalse(result.ok)
        self.assertIsNone(result.machine_id)
        self.assertTrue(any("machine_id" in e for e in result.errors))

    def test_missing_timestamp_rejected(self):
        bad = {k: v for k, v in VALID_TELEMETRY.items() if k != "timestamp"}
        result = validate_telemetry(bad)
        self.assertFalse(result.ok)
        self.assertTrue(any("timestamp" in e for e in result.errors))

    def test_non_numeric_sensor_rejected(self):
        bad = dict(VALID_TELEMETRY, temperature_c="hot")
        result = validate_telemetry(bad)
        self.assertFalse(result.ok)
        self.assertTrue(any("temperature_c" in e for e in result.errors))

    def test_out_of_bounds_sensor_rejected(self):
        bad = dict(VALID_TELEMETRY, rpm=99999)
        result = validate_telemetry(bad)
        self.assertFalse(result.ok)

    def test_non_object_rejected(self):
        self.assertFalse(validate_telemetry([1, 2, 3]).ok)  # type: ignore[arg-type]


class TestStatusValidation(unittest.TestCase):
    def test_lwt_payload_without_timestamp_defaults_to_receipt_time(self):
        lwt = {
            "machine_id": "PUMP-02",
            "connectivity": "OFFLINE",
            "machine_status": "OFFLINE",
            "reason": "unexpected_disconnect_lwt",
        }
        result = validate_status(lwt)
        self.assertTrue(result.ok, result.errors)
        self.assertIsNotNone(result.event_time)  # defaulted to local receipt time
        self.assertEqual(result.payload["connectivity"], "OFFLINE")

    def test_missing_machine_id_rejected(self):
        self.assertFalse(validate_status({"connectivity": "ONLINE"}).ok)


class TestEnvelopeAndKeys(unittest.TestCase):
    def test_envelope_carries_required_metadata(self):
        env = build_envelope(
            "telemetry", "PUMP-01", {"rpm": 1480.0},
            event_time="2026-11-14T16:55:12.401Z", source="kafka-bridge",
        )
        for field in ("schema_version", "event_type", "machine_id", "event_time", "ingest_time", "source", "payload"):
            self.assertIn(field, env)
        self.assertEqual(env["machine_id"], "PUMP-01")

    def test_envelope_rejects_unknown_type_and_empty_key(self):
        with self.assertRaises(ValueError):
            build_envelope("bogus", "PUMP-01", {}, source="t")
        with self.assertRaises(ValueError):
            build_envelope("telemetry", "", {}, source="t")

    def test_key_encoding(self):
        self.assertEqual(encode_key("PUMP-01"), b"PUMP-01")
        with self.assertRaises(ValueError):
            encode_key("")

    def test_serialization_is_deterministic(self):
        event = {"b": 1, "a": {"z": 2, "y": 3}}
        self.assertEqual(serialize_event(event), serialize_event(dict(reversed(list(event.items())))))

    def test_all_six_topics_defined(self):
        self.assertEqual(
            set(ALL_TOPICS),
            {
                "machine.telemetry", "machine.features", "machine.health",
                "machine.alerts", "maintenance.actions", "machine.audit",
            },
        )

    def test_replay_group_ids_are_unique(self):
        self.assertNotEqual(new_replay_group_id(), new_replay_group_id())


class TestAuditStore(unittest.TestCase):
    def _event(self, machine_id: str, seq: int) -> dict:
        return build_envelope(
            "telemetry", machine_id,
            {"machine_id": machine_id, "seq": seq, "rpm": 1476.0},
            source="kafka-bridge",
        )

    def test_append_creates_per_machine_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            with AuditStore(tmp) as store:
                self.assertTrue(store.append("machine.audit", 0, 0, self._event("PUMP-01", 1)))
                self.assertTrue(store.append("machine.audit", 0, 1, self._event("PUMP-02", 1)))
            path = Path(tmp) / "machine.audit" / "PUMP-01.jsonl"
            self.assertTrue(path.exists())
            row = json.loads(path.read_text(encoding="utf-8").strip())
            self.assertEqual(row["kafka_topic"], "machine.audit")
            self.assertEqual(row["kafka_offset"], 0)
            self.assertEqual(row["machine_id"], "PUMP-01")

    def test_duplicate_offsets_are_skipped_idempotently(self):
        with tempfile.TemporaryDirectory() as tmp:
            with AuditStore(tmp) as store:
                event = self._event("PUMP-03", 7)
                self.assertTrue(store.append("machine.telemetry", 2, 42, event))
                self.assertFalse(store.append("machine.telemetry", 2, 42, event))  # redelivery
                self.assertEqual(store.records_written, 1)
                self.assertEqual(store.duplicates_skipped, 1)
            lines = (Path(tmp) / "machine.telemetry" / "PUMP-03.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)


if __name__ == "__main__":
    unittest.main()
