"""MQTT -> Kafka bridge: validates live machine events and appends them to Kafka.

Data flow (README architecture, arrow "Validated Telemetry & Status"):

    Mosquitto (factory/site01/machine/+/telemetry, +/status)
        -> validation (schemas.py)
        -> machine.telemetry  (valid telemetry, key = machine_id)
        -> machine.alerts     (OFFLINE connectivity incidents from broker LWT)
        -> machine.audit      (consolidated append-only copy of every event)
        -> machine.audit      (quarantine events for malformed payloads)

The bridge is intentionally a *separate process* from the simulator and from
the consumers: MQTT stays the live edge link, Kafka becomes the durable,
replayable system of record. Node-RED (Member 3) will publish its derived
``features``/``health``/``maintenance_action`` events through the same
``EventProducer``/envelope contract.

Usage:
    python -m kafka_streaming.bridge --mqtt-host localhost --bootstrap-servers localhost:9092
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from typing import Any, Dict

from simulator.config import (
    DEFAULT_MQTT_HOST,
    DEFAULT_MQTT_PORT,
    TOPIC_PREFIX,
)

from .config import (
    DEFAULT_BOOTSTRAP_SERVERS,
    SOURCE_BRIDGE,
    TOPIC_ALERTS,
    TOPIC_AUDIT,
    TOPIC_TELEMETRY,
)
from .producer import EventProducer
from .schemas import ValidationResult, build_envelope, validate_status, validate_telemetry

log = logging.getLogger("kafka.bridge")

TELEMETRY_SUB = f"{TOPIC_PREFIX}/+/telemetry"
STATUS_SUB = f"{TOPIC_PREFIX}/+/status"


class MqttKafkaBridge:
    """Subscribes to MQTT machine topics and produces validated Kafka events."""

    def __init__(
        self,
        producer: EventProducer,
        *,
        mqtt_host: str = DEFAULT_MQTT_HOST,
        mqtt_port: int = DEFAULT_MQTT_PORT,
    ) -> None:
        self.producer = producer
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self._client = None
        self._stop = threading.Event()
        self.stats = {
            "telemetry_ok": 0,
            "status_ok": 0,
            "quarantined": 0,
            "alerts": 0,
            "produce_errors": 0,
        }

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------
    def _produce(self, topic: str, event: Dict[str, Any]) -> None:
        try:
            meta = self.producer.send(topic, event)
            log.debug(
                "-> %s[%d]@%d key=%s type=%s",
                meta["topic"], meta["partition"], meta["offset"],
                event["machine_id"], event["event_type"],
            )
        except Exception:
            self.stats["produce_errors"] += 1
            log.exception("failed to produce %s event for %s", event.get("event_type"), event.get("machine_id"))

    def _quarantine(self, kind: str, raw_payload: bytes, errors: list[str], machine_id: str | None) -> None:
        """Preserve malformed input on the audit topic instead of dropping it."""
        self.stats["quarantined"] += 1
        log.warning("quarantined malformed %s payload (%s): %r", kind, "; ".join(errors), raw_payload[:200])
        event = build_envelope(
            "quarantine",
            machine_id or "UNKNOWN",
            {
                "rejected_from": kind,
                "errors": errors,
                "raw_payload": raw_payload.decode("utf-8", errors="replace")[:2000],
            },
            source=SOURCE_BRIDGE,
        )
        self._produce(TOPIC_AUDIT, event)

    def handle_telemetry(self, raw_payload: bytes) -> None:
        try:
            raw = json.loads(raw_payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._quarantine("telemetry", raw_payload, [f"invalid JSON: {exc}"], None)
            return

        result: ValidationResult = validate_telemetry(raw)
        if not result.ok:
            self._quarantine("telemetry", raw_payload, result.errors, result.machine_id)
            return

        event = build_envelope(
            "telemetry",
            result.machine_id,  # type: ignore[arg-type]
            result.payload,
            event_time=result.event_time,
            source=SOURCE_BRIDGE,
        )
        self._produce(TOPIC_TELEMETRY, event)
        self._produce(TOPIC_AUDIT, event)
        self.stats["telemetry_ok"] += 1

    def handle_status(self, raw_payload: bytes) -> None:
        try:
            raw = json.loads(raw_payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._quarantine("status", raw_payload, [f"invalid JSON: {exc}"], None)
            return

        result = validate_status(raw)
        if not result.ok:
            self._quarantine("status", raw_payload, result.errors, result.machine_id)
            return

        event = build_envelope(
            "status",
            result.machine_id,  # type: ignore[arg-type]
            result.payload,
            event_time=result.event_time,
            source=SOURCE_BRIDGE,
        )
        self._produce(TOPIC_AUDIT, event)
        self.stats["status_ok"] += 1

        # LWT / disconnect => connectivity incident on the alerts stream.
        connectivity = str(result.payload.get("connectivity", "")).upper()
        machine_status = str(result.payload.get("machine_status", "")).upper()
        if connectivity == "OFFLINE" or machine_status == "OFFLINE":
            alert = build_envelope(
                "alert",
                result.machine_id,  # type: ignore[arg-type]
                {
                    "alert_type": "CONNECTIVITY",
                    "severity": "CRITICAL",
                    "reason": result.payload.get("reason", "unexpected_disconnect_lwt"),
                    "machine_status": "OFFLINE",
                },
                event_time=result.event_time,
                source=SOURCE_BRIDGE,
            )
            self._produce(TOPIC_ALERTS, alert)
            self._produce(TOPIC_AUDIT, alert)
            self.stats["alerts"] += 1

    # ------------------------------------------------------------------
    # MQTT plumbing
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc == 0:
            log.info("connected to MQTT %s:%d, subscribing to %s and %s", self.mqtt_host, self.mqtt_port, TELEMETRY_SUB, STATUS_SUB)
            client.subscribe(TELEMETRY_SUB, qos=0)
            client.subscribe(STATUS_SUB, qos=1)
        else:
            log.error("MQTT connect failed with rc=%s", rc)

    def _on_message(self, client, userdata, msg) -> None:
        if msg.topic.endswith("/telemetry"):
            self.handle_telemetry(msg.payload)
        elif msg.topic.endswith("/status"):
            self.handle_status(msg.payload)

    def run(self) -> None:
        import paho.mqtt.client as mqtt

        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="kafka-bridge")
        except (AttributeError, TypeError):  # paho-mqtt 1.x
            client = mqtt.Client(client_id="kafka-bridge")
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        self._client = client

        log.info("bridge starting (MQTT %s:%d -> Kafka %s)", self.mqtt_host, self.mqtt_port, self.producer.bootstrap_servers)
        client.connect(self.mqtt_host, self.mqtt_port, keepalive=30)
        client.loop_start()
        try:
            while not self._stop.is_set():
                self._stop.wait(1.0)
        finally:
            client.loop_stop()
            client.disconnect()
            self.producer.close()
            log.info("bridge stopped. stats=%s", self.stats)

    def stop(self, *_: Any) -> None:
        self._stop.set()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MQTT -> Kafka validation bridge (Member 2).")
    parser.add_argument("--mqtt-host", default=DEFAULT_MQTT_HOST)
    parser.add_argument("--mqtt-port", type=int, default=DEFAULT_MQTT_PORT)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        producer = EventProducer(args.bootstrap_servers)
    except Exception as exc:
        log.error("cannot reach Kafka at %s: %s", args.bootstrap_servers, exc)
        return 1

    bridge = MqttKafkaBridge(producer, mqtt_host=args.mqtt_host, mqtt_port=args.mqtt_port)
    signal.signal(signal.SIGINT, bridge.stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, bridge.stop)
    try:
        bridge.run()
    except Exception as exc:
        log.error("bridge failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
