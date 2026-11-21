"""Per-machine MQTT client handling telemetry, retained status, LWT, and commands."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import socket
import threading
from typing import Any, Dict

import paho.mqtt.client as mqtt

from .config import (
    DEFAULT_MQTT_KEEPALIVE,
    TOPIC_PREFIX,
    broadcast_command_topic,
    command_topic,
    status_topic,
    telemetry_topic,
)
from .pump_model import VirtualPump

logger = logging.getLogger("simulator.mqtt")


def create_mqtt_client(client_id: str, clean_session: bool = True) -> mqtt.Client:
    """Create a Paho MQTT client compatible with both paho-mqtt v2.x and v1.x."""
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=clean_session,
        )
    return mqtt.Client(client_id=client_id, clean_session=clean_session)


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class PumpMQTTWorker:
    """Dedicated MQTT client for one virtual motor-pump unit.

    Maintains its own TCP connection to the MQTT broker so each virtual machine
    has an independent Last Will and Testament (LWT) and online/offline lifecycle.
    """

    def __init__(
        self,
        pump: VirtualPump,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        keepalive: int = DEFAULT_MQTT_KEEPALIVE,
    ) -> None:
        self.pump = pump
        self.machine_id = pump.profile.machine_id
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.keepalive = keepalive

        self.topic_telemetry = telemetry_topic(self.machine_id)
        self.topic_status = status_topic(self.machine_id)
        self.topic_command = command_topic(self.machine_id)
        self.topic_broadcast = broadcast_command_topic()

        self._state_lock = threading.Lock()
        self._connected = False
        self._crashed = False
        self._client: mqtt.Client | None = None
        self.last_telemetry: Dict[str, Any] | None = None

    @property
    def is_connected(self) -> bool:
        with self._state_lock:
            return self._connected and not self._crashed

    @property
    def is_crashed(self) -> bool:
        with self._state_lock:
            return self._crashed

    def _build_client(self) -> mqtt.Client:
        client_id = f"sim-{self.machine_id}"
        client = create_mqtt_client(client_id=client_id, clean_session=True)

        if self.username:
            client.username_pw_set(self.username, self.password)

        # Register Last Will and Testament (LWT) BEFORE connecting.
        # Broker publishes this retained QoS 1 payload automatically if the
        # client's TCP socket drops without a clean MQTT DISCONNECT packet.
        lwt_payload = json.dumps(
            {
                "machine_id": self.machine_id,
                "asset_name": self.pump.profile.asset_name,
                "asset_type": self.pump.profile.asset_type,
                "location": self.pump.profile.location,
                "connectivity": "OFFLINE",
                "machine_status": "OFFLINE",
                "reason": "unexpected_disconnect_lwt",
            }
        )
        client.will_set(
            topic=self.topic_status,
            payload=lwt_payload,
            qos=1,
            retain=True,
        )

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        return client

    def start(self) -> None:
        """Connect to the MQTT broker and start the background network loop."""
        with self._state_lock:
            if self._client is not None and not self._crashed:
                return
            self._crashed = False
            client = self._build_client()
            self._client = client

        try:
            client.connect(self.host, self.port, keepalive=self.keepalive)
            client.loop_start()
        except Exception:
            with self._state_lock:
                if self._client is client:
                    self._client = None
                    self._crashed = True
            raise

    def stop_gracefully(self) -> None:
        """Publish an explicit graceful OFFLINE status and disconnect cleanly."""
        with self._state_lock:
            client = self._client
            was_crashed = self._crashed
            self._connected = False

        if client is None or was_crashed:
            return

        try:
            offline_payload = json.dumps(
                {
                    **self.pump.get_status_metadata(),
                    "connectivity": "OFFLINE",
                    "machine_status": "OFFLINE",
                    "reason": "graceful_shutdown",
                    "updated_at": _utc_now_iso(),
                }
            )
            info = client.publish(self.topic_status, offline_payload, qos=1, retain=True)
            info.wait_for_publish(timeout=2.0)
        except Exception as exc:  # pragma: no cover - best-effort shutdown
            logger.debug("[%s] Graceful status publish skipped: %s", self.machine_id, exc)
        finally:
            try:
                client.disconnect()
            finally:
                client.loop_stop()

    def simulate_crash(self) -> None:
        """Force-drop the underlying TCP socket WITHOUT sending MQTT DISCONNECT.

        This causes the MQTT broker to detect an ungraceful disconnect and fire
        this machine's Last Will and Testament (LWT) while other machines keep running.
        """
        with self._state_lock:
            if self._crashed or self._client is None:
                return
            self._crashed = True
            self._connected = False
            client = self._client
            self._client = None

        logger.warning(
            "[%s] Simulating ungraceful gateway/device crash (expect broker LWT on %s)",
            self.machine_id,
            self.topic_status,
        )
        # Prevent Paho's background thread from auto-reconnecting after socket drop
        client._thread_terminate = True  # noqa: SLF001
        sock = client.socket()
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        client.loop_stop()

    def reconnect(self) -> None:
        """Recover from a simulated crash by establishing a fresh MQTT session."""
        with self._state_lock:
            if not self._crashed and self._client is not None:
                return
        logger.info("[%s] Reconnecting virtual machine to MQTT broker...", self.machine_id)
        try:
            self.start()
        except Exception as exc:
            logger.error("[%s] Reconnect failed: %s", self.machine_id, exc)

    def publish_status(self, reason: str = "state_update") -> None:
        """Publish retained ONLINE status with current identity and scenario metadata."""
        with self._state_lock:
            if self._crashed or not self._connected or self._client is None:
                return
            client = self._client

        payload = json.dumps(
            {
                **self.pump.get_status_metadata(),
                "connectivity": "ONLINE",
                "reason": reason,
                "updated_at": _utc_now_iso(),
            }
        )
        client.publish(self.topic_status, payload, qos=1, retain=True)

    def step_and_publish(self, dt_sec: float) -> Dict[str, Any] | None:
        """Advance pump physics by one tick and publish QoS 0 telemetry if online."""
        if self.is_crashed:
            return None

        telemetry, status_changed = self.pump.step(dt_sec)
        self.last_telemetry = telemetry

        with self._state_lock:
            client = self._client if self._connected else None

        if client is not None:
            client.publish(
                self.topic_telemetry,
                json.dumps(telemetry),
                qos=0,
                retain=False,
            )
            if status_changed:
                logger.info(
                    "[%s] Maintenance recovery completed -> back to NORMAL.",
                    self.machine_id,
                )
                self.publish_status(reason="maintenance_recovery_complete")

        return telemetry

    # ------------------------------------------------------------------
    # Paho v1 / v2 compatible callbacks
    # ------------------------------------------------------------------
    def _on_connect(self, client: mqtt.Client, userdata: Any, *args: Any) -> None:
        # Paho v2: (client, userdata, flags, reason_code, properties)
        # Paho v1: (client, userdata, flags, rc)
        rc = args[1] if len(args) >= 2 else 0
        failed = (rc != 0) if isinstance(rc, int) else getattr(rc, "is_failure", False)
        if failed:
            logger.error("[%s] MQTT connection failed (code=%s)", self.machine_id, rc)
            return

        with self._state_lock:
            if self._crashed:
                return
            self._connected = True

        client.subscribe(self.topic_command, qos=1)
        client.subscribe(self.topic_broadcast, qos=1)
        self.publish_status(reason="connected")
        logger.info("[%s] Connected to MQTT broker (%s:%d)", self.machine_id, self.host, self.port)

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, *args: Any) -> None:
        with self._state_lock:
            self._connected = False

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        raw = msg.payload.decode("utf-8", errors="replace").strip()
        self.handle_command_payload(raw)

    def handle_command_payload(self, raw: str) -> bool:
        """Parse and execute a command JSON or shorthand string. Returns True if handled."""
        try:
            data = json.loads(raw) if raw.startswith("{") else {"command": raw}
        except json.JSONDecodeError:
            logger.warning("[%s] Ignored invalid JSON command: %s", self.machine_id, raw)
            return False

        # Support flexible field aliases from Node-RED Dashboard or CLI
        cmd = (
            str(
                data.get("command")
                or data.get("action")
                or data.get("cmd")
                or ""
            )
            .strip()
            .lower()
        )
        # Shorthand: {"scenario": "BEARING_WEAR"} or {"mode": "OVERHEATING"}
        if not cmd and ("scenario" in data or "mode" in data):
            cmd = "set_scenario"

        if cmd in ("set_scenario", "set_mode", "scenario", "mode"):
            target_mode = str(data.get("scenario") or data.get("mode") or "NORMAL")
            ramp_sec = data.get("ramp_sec")
            try:
                self.pump.set_scenario(target_mode, ramp_sec=ramp_sec)
                logger.info("[%s] Scenario switched -> %s", self.machine_id, target_mode.upper())
                self.publish_status(reason="scenario_changed")
                return True
            except ValueError as exc:
                logger.warning("[%s] %s", self.machine_id, exc)
                return False

        if cmd in ("accept_maintenance", "maintenance_accepted", "maintain", "repair", "reset"):
            action_id = data.get("action_id")
            recovery_sec = data.get("recovery_sec")
            _, started = self.pump.accept_maintenance(
                action_id=action_id,
                recovery_sec=recovery_sec,
            )
            if started:
                logger.info("[%s] Maintenance accepted -> entering smooth recovery", self.machine_id)
                self.publish_status(reason="maintenance_accepted")
            else:
                logger.info(
                    "[%s] Maintenance command ignored (already NORMAL/RECOVERING or duplicate action_id)",
                    self.machine_id,
                )
            return True

        if cmd in ("simulate_crash", "crash", "disconnect_ungraceful"):
            # Run crash on a short helper thread so we do not join loop_stop inside Paho's callback thread
            threading.Thread(target=self.simulate_crash, daemon=True).start()
            return True

        if cmd == "reconnect":
            threading.Thread(target=self.reconnect, daemon=True).start()
            return True

        logger.warning("[%s] Unknown command payload: %s", self.machine_id, raw)
        return False


class SupervisorCommandRouter:
    """Lightweight MQTT listener that routes 'reconnect' commands to crashed pumps."""

    def __init__(
        self,
        workers: Dict[str, PumpMQTTWorker],
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.workers = workers
        self.host = host
        self.port = port
        self._client = create_mqtt_client(client_id="sim-supervisor", clean_session=True)
        if username:
            self._client.username_pw_set(username, password)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def start(self) -> None:
        self._client.connect(self.host, self.port, keepalive=DEFAULT_MQTT_KEEPALIVE)
        self._client.loop_start()

    def stop(self) -> None:
        try:
            self._client.disconnect()
        finally:
            self._client.loop_stop()

    def _on_connect(self, client: mqtt.Client, userdata: Any, *args: Any) -> None:
        # Single-level '+' already covers both individual machines and 'all/command'
        client.subscribe(f"{TOPIC_PREFIX}/+/command", qos=1)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        raw = msg.payload.decode("utf-8", errors="replace").strip()
        try:
            data = json.loads(raw) if raw.startswith("{") else {"command": raw}
        except json.JSONDecodeError:
            return
        cmd = str(data.get("command") or data.get("action") or data.get("cmd") or "").strip().lower()
        if cmd != "reconnect":
            return

        parts = msg.topic.split("/")
        target_id = parts[3] if len(parts) >= 5 else "all"
        targets = (
            list(self.workers.values())
            if target_id == "all"
            else [self.workers[target_id]] if target_id in self.workers else []
        )
        for worker in targets:
            if worker.is_crashed:
                threading.Thread(target=worker.reconnect, daemon=True).start()
