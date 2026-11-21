"""CLI control and test harness for the virtual pump MQTT simulator.

Supports:
  - Fault scenario switching (NORMAL, BEARING_WEAR, OVERHEATING, OVERLOAD, COMBINED_FAILURE)
  - Maintenance acceptance (closed-loop smooth recovery)
  - Ungraceful single-pump crash simulation (verifies MQTT LWT) & reconnect
  - Malformed telemetry injection (verifies Node-RED P5 validation/quarantine)
  - Live MQTT traffic monitor (shows QoS, [RETAINED] flag, LWT, and telemetry)
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import threading
import time
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import paho.mqtt.client as mqtt

from simulator.config import (
    DEFAULT_MQTT_HOST,
    DEFAULT_MQTT_PASSWORD,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_USERNAME,
    PUMP_ASSETS,
    TOPIC_PREFIX,
    VALID_SCENARIOS,
    broadcast_command_topic,
    command_topic,
    telemetry_topic,
)
from simulator.mqtt_worker import create_mqtt_client

VALID_TARGETS = tuple(PUMP_ASSETS.keys()) + ("all",)


def _resolve_command_topic(target: str) -> str:
    norm = target.strip().upper()
    if norm == "ALL":
        return broadcast_command_topic()
    if norm not in PUMP_ASSETS:
        raise ValueError(
            f"Unknown target '{target}'. Valid targets: {', '.join(PUMP_ASSETS.keys())}, all"
        )
    return command_topic(norm)


def _publish_once(
    args: argparse.Namespace,
    topic: str,
    payload: str,
    qos: int = 1,
    retain: bool = False,
) -> None:
    connected_evt = threading.Event()
    connect_err: list[str] = []

    def on_connect(client: mqtt.Client, userdata: Any, *cb_args: Any) -> None:
        rc = cb_args[1] if len(cb_args) >= 2 else 0
        failed = (rc != 0) if isinstance(rc, int) else getattr(rc, "is_failure", False)
        if failed:
            connect_err.append(f"CONNACK rejected (code={rc})")
        connected_evt.set()

    client = create_mqtt_client(client_id=f"sim-cli-{os.getpid()}", clean_session=True)
    if args.username:
        client.username_pw_set(args.username, args.password)
    client.on_connect = on_connect

    client.connect(args.host, args.port, keepalive=10)
    client.loop_start()
    try:
        if not connected_evt.wait(timeout=3.0):
            raise TimeoutError("Timed out waiting for MQTT CONNACK")
        if connect_err:
            raise RuntimeError(connect_err[0])

        info = client.publish(topic, payload, qos=qos, retain=retain)
        info.wait_for_publish(timeout=3.0)
        print(f"Published (QoS {qos}, retain={retain}) -> {topic}")
        print(f"  Payload: {payload}")
    finally:
        client.disconnect()
        client.loop_stop()


def cmd_scenario(args: argparse.Namespace) -> int:
    topic = _resolve_command_topic(args.target)
    body: dict[str, Any] = {
        "command": "set_scenario",
        "scenario": args.scenario.upper(),
    }
    if args.ramp_sec is not None:
        body["ramp_sec"] = args.ramp_sec
    _publish_once(args, topic, json.dumps(body), qos=1, retain=False)
    return 0


def cmd_maintain(args: argparse.Namespace) -> int:
    topic = _resolve_command_topic(args.target)
    body: dict[str, Any] = {"command": "accept_maintenance"}
    if args.action_id:
        body["action_id"] = args.action_id
    if args.recovery_sec is not None:
        body["recovery_sec"] = args.recovery_sec
    _publish_once(args, topic, json.dumps(body), qos=1, retain=False)
    return 0


def cmd_crash(args: argparse.Namespace) -> int:
    norm = args.target.strip().upper()
    if norm not in PUMP_ASSETS:
        raise ValueError(
            f"Crash target must be a specific pump ({', '.join(PUMP_ASSETS.keys())})."
        )
    topic = command_topic(norm)
    body = {"command": "simulate_crash"}
    _publish_once(args, topic, json.dumps(body), qos=1, retain=False)
    return 0


def cmd_reconnect(args: argparse.Namespace) -> int:
    topic = _resolve_command_topic(args.target)
    body = {"command": "reconnect"}
    _publish_once(args, topic, json.dumps(body), qos=1, retain=False)
    return 0


def cmd_bad_payload(args: argparse.Namespace) -> int:
    pump_id = args.target.strip().upper()
    topic = telemetry_topic(pump_id)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    if args.kind == "missing_id":
        # Missing mandatory machine_id field (Proposal Test P5)
        payload = json.dumps(
            {
                "timestamp": now_iso,
                "temperature_c": 71.2,
                "vibration_rms": 3.1,
                "current_a": 13.0,
                "rpm": 1470,
            }
        )
    elif args.kind == "non_numeric":
        # String sensor value instead of numeric float
        payload = json.dumps(
            {
                "machine_id": pump_id,
                "timestamp": now_iso,
                "temperature_c": "NaN_SENSOR_ERR",
                "vibration_rms": None,
                "current_a": 12.5,
                "rpm": 1475,
            }
        )
    else:
        # Truncated/unparseable JSON syntax
        payload = '{"machine_id": "' + pump_id + '", "temperature_c": 85.0, BROKEN_JSON'

    _publish_once(args, topic, payload, qos=0, retain=False)
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    sub_topic = (
        f"{TOPIC_PREFIX}/+/status"
        if args.status_only
        else f"{TOPIC_PREFIX}/+/#"
    )

    def on_connect(client: mqtt.Client, userdata: Any, *cb_args: Any) -> None:
        client.subscribe(sub_topic, qos=1)
        print(f"Subscribed to '{sub_topic}' on {args.host}:{args.port} (Ctrl+C to stop)...")

    def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        retain_tag = " [RETAINED]" if msg.retain else ""
        text = msg.payload.decode("utf-8", errors="replace")
        print(f"[QoS {msg.qos}]{retain_tag} {msg.topic} -> {text}", flush=True)

    client = create_mqtt_client(client_id=f"sim-monitor-{os.getpid()}", clean_session=True)
    if args.username:
        client.username_pw_set(args.username, args.password)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.host, args.port, keepalive=15)
    client.loop_start()
    start_ts = time.monotonic()
    try:
        while True:
            time.sleep(0.2)
            if args.duration > 0 and (time.monotonic() - start_ts) >= args.duration:
                break
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
        client.loop_stop()
    return 0


def build_parser() -> argparse.ArgumentParser:
    # Shared connection flags accepted either before or after the subcommand
    conn_parent = argparse.ArgumentParser(add_help=False)
    conn_parent.add_argument(
        "--host",
        default=argparse.SUPPRESS,
        help=f"MQTT broker host (default: {DEFAULT_MQTT_HOST})",
    )
    conn_parent.add_argument(
        "--port",
        type=int,
        default=argparse.SUPPRESS,
        help=f"MQTT broker port (default: {DEFAULT_MQTT_PORT})",
    )
    conn_parent.add_argument("--username", default=argparse.SUPPRESS)
    conn_parent.add_argument("--password", default=argparse.SUPPRESS)

    parser = argparse.ArgumentParser(
        parents=[conn_parent],
        description="Control scenarios, maintenance actions, LWT tests, and bad-payload tests.",
    )
    parser.set_defaults(
        host=DEFAULT_MQTT_HOST,
        port=DEFAULT_MQTT_PORT,
        username=DEFAULT_MQTT_USERNAME,
        password=DEFAULT_MQTT_PASSWORD,
    )

    sub = parser.add_subparsers(dest="subcommand", required=True)

    # scenario
    p_scen = sub.add_parser(
        "scenario",
        parents=[conn_parent],
        help="Set operating or fault scenario on a pump (or all)",
    )
    p_scen.add_argument("target", help="PUMP-01 | PUMP-02 | PUMP-03 | all")
    p_scen.add_argument(
        "scenario",
        choices=[s.lower() for s in VALID_SCENARIOS] + list(VALID_SCENARIOS),
        help="Target scenario",
    )
    p_scen.add_argument("--ramp-sec", type=float, default=None, help="Optional custom ramp seconds")
    p_scen.set_defaults(func=cmd_scenario)

    # maintain
    p_maint = sub.add_parser(
        "maintain",
        parents=[conn_parent],
        help="Send accept_maintenance command to recover a pump",
    )
    p_maint.add_argument("target", help="PUMP-01 | PUMP-02 | PUMP-03 | all")
    p_maint.add_argument("--action-id", default=None, help="Optional maintenance action ID")
    p_maint.add_argument("--recovery-sec", type=float, default=None, help="Optional recovery duration")
    p_maint.set_defaults(func=cmd_maintain)

    # crash
    p_crash = sub.add_parser(
        "crash",
        parents=[conn_parent],
        help="Force ungraceful TCP disconnect on one pump to trigger broker LWT",
    )
    p_crash.add_argument("target", help="PUMP-01 | PUMP-02 | PUMP-03")
    p_crash.set_defaults(func=cmd_crash)

    # reconnect
    p_rec = sub.add_parser(
        "reconnect",
        parents=[conn_parent],
        help="Reconnect a crashed pump back to ONLINE",
    )
    p_rec.add_argument("target", help="PUMP-01 | PUMP-02 | PUMP-03 | all")
    p_rec.set_defaults(func=cmd_reconnect)

    # bad-payload
    p_bad = sub.add_parser(
        "bad-payload",
        parents=[conn_parent],
        help="Publish a malformed telemetry event to test Node-RED validation (Test P5)",
    )
    p_bad.add_argument("target", nargs="?", default="PUMP-01", help="Target machine topic")
    p_bad.add_argument(
        "--kind",
        choices=["missing_id", "non_numeric", "corrupt_json"],
        default="missing_id",
        help="Type of malformed telemetry payload",
    )
    p_bad.set_defaults(func=cmd_bad_payload)

    # monitor
    p_mon = sub.add_parser(
        "monitor",
        parents=[conn_parent],
        help="Subscribe and inspect live MQTT topics, QoS, and LWT",
    )
    p_mon.add_argument(
        "--status-only",
        action="store_true",
        help="Only display retained/live /status messages",
    )
    p_mon.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Auto-exit after N seconds (0 = until Ctrl+C)",
    )
    p_mon.set_defaults(func=cmd_monitor)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            f"MQTT error ({args.host}:{args.port}): {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
