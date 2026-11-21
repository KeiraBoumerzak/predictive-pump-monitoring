"""Main entry point for running the three virtual motor-pump units."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from typing import Any, Dict

# Allow direct execution via `python simulator/run_simulator.py` as well as `-m simulator.run_simulator`
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator.config import (
    DEFAULT_INTERVAL_SEC,
    DEFAULT_MQTT_HOST,
    DEFAULT_MQTT_KEEPALIVE,
    DEFAULT_MQTT_PASSWORD,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_USERNAME,
    DEFAULT_RAMP_SEC,
    DEFAULT_RECOVERY_SEC,
    PUMP_ASSETS,
    VALID_SCENARIOS,
)
from simulator.pump_model import VirtualPump


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the 3-pump IoT telemetry simulator over MQTT."
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_MQTT_HOST,
        help=f"MQTT broker hostname (default: {DEFAULT_MQTT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_MQTT_PORT,
        help=f"MQTT broker port (default: {DEFAULT_MQTT_PORT})",
    )
    parser.add_argument(
        "--username",
        default=DEFAULT_MQTT_USERNAME,
        help="Optional MQTT username",
    )
    parser.add_argument(
        "--password",
        default=DEFAULT_MQTT_PASSWORD,
        help="Optional MQTT password",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SEC,
        help=f"Telemetry sampling interval in seconds (default: {DEFAULT_INTERVAL_SEC})",
    )
    parser.add_argument(
        "--ramp-sec",
        type=float,
        default=DEFAULT_RAMP_SEC,
        help=f"Fault progression ramp duration in seconds (default: {DEFAULT_RAMP_SEC})",
    )
    parser.add_argument(
        "--recovery-sec",
        type=float,
        default=DEFAULT_RECOVERY_SEC,
        help=f"Maintenance recovery duration in seconds (default: {DEFAULT_RECOVERY_SEC})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for deterministic/reproducible runs",
    )
    parser.add_argument(
        "--initial-scenario",
        action="append",
        default=[],
        metavar="PUMP=SCENARIO",
        help=(
            "Optional initial scenario assignment, e.g. --initial-scenario PUMP-03=BEARING_WEAR "
            f"(choices: {', '.join(VALID_SCENARIOS)})"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run physics simulation locally without connecting to an MQTT broker",
    )
    parser.add_argument(
        "--max-ticks",
        type=int,
        default=0,
        help="Stop automatically after N ticks (0 = run indefinitely)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-tick telemetry summary lines in console",
    )
    return parser.parse_args(argv)


def _apply_initial_scenarios(
    pumps: Dict[str, VirtualPump],
    assignments: list[str],
) -> None:
    for item in assignments:
        if "=" not in item:
            raise ValueError(
                f"Invalid --initial-scenario '{item}'. Expected format PUMP-01=BEARING_WEAR."
            )
        pump_id, scenario = [part.strip().upper() for part in item.split("=", 1)]
        if pump_id not in pumps:
            raise ValueError(
                f"Unknown machine_id '{pump_id}'. Valid IDs: {', '.join(pumps.keys())}"
            )
        pumps[pump_id].set_scenario(scenario)


def _format_tick_line(
    tick: int,
    pumps: Dict[str, VirtualPump],
    workers: Dict[str, Any],
    last_samples: Dict[str, Dict[str, Any]],
    dry_run: bool,
) -> str:
    segments: list[str] = []
    for machine_id in pumps:
        worker = workers.get(machine_id)
        if not dry_run and worker is not None:
            if worker.is_crashed:
                segments.append(f"{machine_id}[OFFLINE/LWT]")
                continue
            if not worker.is_connected:
                segments.append(f"{machine_id}[DISCONNECTED]")
                continue

        sample = last_samples.get(machine_id)
        if sample is None:
            continue

        mode_tag = (
            "RECOVERING"
            if sample["machine_status"] == "RECOVERING"
            else sample["sim_scenario"]
        )
        segments.append(
            f"{machine_id}[{mode_tag:<16}] "
            f"T={sample['temperature_c']:5.1f}C "
            f"V={sample['vibration_rms']:4.2f}mm/s "
            f"I={sample['current_a']:5.2f}A "
            f"RPM={sample['rpm']:4d}"
        )
    return f"[Tick {tick:04d}] " + " | ".join(segments)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("simulator")

    # Instantiate the three virtual motor-pump units
    pumps: Dict[str, VirtualPump] = {}
    for idx, (machine_id, profile) in enumerate(PUMP_ASSETS.items()):
        pump_seed = (args.seed + idx * 101) if args.seed is not None else None
        pumps[machine_id] = VirtualPump(
            profile=profile,
            ramp_sec=args.ramp_sec,
            recovery_sec=args.recovery_sec,
            seed=pump_seed,
        )

    try:
        _apply_initial_scenarios(pumps, args.initial_scenario)
    except ValueError as exc:
        log.error("%s", exc)
        return 2

    workers: Dict[str, Any] = {}
    last_samples: Dict[str, Dict[str, Any]] = {}
    supervisor: Any = None

    stop_requested = False

    def _request_stop(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    if not args.dry_run:
        try:
            from simulator.mqtt_worker import PumpMQTTWorker, SupervisorCommandRouter
        except ModuleNotFoundError:
            log.error(
                "Missing dependency 'paho-mqtt'. Install it via `pip install -r requirements.txt` "
                "or run with `--dry-run`."
            )
            return 2

        for machine_id, pump in pumps.items():
            workers[machine_id] = PumpMQTTWorker(
                pump=pump,
                host=args.host,
                port=args.port,
                username=args.username,
                password=args.password,
                keepalive=DEFAULT_MQTT_KEEPALIVE,
            )

        log.info(
            "Connecting 3 virtual motor-pump units to MQTT broker at %s:%d ...",
            args.host,
            args.port,
        )
        try:
            for worker in workers.values():
                worker.start()
            supervisor = SupervisorCommandRouter(
                workers=workers,
                host=args.host,
                port=args.port,
                username=args.username,
                password=args.password,
            )
            supervisor.start()
            # Brief pause so initial on_connect + retained ONLINE status completes before tick 1
            time.sleep(0.25)
        except Exception as exc:
            log.error(
                "Could not connect to MQTT broker at %s:%d (%s). "
                "Start Mosquitto first or pass --dry-run to test locally.",
                args.host,
                args.port,
                exc,
            )
            return 1
    else:
        log.info("Starting simulator in --dry-run mode (no MQTT broker required).")

    interval = max(0.1, float(args.interval))
    tick = 0
    last_step_ts = time.monotonic()

    try:
        while not stop_requested:
            now = time.monotonic()
            dt = interval if tick == 0 else max(0.05, now - last_step_ts)
            last_step_ts = now
            tick += 1

            for machine_id, pump in pumps.items():
                if args.dry_run:
                    telemetry, _ = pump.step(dt)
                    last_samples[machine_id] = telemetry
                else:
                    sample = workers[machine_id].step_and_publish(dt)
                    if sample is not None:
                        last_samples[machine_id] = sample

            if not args.quiet:
                print(
                    _format_tick_line(tick, pumps, workers, last_samples, args.dry_run),
                    flush=True,
                )

            if args.max_ticks > 0 and tick >= args.max_ticks:
                log.info("Reached --max-ticks=%d, stopping simulator.", args.max_ticks)
                break

            elapsed = time.monotonic() - now
            sleep_for = max(0.0, interval - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        if not args.dry_run:
            log.info("Shutting down virtual assets gracefully...")
            if supervisor is not None:
                supervisor.stop()
            for worker in workers.values():
                worker.stop_gracefully()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
