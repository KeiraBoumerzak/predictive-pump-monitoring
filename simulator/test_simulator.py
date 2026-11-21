"""Automated unit and MQTT protocol tests for the Member 1 pump simulator."""

from __future__ import annotations

import importlib.util
import json
import os
import socket
import struct
import sys
import threading
import time
import unittest
from typing import Dict, List, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator.config import PUMP_ASSETS
from simulator.pump_model import VirtualPump

HAS_PAHO = importlib.util.find_spec("paho") is not None


def _linear_r_squared(values: List[float]) -> Tuple[float, float]:
    """Compute (slope, R^2) for equally spaced samples x = 0..n-1."""
    n = len(values)
    mean_x = (n - 1) / 2.0
    mean_y = sum(values) / n
    ss_xx = sum((i - mean_x) ** 2 for i in range(n))
    ss_xy = sum((i - mean_x) * (y - mean_y) for i, y in enumerate(values))
    ss_yy = sum((y - mean_y) ** 2 for y in values)
    if ss_xx == 0.0 or ss_yy == 0.0:
        return 0.0, 0.0
    slope = ss_xy / ss_xx
    r_sq = (ss_xy ** 2) / (ss_xx * ss_yy)
    return slope, r_sq


class TestVirtualPumpPhysics(unittest.TestCase):
    """Verify operating bounds, fault progression, R^2 trend quality, and recovery."""

    def test_normal_mode_stays_within_bounds_and_flat_trend(self) -> None:
        pump = VirtualPump(PUMP_ASSETS["PUMP-01"], seed=42)
        temps, vibs = [], []
        for _ in range(300):  # 5 minutes at 1s sampling (Proposal Test P1)
            sample, changed = pump.step(1.0)
            self.assertFalse(changed)
            self.assertEqual(sample["machine_status"], "RUNNING")
            self.assertEqual(sample["sim_scenario"], "NORMAL")
            self.assertLessEqual(sample["temperature_c"], 70.0)
            self.assertLessEqual(sample["vibration_rms"], 3.0)
            self.assertLessEqual(sample["current_a"], 15.0)
            self.assertTrue(1400 <= sample["rpm"] <= 1500)
            temps.append(sample["temperature_c"])
            vibs.append(sample["vibration_rms"])

        # Trend on last 20 normal samples must NOT pass R^2 >= 0.70 (TTC -> Unknown)
        _, r2_temp = _linear_r_squared(temps[-20:])
        _, r2_vib = _linear_r_squared(vibs[-20:])
        self.assertLess(r2_temp, 0.70)
        self.assertLess(r2_vib, 0.70)

    def test_bearing_wear_ramps_cleanly_for_ttc_and_dominates_vibration(self) -> None:
        pump = VirtualPump(PUMP_ASSETS["PUMP-03"], ramp_sec=40.0, seed=7)
        pump.set_scenario("BEARING_WEAR")

        vibs = []
        last_sample = {}
        for _ in range(42):
            last_sample, _ = pump.step(1.0)
            vibs.append(last_sample["vibration_rms"])

        slope, r2 = _linear_r_squared(vibs[:25])
        self.assertGreater(slope, 0.10)
        self.assertGreaterEqual(r2, 0.90)  # Easily clears the R^2 >= 0.70 TTC threshold
        self.assertGreater(last_sample["vibration_rms"], 6.0)   # Critical vibration
        self.assertGreater(last_sample["temperature_c"], 80.0)  # Secondary friction heat
        self.assertLessEqual(last_sample["current_a"], 15.0)    # Current stays normal
        self.assertTrue(1400 <= last_sample["rpm"] <= 1500)     # RPM stays normal

    def test_overheating_and_overload_and_combined_reach_critical(self) -> None:
        # OVERHEATING
        p_heat = VirtualPump(PUMP_ASSETS["PUMP-02"], ramp_sec=30.0, seed=11)
        p_heat.set_scenario("OVERHEATING")
        for _ in range(32):
            s_heat, _ = p_heat.step(1.0)
        self.assertGreater(s_heat["temperature_c"], 95.0)
        self.assertLessEqual(s_heat["vibration_rms"], 3.0)
        self.assertLessEqual(s_heat["current_a"], 15.0)

        # OVERLOAD
        p_load = VirtualPump(PUMP_ASSETS["PUMP-02"], ramp_sec=30.0, seed=19)
        p_load.set_scenario("OVERLOAD")
        for _ in range(32):
            s_load, _ = p_load.step(1.0)
        self.assertGreater(s_load["current_a"], 20.0)
        self.assertLess(s_load["rpm"], 1350)
        self.assertLessEqual(s_load["vibration_rms"], 3.0)  # Vibration stays strictly normal under overload

        # COMBINED_FAILURE
        p_comb = VirtualPump(PUMP_ASSETS["PUMP-03"], ramp_sec=30.0, seed=23)
        p_comb.set_scenario("COMBINED_FAILURE")
        for _ in range(30):
            s_comb, _ = p_comb.step(1.0)
        self.assertGreater(s_comb["temperature_c"], 95.0)
        self.assertGreater(s_comb["vibration_rms"], 8.0)
        self.assertGreater(s_comb["current_a"], 22.0)
        self.assertLess(s_comb["rpm"], 1330)

    def test_smooth_recovery_after_maintenance_accepted(self) -> None:
        pump = VirtualPump(PUMP_ASSETS["PUMP-03"], ramp_sec=20.0, recovery_sec=10.0, seed=5)
        pump.set_scenario("COMBINED_FAILURE")
        for _ in range(22):
            degraded, _ = pump.step(1.0)
        self.assertGreater(degraded["vibration_rms"], 8.0)

        _, started = pump.accept_maintenance(action_id="ACT-TEST", recovery_sec=10.0)
        self.assertTrue(started)
        mid_sample, changed_mid = pump.step(5.0)
        self.assertFalse(changed_mid)
        self.assertEqual(mid_sample["machine_status"], "RECOVERING")
        # Duplicate QoS 1 accept_maintenance while RECOVERING must be idempotent
        dup_snap, dup_started = pump.accept_maintenance(action_id="ACT-TEST")
        self.assertFalse(dup_started)
        self.assertEqual(dup_snap["maintenance_count"], 1)
        self.assertAlmostEqual(pump.elapsed_in_mode_sec, 5.0)
        # Halfway through recovery: lower than peak fault, higher than baseline
        self.assertLess(mid_sample["vibration_rms"], degraded["vibration_rms"])
        self.assertGreater(mid_sample["vibration_rms"], 2.5)

        final_sample, changed_end = pump.step(6.0)
        self.assertTrue(changed_end)
        self.assertEqual(final_sample["machine_status"], "RUNNING")
        self.assertEqual(final_sample["sim_scenario"], "NORMAL")
        self.assertLessEqual(final_sample["vibration_rms"], 3.0)
        self.assertLessEqual(final_sample["temperature_c"], 70.0)

        # Calling accept_maintenance on an already-healthy NORMAL pump is a clean no-op
        noop_snap, noop_started = pump.accept_maintenance(action_id="ACT-NOOP")
        self.assertFalse(noop_started)
        self.assertEqual(noop_snap["machine_status"], "RUNNING")
        self.assertEqual(noop_snap["maintenance_count"], 1)

        # Custom recovery_sec must only apply to that recovery, not overwrite default_recovery_sec
        pump.set_scenario("OVERHEATING")
        pump.step(5.0)
        _, started_fast = pump.accept_maintenance(action_id="ACT-FAST", recovery_sec=4.0)
        self.assertTrue(started_fast)
        self.assertAlmostEqual(pump.active_recovery_sec, 4.0)
        self.assertAlmostEqual(pump.default_recovery_sec, 10.0)

    @unittest.skipUnless(HAS_PAHO, "paho-mqtt not installed in current Python environment")
    def test_idempotent_maintenance_command_does_not_publish_duplicate_status(self) -> None:
        from simulator.mqtt_worker import PumpMQTTWorker

        pump = VirtualPump(PUMP_ASSETS["PUMP-01"], ramp_sec=20.0, recovery_sec=10.0, seed=3)
        worker = PumpMQTTWorker(pump, host="127.0.0.1", port=1883)
        published_reasons: List[str] = []
        worker.publish_status = lambda reason="state_update": published_reasons.append(reason)  # type: ignore[method-assign]

        # Healthy NORMAL pump ignores maintenance without emitting /status
        worker.handle_command_payload(json.dumps({"command": "accept_maintenance", "action_id": "ACT-0"}))
        self.assertEqual(published_reasons, [])

        # Faulted pump accepts once, then ignores QoS 1 duplicate without emitting second /status
        pump.set_scenario("BEARING_WEAR")
        pump.step(5.0)
        worker.handle_command_payload(json.dumps({"command": "accept_maintenance", "action_id": "ACT-1"}))
        worker.handle_command_payload(json.dumps({"command": "accept_maintenance", "action_id": "ACT-1"}))
        self.assertEqual(published_reasons, ["maintenance_accepted"])


# ---------------------------------------------------------------------------
# Minimal Embedded MQTT 3.1.1 Broker for Self-Contained LWT/Retain Testing
# ---------------------------------------------------------------------------
class _MiniMQTTBroker:
    """Lightweight in-process MQTT 3.1.1 test broker supporting Retain, QoS 0/1, and LWT."""

    def __init__(self) -> None:
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self.port: int = self._srv.getsockname()[1]
        self._srv.listen(16)
        self._stop = False
        self._lock = threading.Lock()
        self.retained: Dict[str, bytes] = {}
        self.published_log: List[Tuple[str, str, bool]] = []
        self._subs: List[Tuple[socket.socket, str]] = []
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop = True
        try:
            self._srv.close()
        except OSError:
            pass

    @staticmethod
    def _recv_exact(conn: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("EOF")
            buf += chunk
        return buf

    @classmethod
    def _read_packet(cls, conn: socket.socket) -> Tuple[int, bytes]:
        first = cls._recv_exact(conn, 1)[0]
        mult = 1
        rem_len = 0
        while True:
            b = cls._recv_exact(conn, 1)[0]
            rem_len += (b & 0x7F) * mult
            if (b & 0x80) == 0:
                break
            mult *= 128
        body = cls._recv_exact(conn, rem_len) if rem_len else b""
        return first, body

    @staticmethod
    def _encode_packet(header_byte: int, body: bytes) -> bytes:
        out = bytearray([header_byte])
        x = len(body)
        while True:
            enc = x % 128
            x //= 128
            if x > 0:
                enc |= 0x80
            out.append(enc)
            if x == 0:
                break
        out.extend(body)
        return bytes(out)

    @staticmethod
    def _topic_matches(pattern: str, topic: str) -> bool:
        p_parts = pattern.split("/")
        t_parts = topic.split("/")
        for idx, p in enumerate(p_parts):
            if p == "#":
                return True
            if idx >= len(t_parts):
                return False
            if p != "+" and p != t_parts[idx]:
                return False
        return len(p_parts) == len(t_parts)

    def _fanout(self, topic: str, payload: bytes, retain: bool) -> None:
        with self._lock:
            if retain:
                self.retained[topic] = payload
            self.published_log.append((topic, payload.decode("utf-8", errors="replace"), retain))
            targets = [c for c, pat in self._subs if self._topic_matches(pat, topic)]

        t_bytes = topic.encode("utf-8")
        pkt = self._encode_packet(0x30 | (0x01 if retain else 0x00), struct.pack("!H", len(t_bytes)) + t_bytes + payload)
        for c in targets:
            try:
                c.sendall(pkt)
            except OSError:
                pass

    def _accept_loop(self) -> None:
        while not self._stop:
            try:
                conn, _ = self._srv.accept()
            except OSError:
                break
            threading.Thread(target=self._client_loop, args=(conn,), daemon=True).start()

    def _client_loop(self, conn: socket.socket) -> None:
        will_info: Tuple[str, bytes, bool] | None = None
        clean_disconnect = False
        try:
            while not self._stop:
                first, body = self._read_packet(conn)
                pkt_type = first >> 4

                if pkt_type == 1:  # CONNECT
                    idx = 0
                    proto_len = struct.unpack("!H", body[idx : idx + 2])[0]
                    idx += 2 + proto_len + 1  # skip protocol name + level
                    flags = body[idx]
                    idx += 1 + 2  # skip connect flags + keepalive
                    cid_len = struct.unpack("!H", body[idx : idx + 2])[0]
                    idx += 2 + cid_len
                    if flags & 0x04:  # Will flag set
                        wt_len = struct.unpack("!H", body[idx : idx + 2])[0]
                        idx += 2
                        w_topic = body[idx : idx + wt_len].decode("utf-8")
                        idx += wt_len
                        wm_len = struct.unpack("!H", body[idx : idx + 2])[0]
                        idx += 2
                        w_msg = body[idx : idx + wm_len]
                        w_retain = bool(flags & 0x20)
                        will_info = (w_topic, w_msg, w_retain)
                    conn.sendall(b"\x20\x02\x00\x00")  # CONNACK

                elif pkt_type == 3:  # PUBLISH
                    qos = (first >> 1) & 0x03
                    retain = bool(first & 0x01)
                    t_len = struct.unpack("!H", body[0:2])[0]
                    topic = body[2 : 2 + t_len].decode("utf-8")
                    idx = 2 + t_len
                    if qos > 0:
                        pkt_id = body[idx : idx + 2]
                        idx += 2
                        conn.sendall(b"\x40\x02" + pkt_id)  # PUBACK
                    payload = body[idx:]
                    self._fanout(topic, payload, retain=False)
                    if retain:
                        with self._lock:
                            self.retained[topic] = payload

                elif pkt_type == 8:  # SUBSCRIBE
                    pkt_id = body[0:2]
                    idx = 2
                    grants = bytearray()
                    while idx < len(body):
                        f_len = struct.unpack("!H", body[idx : idx + 2])[0]
                        idx += 2
                        pat = body[idx : idx + f_len].decode("utf-8")
                        idx += f_len + 1
                        grants.append(0x01)
                        with self._lock:
                            self._subs.append((conn, pat))
                            matching_retained = [
                                (t, p)
                                for t, p in self.retained.items()
                                if self._topic_matches(pat, t)
                            ]
                    conn.sendall(self._encode_packet(0x90, pkt_id + bytes(grants)))
                    for t, p in matching_retained:
                        tb = t.encode("utf-8")
                        conn.sendall(self._encode_packet(0x31, struct.pack("!H", len(tb)) + tb + p))

                elif pkt_type == 12:  # PINGREQ
                    conn.sendall(b"\xD0\x00")

                elif pkt_type == 14:  # DISCONNECT
                    clean_disconnect = True
                    break
        except (ConnectionError, OSError):
            pass
        finally:
            with self._lock:
                self._subs = [(c, pat) for c, pat in self._subs if c is not conn]
            try:
                conn.close()
            except OSError:
                pass
            if not clean_disconnect and will_info is not None and not self._stop:
                w_topic, w_msg, w_retain = will_info
                self._fanout(w_topic, w_msg, retain=w_retain)


@unittest.skipUnless(HAS_PAHO, "paho-mqtt not installed in current Python environment")
class TestMQTTIntegrationAndLWT(unittest.TestCase):
    """End-to-end test of retained status, commands, single-pump LWT crash, and reconnect."""

    def test_mqtt_worker_lifecycle_and_lwt(self) -> None:
        from simulator.mqtt_worker import PumpMQTTWorker, SupervisorCommandRouter, create_mqtt_client

        broker = _MiniMQTTBroker()
        broker.start()
        try:
            pump1 = VirtualPump(PUMP_ASSETS["PUMP-01"], seed=1)
            pump2 = VirtualPump(PUMP_ASSETS["PUMP-02"], seed=2)
            w1 = PumpMQTTWorker(pump1, host="127.0.0.1", port=broker.port)
            w2 = PumpMQTTWorker(pump2, host="127.0.0.1", port=broker.port)
            router = SupervisorCommandRouter({"PUMP-01": w1, "PUMP-02": w2}, "127.0.0.1", broker.port)

            w1.start()
            w2.start()
            router.start()
            time.sleep(0.25)

            self.assertTrue(w1.is_connected)
            self.assertTrue(w2.is_connected)

            # Retained ONLINE status must exist for both pumps
            self.assertIn("factory/site01/machine/PUMP-01/status", broker.retained)
            st1 = json.loads(broker.retained["factory/site01/machine/PUMP-01/status"].decode())
            self.assertEqual(st1["connectivity"], "ONLINE")

            # Publish telemetry tick
            t1 = w1.step_and_publish(1.0)
            self.assertIsNotNone(t1)

            # Send fault injection command to PUMP-01 via MQTT
            helper = create_mqtt_client("test-cmd")
            helper.connect("127.0.0.1", broker.port, 10)
            helper.loop_start()
            helper.publish(
                "factory/site01/machine/PUMP-01/command",
                json.dumps({"command": "set_scenario", "scenario": "OVERHEATING"}),
                qos=1,
            ).wait_for_publish(2.0)
            time.sleep(0.15)
            self.assertEqual(pump1.sim_scenario, "OVERHEATING")

            # Crash PUMP-02 ungracefully -> Broker must fire LWT OFFLINE for PUMP-02 while PUMP-01 stays ONLINE
            w2.simulate_crash()
            time.sleep(0.25)
            st2_lwt = json.loads(broker.retained["factory/site01/machine/PUMP-02/status"].decode())
            self.assertEqual(st2_lwt["connectivity"], "OFFLINE")
            self.assertEqual(st2_lwt["reason"], "unexpected_disconnect_lwt")
            self.assertTrue(w1.is_connected)

            # Reconnect PUMP-02 remotely via SupervisorCommandRouter
            helper.publish(
                "factory/site01/machine/PUMP-02/command",
                json.dumps({"command": "reconnect"}),
                qos=1,
            ).wait_for_publish(2.0)
            time.sleep(0.35)
            st2_back = json.loads(broker.retained["factory/site01/machine/PUMP-02/status"].decode())
            self.assertEqual(st2_back["connectivity"], "ONLINE")

            # Verify sim_cli QoS 0 bad-payload & trailing --port/--host CLI flag support
            from simulator import sim_cli

            rc_bad = sim_cli.main(
                [
                    "bad-payload",
                    "PUMP-01",
                    "--kind",
                    "missing_id",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(broker.port),
                ]
            )
            self.assertEqual(rc_bad, 0)
            time.sleep(0.1)
            bad_events = [
                json.loads(payload)
                for topic, payload, _ in broker.published_log
                if topic == "factory/site01/machine/PUMP-01/telemetry" and "machine_id" not in payload
            ]
            self.assertEqual(len(bad_events), 1)

            helper.disconnect()
            helper.loop_stop()
            router.stop()
            w1.stop_gracefully()
            w2.stop_gracefully()
        finally:
            broker.stop()


if __name__ == "__main__":
    unittest.main()
