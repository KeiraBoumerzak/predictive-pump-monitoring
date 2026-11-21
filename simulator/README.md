# Virtual Asset Simulator & MQTT Communication Layer (Member 1)

Simulates three industrial centrifugal water pump units driven by three-phase induction motors (`PUMP-01`, `PUMP-02`, `PUMP-03`), publishing live telemetry, retained device status, and Last Will and Testament (LWT) events over MQTT, with closed-loop maintenance recovery.

---

## 1. Module Structure

| File | Responsibility |
| :--- | :--- |
| `simulator/config.py` | Asset registry (`PUMP-01..03`), nominal baselines, reference thresholds, fault profiles, and MQTT topic builders |
| `simulator/pump_model.py` | Thread-safe physical state machine (`NORMAL`, 4 fault scenarios, and smooth `RECOVERING` transition) |
| `simulator/mqtt_worker.py` | Dedicated MQTT client per pump (QoS 0 telemetry, QoS 1 retained status, LWT, command listener, ungraceful crash simulation) |
| `simulator/run_simulator.py` | Main simulator process running all 3 virtual motor-pump units concurrently |
| `simulator/sim_cli.py` | CLI control & verification tool (fault injection, maintenance acceptance, LWT crash/reconnect, bad-payload test, live MQTT monitor) |
| `simulator/test_simulator.py` | Automated unit & embedded-MQTT 3.1.1 protocol tests (physics, `R^2`, LWT, idempotency, bad-payload) |
| `simulator/mosquitto/mosquitto.conf` | Minimal Mosquitto 2.x config for standalone local/Docker testing |

---

## 2. MQTT Topics, Last Will (LWT) & Delivery Semantics

All topics share the hierarchy `factory/site01/machine/{machine_id}/...`:

| Topic Pattern | Direction | QoS | Delivery Semantics | Retained | Purpose |
| :--- | :--- | :---: | :--- | :---: | :--- |
| `factory/site01/machine/{machine_id}/telemetry` | Simulator -> Subscribers | `0` | **At-most-once** | `False` | Periodic unified sensor telemetry (`1.0 s` default). Wildcard: `factory/site01/machine/+/telemetry`. |
| `factory/site01/machine/{machine_id}/status` | Simulator / Broker -> Subscribers | `1` | **At-least-once** | `True` | Online/offline state, asset metadata, current scenario, and **Broker LWT target**. Wildcard: `factory/site01/machine/+/status`. |
| `factory/site01/machine/{machine_id}/command` | Node-RED / CLI -> Simulator | `1` | **At-least-once** | `False` | Per-machine control channel (`set_scenario`, `accept_maintenance`, `simulate_crash`, `reconnect`). |
| `factory/site01/machine/all/command` | Node-RED / CLI -> Simulator | `1` | **At-least-once** | `False` | Broadcast control channel targeting all 3 pumps simultaneously. |

### 2.1 Justification of Topics & Last Will and Testament (LWT)
- **Unified Telemetry vs. Split Sensor Topics:** Publishing all four simultaneous measurements (`temperature_c`, `vibration_rms`, `current_a`, `rpm`) in one JSON event on `/telemetry` avoids multi-topic synchronization skew in downstream windowing and risk scoring.
- **Dedicated `/status` & Per-Pump LWT Isolation:** Each virtual pump establishes its own independent MQTT client session (`sim-PUMP-01`, `sim-PUMP-02`, `sim-PUMP-03`) and registers a retained `QoS 1` Last Will on `factory/site01/machine/{machine_id}/status` before connecting.
  - When a pump connects normally, it publishes `"connectivity": "ONLINE"` (`retain=True`), allowing any newly started consumer (Node-RED or Python bridge) to discover active fleet state immediately.
  - If a single pump/gateway crashes ungracefully (TCP socket drop without an MQTT `DISCONNECT` packet), Mosquitto automatically publishes the LWT `"connectivity": "OFFLINE", "reason": "unexpected_disconnect_lwt"` (`retain=True`) to that pump's `/status` topic—without affecting the other two pumps or polluting `/telemetry` with non-telemetry payloads.

### 2.2 Producer / Consumer Message Delivery Semantics
- **At-most-once (`QoS 0`) for `/telemetry`:** High-frequency periodic sensor readings (`1 Hz`) are continuously superseded by the next sample one second later. Fire-and-forget delivery minimizes broker/network overhead without degrading sliding-window trend estimation if an occasional packet drops.
- **At-least-once (`QoS 1`) for `/status` (incl. LWT) and `/command`:** State transitions, unexpected disconnect alerts, and maintenance commands are infrequent, critical control events that must not be silently lost (`PUBACK` retry guarantee). Because `QoS 1` can deliver duplicates upon retransmission, the simulator implements **idempotent command handling** (`VirtualPump.accept_maintenance()` ignores duplicate `action_id`s or repeated triggers while already `RECOVERING` or `NORMAL`), achieving safe effective-once state transitions.
- **Why not Exactly-once (`QoS 2`)?** `QoS 2` requires a four-part handshake (`PUBLISH -> PUBREC -> PUBREL -> PUBCOMP`) per message. Given idempotent consumers on `/command` and state-replacement semantics on `/status`, `QoS 1` provides equivalent operational reliability at half the protocol round-trip cost.

---

## 3. Message Payloads

### 3.1 Telemetry Event (`/telemetry`, QoS 0, Retain=False)

```json
{
  "machine_id": "PUMP-03",
  "timestamp": "2026-11-14T16:55:12.401Z",
  "seq": 48,
  "machine_status": "RUNNING",
  "temperature_c": 86.42,
  "vibration_rms": 7.65,
  "current_a": 13.84,
  "rpm": 1456,
  "sim_scenario": "BEARING_WEAR"
}
```

- `machine_status`: `"RUNNING"` or `"RECOVERING"` (while returning to normal after maintenance).
- `seq`: Monotonically increasing per-pump sequence counter (useful for verifying per-key Kafka partition ordering).
- `sim_scenario`: Ground-truth simulator mode (`NORMAL`, `BEARING_WEAR`, `OVERHEATING`, `OVERLOAD`, `COMBINED_FAILURE`) for audit history and validation comparison. **Node-RED health/fault decision logic should infer state from the four numeric sensor signals, not from `sim_scenario`.**

### 3.2 Device Status & LWT Event (`/status`, QoS 1, Retain=True)

**Online / State Update:**
```json
{
  "machine_id": "PUMP-01",
  "asset_name": "Centrifugal Water Pump Unit 01",
  "asset_type": "3-phase_induction_motor_centrifugal_pump",
  "location": "factory/site01/cooling-loop-A",
  "machine_status": "RUNNING",
  "sim_scenario": "NORMAL",
  "maintenance_count": 0,
  "last_action_id": null,
  "connectivity": "ONLINE",
  "reason": "connected",
  "updated_at": "2026-11-14T16:55:00.112Z"
}
```

**Unexpected Disconnect (Fired Automatically by Broker via LWT):**
```json
{
  "machine_id": "PUMP-03",
  "asset_name": "Centrifugal Water Pump Unit 03",
  "asset_type": "3-phase_induction_motor_centrifugal_pump",
  "location": "factory/site01/main-supply",
  "connectivity": "OFFLINE",
  "machine_status": "OFFLINE",
  "reason": "unexpected_disconnect_lwt"
}
```

> **Consumer Contract Note (Member 2 & Member 3):** `/telemetry` uses `timestamp`, whereas `/status` uses `updated_at`. Because the LWT payload is statically registered with the broker at `CONNECT` time, LWT messages intentionally **omit** `updated_at`, `sim_scenario`, `maintenance_count`, and `last_action_id` (preventing stale connect-time timestamps). Downstream Node-RED and Kafka consumers should default missing `updated_at` to local receipt time and treat `sim_scenario` as optional on `/status` events.

### 3.3 Downlink Commands (`/command`, QoS 1)

Node-RED (`mqtt out`) or `sim_cli.py` can publish any of the following JSON payloads to `factory/site01/machine/{machine_id}/command` (or `.../all/command`):

- **Accept Maintenance (triggers smooth recovery back to `NORMAL`):**
  ```json
  {"command": "accept_maintenance", "action_id": "ACT-001"}
  ```
  *(Also accepts `"action": "accept_maintenance"` or `"command": "repair"` / `"reset"`.)*
- **Inject Fault / Switch Scenario:**
  ```json
  {"command": "set_scenario", "scenario": "BEARING_WEAR"}
  ```
  Valid scenarios: `NORMAL`, `BEARING_WEAR`, `OVERHEATING`, `OVERLOAD`, `COMBINED_FAILURE`.
- **Simulate Ungraceful Crash (fires Broker LWT for that single pump):**
  ```json
  {"command": "simulate_crash"}
  ```
- **Reconnect Crashed Pump:**
  ```json
  {"command": "reconnect"}
  ```

---

## 4. Signal Ranges & Scenario Behavior

Aligned with the prototype threshold specification:

| Signal | Normal Range | Warning Region | Critical Region |
| :--- | :--- | :--- | :--- |
| `temperature_c` | `<= 70 C` | `70 - 80 C` | `> 80 C` |
| `vibration_rms` | `<= 3 mm/s` | `3 - 6 mm/s` | `> 6 mm/s` |
| `current_a` | `<= 15 A` | `15 - 20 A` | `> 20 A` |
| `rpm` | `1400 - 1500 RPM` | `1350 - 1400` or `1500 - 1550` | `< 1350` or `> 1550` |

| Scenario | Signal Progression (Default 50 s Linear Ramp -> Plateau) |
| :--- | :--- |
| `NORMAL` | `~62.5-64 C`, `~2.05-2.2 mm/s`, `~12.1-12.4 A`, `~1472-1480 RPM` with low Gaussian noise (zero false alarms; flat trend -> `TTC = Unknown`). |
| `BEARING_WEAR` | **Vibration** ramps linearly `~2.1 -> 8.8 mm/s` (`R^2 > 0.95` for TTC); **Temperature** rises secondarily `~63 -> 84.5 C` with slight lag; current/RPM remain normal. |
| `OVERHEATING` | **Temperature** ramps linearly `~63 -> 102 C`; vibration, current, and RPM stay normal. |
| `OVERLOAD` | **Current** ramps `~12.2 -> 23.8 A` while **RPM** drops `~1476 -> 1306 RPM` (`< 1350` critical); mild delayed heating. |
| `COMBINED_FAILURE` | All four signals degrade simultaneously (`Temp -> 106 C`, `Vib -> 9.4 mm/s`, `Current -> 24.5 A`, `RPM -> 1290`). |
| `RECOVERING` | Triggered on `accept_maintenance`: smoothly interpolates from current degraded values back to nominal baseline over `12 s`, then transitions back to `NORMAL`. |

---

## 5. Quickstart & Verification Commands

### Step 1: Install Dependency
```bash
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\pip install -r requirements.txt
# Linux / macOS:
.venv/bin/pip install -r requirements.txt
```

### Step 2: Start an MQTT Broker (Standalone Docker Example)
```powershell
# Windows PowerShell (from repo root D:\code\DIS):
docker run -d --rm --name dis-mosquitto -p 1883:1883 -v "${PWD}/simulator/mosquitto:/mosquitto/config" eclipse-mosquitto:2
```
*(Or test physics without a broker using `python -m simulator.run_simulator --dry-run`.)*

### Step 3: Run the 3-Pump Simulator
```bash
python -m simulator.run_simulator --host localhost --port 1883 --interval 1.0
```

### Step 4: Control & Test Scenarios (In Another Terminal)
```bash
# Inspect retained /status and live telemetry
python -m simulator.sim_cli monitor

# Inject bearing wear on PUMP-03 (PUMP-01 and PUMP-02 stay NORMAL)
python -m simulator.sim_cli scenario PUMP-03 BEARING_WEAR

# Accept maintenance on PUMP-03 and observe smooth 12s recovery
python -m simulator.sim_cli maintain PUMP-03

# Test P9: Force ungraceful disconnect on PUMP-02 to verify Broker LWT OFFLINE
python -m simulator.sim_cli crash PUMP-02
python -m simulator.sim_cli reconnect PUMP-02

# Test P5: Send malformed payload (missing machine_id) to verify Node-RED quarantine
python -m simulator.sim_cli bad-payload PUMP-01 --kind missing_id

# Run automated unit & embedded-MQTT LWT verification suite
python -m unittest simulator.test_simulator -v
```
