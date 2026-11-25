# Kafka Streaming Layer (Member 2)

My part of the project, the Kafka side. Short version: MQTT (Member 1's
simulator) handles the live sensor data, and everything that needs to be
*kept* goes through Kafka so we have a history we can replay later.
Node-RED (Member 3) will also publish its derived stuff (features, health
scores, maintenance actions) into the same topics once that part is done.

## What's in here

| File | What it does |
| :--- | :--- |
| `config.py` | Topic names, partition counts, consumer group ids, broker address. All in one place so I don't typo a topic name in five files. |
| `schemas.py` | Validation for incoming telemetry/status messages + the metadata envelope we wrap around every Kafka record. Bad messages become "quarantine" events instead of being silently dropped. |
| `producer.py` | Small wrapper around `KafkaProducer`. Always uses `machine_id` as the key and `acks=all`. |
| `bridge.py` | The MQTT -> Kafka bridge. Subscribes to the simulator's topics, validates, and produces to Kafka. |
| `consumer_audit.py` | Consumer group 1: writes every event to per-machine `.jsonl` files under `data/audit/`. |
| `consumer_replay.py` | Consumer group 2: the replay demo. Reads history back with a fresh group and checks per-machine ordering. |
| `create_topics.py` | Creates the 6 topics (3 partitions each). |
| `docker-compose.yml` | Kafka (single broker, KRaft mode) + Mosquitto so the whole stack starts with one command. |
| `test_kafka_streaming.py` | Unit tests that don't need a broker running. |

## Topics

Six topics, all keyed by `machine_id`, all with 3 partitions (one per pump):

| Topic | What goes in it | Who writes it |
| :--- | :--- | :--- |
| `machine.telemetry` | validated raw sensor readings | bridge (this part) |
| `machine.features` | rolling averages, slopes, R² | Node-RED → bridge |
| `machine.health` | risk score, state, TTC, priority | Node-RED → bridge |
| `machine.alerts` | WARNING/CRITICAL/OFFLINE events | bridge + Node-RED → bridge |
| `maintenance.actions` | recommended + accepted actions | Node-RED → bridge |
| `machine.audit` | copy of everything, for replay | bridge (+ everyone) |

All Kafka producers are Python. Node-RED doesn't produce to Kafka directly
(avoids the flaky Node-RED Kafka plugin situation) — it publishes its derived
events to MQTT and the bridge forwards them:

| MQTT topic (Node-RED publishes) | Kafka topic | event_type |
| :--- | :--- | :--- |
| `factory/site01/machine/+/features` | `machine.features` | `features` |
| `factory/site01/machine/+/health` | `machine.health` | `health` |
| `factory/site01/machine/+/alerts` | `machine.alerts` | `alert` |
| `factory/site01/machine/+/actions` | `maintenance.actions` | `maintenance_action` |

Derived events also get copied to `machine.audit` and carry
`"source": "nodered"` in the envelope.

**Why `machine_id` as the key:** Kafka hashes the key to pick a partition, so
all events for one pump always land in the same partition, in order. We can
actually prove this works because the simulator puts a `seq` counter in every
message, and the replay consumer checks that `seq` never goes backwards per
machine.

**Why 3 partitions:** we have 3 pumps. This way each pump's data can be
handled by a different consumer instance if we ever scale out, without
breaking ordering.

**Replication:** 1 for the local demo (single broker on a laptop). The topic
script takes `--replication-factor` if we want to show the multi-broker setup.

## The event envelope

Every record we write looks like this, so the audit/replay side always knows
where an event came from and when:

```json
{
  "schema_version": "1.0",
  "event_type": "telemetry",
  "machine_id": "PUMP-03",
  "event_time": "2026-11-14T16:55:12.401Z",
  "ingest_time": "2026-11-14T16:55:12.507Z",
  "source": "kafka-bridge",
  "payload": { "seq": 48, "temperature_c": 86.42, "vibration_rms": 7.65 }
}
```

`event_time` is the timestamp from the simulator, `ingest_time` is when the
bridge got it. The difference between the two is handy for spotting delays.

## Validation & quarantine

The bridge checks `machine_id`, `timestamp` and the four sensor fields before
anything reaches Kafka. If a message is broken (bad JSON, missing id, sensor
value way out of range) it doesn't just get dropped. It goes to
`machine.audit` as a `quarantine` event with the error and the raw payload,
so during the demo we can show exactly what was rejected and why
(`sim_cli bad-payload` triggers this).

## The LWT -> alert path

When a pump crashes ungracefully, Mosquitto fires the Last Will on its
`/status` topic (`connectivity: OFFLINE`, `reason: unexpected_disconnect_lwt`).
The bridge picks that up and writes a `CONNECTIVITY` / `CRITICAL` alert to
`machine.alerts`. Tested with `sim_cli crash PUMP-02`.

A *graceful* shutdown (Ctrl+C on the simulator) also retains an OFFLINE
status, but with `reason: graceful_shutdown` — that's normal operation, so it
does NOT raise a CRITICAL alert. It's still kept in `machine.audit` like any
other status event.

## Delivery semantics (the "why" for the report)

- **Producer: at-least-once.** `acks=all` + retries, so nothing is silently
  lost. Retries can very rarely produce a duplicate record, which is why the
  consumer side has to cope with duplicates:
- **Audit consumer: commits offsets only after the data is flushed to disk**,
  and skips records whose (partition, offset) it already wrote. So if it
  crashes mid-batch it just replays the batch without duplicating history.
- We didn't go for exactly-once on purpose. It needs Kafka transactions,
  which is way too much complexity for a prototype where the consumers are
  idempotent anyway.

## Running it

```powershell
# 1. dependencies (from repo root, venv active)
pip install -r requirements.txt

# 2. start Kafka + Mosquitto
docker compose -f kafka_streaming/docker-compose.yml up -d

# 3. create the topics (only needed once)
python -m kafka_streaming.create_topics

# 4. start the simulator (Member 1)
python -m simulator.run_simulator --interval 1.0

# 5. start the bridge (each of these in its own terminal)
python -m kafka_streaming.bridge

# 6. start the audit consumer
python -m kafka_streaming.consumer_audit
```

Then inject some faults and watch it flow:

```powershell
python -m simulator.sim_cli scenario PUMP-03 BEARING_WEAR   # fault on one pump
python -m simulator.sim_cli crash PUMP-02                   # LWT -> alert
python -m simulator.sim_cli bad-payload PUMP-01 --kind missing_id
```

Check `data/audit/`, there should be `.jsonl` files per topic per machine
filling up live.

## The replay demo (the one we actually need to show)

```powershell
# Full replay with a brand new consumer group — works even if the bridge
# and audit consumer are stopped, because the log is durable:
python -m kafka_streaming.consumer_replay --topic machine.audit

# Resume demo: run this, Ctrl+C halfway, then run the SAME command again.
# It continues from where it stopped instead of starting over:
python -m kafka_streaming.consumer_replay --topic machine.telemetry --group-id cg-replay-demo
```

The summary at the end prints how many records were retained vs replayed, a
per-machine and per-type breakdown, and two correctness checks (exit code 2
if either fails):

- **ordering**: the simulator's `seq` must never go backwards per machine.
  A `seq` that jumps back to `1` is counted as a simulator restart (the Kafka
  log outlives simulator runs), not a violation.
- **partition consistency**: every event for one machine must come from the
  same partition (that's what keying by `machine_id` guarantees).

## Tests

```powershell
python -m unittest kafka_streaming.test_kafka_streaming -v
```

24 tests, no broker needed — they cover the validation rules (telemetry,
status, derived), the envelope, key encoding, the bridge's routing (derived
events, LWT vs graceful shutdown) and the audit-store dedup logic.

## Notes / gotchas I ran into

- The package is called `kafka_streaming` and NOT `kafka`, because a local
  folder named `kafka` shadows the actual kafka-python library and nothing
  imports properly. Found that out the annoying way.
- We use `kafka-python-ng` (the maintained fork), the original
  `kafka-python` breaks on newer Python versions.
- `enable_idempotence` isn't supported by kafka-python-ng, so at-least-once
  with idempotent consumers is the actual design here, not a shortcut.
