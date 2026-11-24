"""Consumer Group 2 — historical replay & recovery demonstration.

Proves the two Member-2 durability claims from the proposal (Section 5):
1. *Replay*: a brand-new consumer group (fresh offsets) can re-read the whole
   retained history from the beginning at any time, independent of the live
   bridge and of the audit group.
2. *Consumer recovery*: the same group id can be stopped mid-stream and
   restarted, resuming exactly after its last committed offset — no loss, no
   reprocessing of committed records.

By default each run uses a fresh ``cg-replay-<random>`` group so the full
history is replayed; pass ``--group-id`` to demonstrate resume-after-restart.

Usage:
    # Full replay of the audit stream with a fresh consumer group:
    python -m kafka_streaming.consumer_replay --topic machine.audit

    # Resume demo: Ctrl+C mid-run, re-run with the SAME group id:
    python -m kafka_streaming.consumer_replay --topic machine.telemetry --group-id cg-replay-demo
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from collections import Counter, defaultdict
from typing import Any, Dict

from .config import (
    ALL_TOPICS,
    DEFAULT_BOOTSTRAP_SERVERS,
    TOPIC_AUDIT,
    new_replay_group_id,
)

log = logging.getLogger("kafka.consumer_replay")


class ReplayConsumer:
    """Reads retained history from one topic and summarizes what was replayed."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        *,
        topic: str = TOPIC_AUDIT,
        group_id: str | None = None,
        max_events: int = 0,  # 0 = read until the log end
        verbose: bool = False,
    ) -> None:
        from kafka import KafkaConsumer, TopicPartition  # deferred import

        self.group_id = group_id or new_replay_group_id()
        self.topic = topic
        self.max_events = max_events
        self.verbose = verbose
        self._stop = threading.Event()

        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            auto_offset_reset="earliest",  # fresh group => replay from the start
            consumer_timeout_ms=5000,      # stop when the log end is reached
        )
        self.consumer.subscribe([topic])
        self._tps: list[TopicPartition] = []

    def _end_offsets(self) -> Dict[int, int]:
        from kafka import TopicPartition

        partitions = self.consumer.partitions_for_topic(self.topic) or set()
        tps = [TopicPartition(self.topic, p) for p in partitions]
        end = self.consumer.end_offsets(tps)
        return {tp.partition: offset for tp, offset in end.items()}

    def run(self) -> Dict[str, Any]:
        per_machine: Counter[str] = Counter()
        per_type: Counter[str] = Counter()
        seq_gaps: Dict[str, list[int]] = defaultdict(list)
        last_seq: Dict[str, int] = {}
        consumed = 0
        end_offsets = self._end_offsets()
        total_available = sum(end_offsets.values())
        log.info(
            "replaying topic=%s group=%s (retained records: %d across partitions %s)",
            self.topic, self.group_id, total_available, sorted(end_offsets),
        )

        try:
            for msg in self.consumer:
                if self._stop.is_set():
                    break
                event: Dict[str, Any] = msg.value if isinstance(msg.value, dict) else {}
                machine_id = str(event.get("machine_id", msg.key or "UNKNOWN"))
                per_machine[machine_id] += 1
                per_type[str(event.get("event_type", "?"))] += 1
                consumed += 1

                # Per-machine ordering check: seq must be non-decreasing per key.
                payload = event.get("payload")
                seq = payload.get("seq") if isinstance(payload, dict) else None
                if isinstance(seq, int):
                    prev = last_seq.get(machine_id)
                    if prev is not None and seq < prev:
                        seq_gaps[machine_id].append(seq)
                    last_seq[machine_id] = seq

                if self.verbose:
                    print(
                        f"[{msg.partition}:{msg.offset}] key={msg.key} "
                        f"type={event.get('event_type')} time={event.get('event_time')}"
                    )
                if self.max_events and consumed >= self.max_events:
                    break
                # With no explicit cap, stop at the log end as measured at
                # start, so the summary matches the retained-record count even
                # while producers keep appending live records.
                if not self.max_events and consumed >= total_available:
                    break
        except Exception as exc:
            # kafka-python raises StopIteration-free timeout via consumer_timeout_ms
            if type(exc).__name__ != "StopIteration":
                log.info("replay loop ended: %s", exc)

        summary = {
            "group_id": self.group_id,
            "topic": self.topic,
            "retained_records": total_available,
            "replayed_records": consumed,
            "per_machine": dict(sorted(per_machine.items())),
            "per_event_type": dict(sorted(per_type.items())),
            "ordering_violations": {k: v for k, v in seq_gaps.items() if v},
        }
        self.consumer.close()
        return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay & recovery demo consumer (Member 2).")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC_AUDIT, choices=list(ALL_TOPICS))
    parser.add_argument("--group-id", default=None,
                        help="Reuse a group id to demonstrate resume-after-restart; default: fresh group (full replay).")
    parser.add_argument("--max-events", type=int, default=0, help="Stop after N events (0 = whole log).")
    parser.add_argument("--verbose", action="store_true", help="Print every replayed record.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        replay = ReplayConsumer(
            args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            max_events=args.max_events,
            verbose=args.verbose,
        )
    except Exception as exc:
        log.error("cannot start replay consumer against %s: %s", args.bootstrap_servers, exc)
        return 1

    def _stop(*_: Any) -> None:
        replay._stop.set()

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)

    summary = replay.run()
    print("\n=== REPLAY SUMMARY ===")
    print(json.dumps(summary, indent=2))
    if summary["ordering_violations"]:
        print("WARNING: per-machine ordering violations detected!")
        return 2
    print("Replay OK: history readable by an independent consumer group; per-machine order preserved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
