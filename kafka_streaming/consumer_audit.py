"""Consumer Group 1 — durable audit & persistence (at-least-once, idempotent writes).

Subscribes to all six Member-2 topics and appends every event to per-topic,
per-machine JSONL history files under ``data/audit/`` — an inspectable,
disk-backed copy of the operational history:

    data/audit/machine.audit/PUMP-01.jsonl
    data/audit/machine.alerts/PUMP-03.jsonl
    ...

Delivery semantics (README Section 3):
- Offsets are committed manually, only *after* the batch is flushed to disk
  => at-least-once. A crash before the commit replays the batch.
- Writes are idempotent: each line carries the Kafka (partition, offset)
  coordinates, and already-persisted offsets are skipped on redelivery, so
  replays never duplicate history rows.

This group is deliberately independent of the replay group: both read the
same log without stealing records from each other.

Usage:
    python -m kafka_streaming.consumer_audit --bootstrap-servers localhost:9092 --data-dir data/audit
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from .config import (
    ALL_TOPICS,
    DEFAULT_BOOTSTRAP_SERVERS,
    GROUP_AUDIT_PERSISTENCE,
)

log = logging.getLogger("kafka.consumer_audit")

# How many (partition, offset) coordinates to remember per topic/machine for
# redelivery deduplication before falling back to "trust the offset order".
_DEDUP_WINDOW = 5000


class AuditStore:
    """Append-only JSONL persistence with per-machine files and offset dedup."""

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._seen: Dict[Tuple[str, str], Set[int]] = {}
        self._handles: Dict[Path, Any] = {}
        self.records_written = 0
        self.duplicates_skipped = 0

    def _path_for(self, topic: str, machine_id: str) -> Path:
        safe_machine = "".join(c if c.isalnum() or c in "-_" else "_" for c in machine_id)
        topic_dir = self.data_dir / topic
        topic_dir.mkdir(parents=True, exist_ok=True)
        return topic_dir / f"{safe_machine}.jsonl"

    def _is_duplicate(self, topic: str, machine_id: str, offset: int) -> bool:
        seen = self._seen.setdefault((topic, machine_id), set())
        if offset in seen:
            return True
        if len(seen) >= _DEDUP_WINDOW:
            # Window full: offsets arrive in order per partition, so anything
            # at/below the smallest retained offset is old; reset the window.
            lowest = min(seen)
            seen = {o for o in seen if o >= lowest}
            self._seen[(topic, machine_id)] = seen
        seen.add(offset)
        return False

    def append(self, topic: str, partition: int, offset: int, event: Dict[str, Any]) -> bool:
        """Append one event; returns False when the offset was already stored."""
        machine_id = str(event.get("machine_id", "UNKNOWN"))
        if self._is_duplicate(topic, machine_id, offset):
            self.duplicates_skipped += 1
            return False
        row = {"kafka_topic": topic, "kafka_partition": partition, "kafka_offset": offset, **event}
        path = self._path_for(topic, machine_id)
        handle = self._handles.get(path)
        if handle is None or handle.closed:
            handle = open(path, "a", encoding="utf-8")
            self._handles[path] = handle
        handle.write(json.dumps(row, sort_keys=True) + "\n")
        self.records_written += 1
        return True

    def flush(self) -> None:
        for handle in self._handles.values():
            if not handle.closed:
                handle.flush()
                os.fsync(handle.fileno())

    def close(self) -> None:
        self.flush()
        for handle in self._handles.values():
            if not handle.closed:
                handle.close()
        self._handles.clear()

    def __enter__(self) -> "AuditStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class AuditConsumer:
    """Kafka consumer group member persisting all events via AuditStore."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        *,
        group_id: str = GROUP_AUDIT_PERSISTENCE,
        data_dir: str | Path = "data/audit",
        topics: tuple[str, ...] = ALL_TOPICS,
    ) -> None:
        from kafka import KafkaConsumer  # deferred: unit tests run without a broker

        self.store = AuditStore(data_dir)
        self._stop = threading.Event()
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            enable_auto_commit=False,          # commit only after fsync => at-least-once
            auto_offset_reset="earliest",      # a new group first drains retained history
        )
        self.group_id = group_id

    def run(self, poll_timeout_ms: int = 1000) -> None:
        log.info("audit consumer started (group=%s)", self.group_id)
        try:
            while not self._stop.is_set():
                records = self.consumer.poll(timeout_ms=poll_timeout_ms)
                if not records:
                    continue
                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            self.store.append(msg.topic, msg.partition, msg.offset, msg.value)
                        except Exception:
                            # Do NOT commit past a failed write; log and retry next poll.
                            log.exception("failed to persist %s[%d]@%d", msg.topic, msg.partition, msg.offset)
                            break
                self.store.flush()
                self.consumer.commit()
        finally:
            self.store.close()
            self.consumer.close()
            log.info(
                "audit consumer stopped. written=%d duplicates_skipped=%d",
                self.store.records_written, self.store.duplicates_skipped,
            )

    def stop(self, *_: Any) -> None:
        self._stop.set()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit & persistence consumer group (Member 2).")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--group-id", default=GROUP_AUDIT_PERSISTENCE)
    parser.add_argument("--data-dir", default="data/audit")
    parser.add_argument("--topics", nargs="*", default=list(ALL_TOPICS), choices=list(ALL_TOPICS))
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        consumer = AuditConsumer(
            args.bootstrap_servers,
            group_id=args.group_id,
            data_dir=args.data_dir,
            topics=tuple(args.topics),
        )
    except Exception as exc:
        log.error("cannot start audit consumer against %s: %s", args.bootstrap_servers, exc)
        return 1

    signal.signal(signal.SIGINT, consumer.stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, consumer.stop)
    consumer.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
