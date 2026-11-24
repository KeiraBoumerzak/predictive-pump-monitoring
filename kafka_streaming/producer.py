"""Durable, keyed Kafka producer wrapper (at-least-once semantics).

Configuration rationale (README Section 3):
- ``acks="all"`` — the leader only acknowledges after all in-sync replicas
  have written the record (with replication-factor 1 this degrades gracefully
  to leader-ack, still disk-backed).
- ``retries`` — transient broker errors are retried, so records are not
  silently lost; rare duplicate appends from retried batches are absorbed by
  the idempotent downstream consumers (offset-dedup in ``consumer_audit``),
  which is why the layer targets at-least-once rather than heavyweight
  transactional exactly-once. (kafka-python-ng does not implement the
  idempotent producer protocol.)
- Key = ``machine_id`` (UTF-8 bytes) — Kafka hashes the key to a partition,
  so every event for one pump is appended to one partition in order.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from .config import DEFAULT_BOOTSTRAP_SERVERS

log = logging.getLogger(__name__)


def encode_key(machine_id: str) -> bytes:
    """Kafka record key: UTF-8 machine_id (guarantees per-machine ordering)."""
    if not machine_id:
        raise ValueError("machine_id must be non-empty (Kafka record key)")
    return machine_id.encode("utf-8")


def serialize_event(event: Dict[str, Any]) -> bytes:
    """Deterministic JSON serialization (sorted keys) for stable hashing/diffing."""
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")


class EventProducer:
    """Thin wrapper around kafka.KafkaProducer for envelope dicts."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        *,
        acks: Any = "all",
        retries: int = 5,
        **producer_kwargs: Any,
    ) -> None:
        from kafka import KafkaProducer  # deferred: unit tests run without a broker

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=None,  # keys are pre-encoded bytes
            value_serializer=None,  # values are pre-encoded bytes
            acks=acks,
            retries=retries,
            **producer_kwargs,
        )
        self.bootstrap_servers = bootstrap_servers

    def send(
        self,
        topic: str,
        event: Dict[str, Any],
        *,
        machine_id: Optional[str] = None,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """Publish one event keyed by machine_id; blocks for the ack.

        Returns the delivery metadata (topic, partition, offset) so callers
        can log/verify exactly where each record was durably stored.
        """
        key = encode_key(machine_id or event.get("machine_id", ""))
        value = serialize_event(event)
        metadata = self._producer.send(topic, key=key, value=value).get(timeout=timeout)
        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout=timeout)

    def close(self) -> None:
        try:
            self._producer.flush(timeout=10.0)
        finally:
            self._producer.close()

    def __enter__(self) -> "EventProducer":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
