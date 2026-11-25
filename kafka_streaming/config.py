"""Kafka streaming layer configuration: broker settings, topics, partitions and consumer groups.

Aligned with the project README (Section 2.3) and the course proposal (Section 5):
every topic uses ``machine_id`` as the record key so that all events for one
motor-pump unit land in the same partition in strict chronological order.
"""

from __future__ import annotations

import os
import uuid

# ---------------------------------------------------------------------------
# Broker Connection
# ---------------------------------------------------------------------------
DEFAULT_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_REPLICATION_FACTOR: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))

# ---------------------------------------------------------------------------
# Topic Layout & Partitioning
# ---------------------------------------------------------------------------
# 3 partitions per topic == one per monitored asset (PUMP-01..03). Keying by
# machine_id guarantees per-machine ordering while allowing up to 3 consumer
# instances per group to process machines in parallel.
DEFAULT_PARTITIONS: int = int(os.getenv("KAFKA_PARTITIONS", "3"))

TOPIC_TELEMETRY: str = "machine.telemetry"
TOPIC_FEATURES: str = "machine.features"
TOPIC_HEALTH: str = "machine.health"
TOPIC_ALERTS: str = "machine.alerts"
TOPIC_MAINTENANCE: str = "maintenance.actions"
TOPIC_AUDIT: str = "machine.audit"

ALL_TOPICS: tuple[str, ...] = (
    TOPIC_TELEMETRY,
    TOPIC_FEATURES,
    TOPIC_HEALTH,
    TOPIC_ALERTS,
    TOPIC_MAINTENANCE,
    TOPIC_AUDIT,
)

# ---------------------------------------------------------------------------
# Consumer Groups (independent, non-competing readers of the same log)
# ---------------------------------------------------------------------------
# Group 1: durable audit & persistence — appends every event to per-machine
# JSONL history files, committing offsets only after the write is flushed.
GROUP_AUDIT_PERSISTENCE: str = os.getenv("KAFKA_GROUP_AUDIT", "cg-audit-persistence")

# Group 2: replay & recovery demo — each demo run defaults to a *fresh* group
# id so the whole retained history is re-readable on demand, proving that the
# log is durable and independent of any live consumer.
REPLAY_GROUP_PREFIX: str = "cg-replay"


def new_replay_group_id() -> str:
    """Return a unique replay consumer-group id (fresh offsets => full replay)."""
    return f"{REPLAY_GROUP_PREFIX}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Event Metadata
# ---------------------------------------------------------------------------
SCHEMA_VERSION: str = "1.0"
SOURCE_BRIDGE: str = "kafka-bridge"
SOURCE_AUDIT_CONSUMER: str = "audit-persistence-consumer"
