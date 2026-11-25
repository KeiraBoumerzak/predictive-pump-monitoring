"""Provision the six Member-2 Kafka topics (3 partitions, keyed by machine_id).

Usage:
    python -m kafka_streaming.create_topics
    python -m kafka_streaming.create_topics --bootstrap-servers localhost:9092 --replication-factor 1
"""

from __future__ import annotations

import argparse
import logging
import sys

from .config import (
    ALL_TOPICS,
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_PARTITIONS,
    DEFAULT_REPLICATION_FACTOR,
)

log = logging.getLogger("kafka.create_topics")


def create_topics(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    *,
    partitions: int = DEFAULT_PARTITIONS,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
) -> list[str]:
    """Create all missing topics; returns the list of topics that exist after the call."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        existing = set(admin.list_topics())
        for name in ALL_TOPICS:
            if name in existing:
                log.info("topic exists, skipping: %s", name)
                continue
            try:
                admin.create_topics(
                    [NewTopic(name=name, num_partitions=partitions, replication_factor=replication_factor)]
                )
                log.info(
                    "created topic %s (partitions=%d, replication-factor=%d)",
                    name, partitions, replication_factor,
                )
            except TopicAlreadyExistsError:  # raced with another creator
                log.info("topic created concurrently, skipping: %s", name)
        return sorted(set(admin.list_topics()) & set(ALL_TOPICS))
    finally:
        admin.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create the Member-2 Kafka topics.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--partitions", type=int, default=DEFAULT_PARTITIONS)
    parser.add_argument("--replication-factor", type=int, default=DEFAULT_REPLICATION_FACTOR)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        created = create_topics(
            args.bootstrap_servers,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        )
    except Exception as exc:  # broker unreachable etc.
        log.error("failed to provision topics against %s: %s", args.bootstrap_servers, exc)
        return 1

    missing = sorted(set(ALL_TOPICS) - set(created))
    if missing:
        log.error("topics still missing after provisioning: %s", missing)
        return 1
    print(f"All {len(ALL_TOPICS)} topics ready: {', '.join(created)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
