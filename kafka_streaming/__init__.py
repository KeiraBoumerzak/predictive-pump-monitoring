"""Member 2 — Kafka Streaming and Distributed Data Management layer.

Provides:
- Topic/partition/consumer-group configuration (`config`)
- Event schemas, validation and metadata envelopes (`schemas`)
- Keyed, durable Kafka producer wrapper (`producer`)
- MQTT -> Kafka bridge for validated telemetry/status events (`bridge`)
- Audit & persistence consumer group (`consumer_audit`)
- Replay & recovery demonstration consumer group (`consumer_replay`)
- Topic provisioning script (`create_topics`)
"""
