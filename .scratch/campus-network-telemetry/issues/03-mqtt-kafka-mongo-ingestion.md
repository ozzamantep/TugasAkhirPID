# 03: MQTT, Kafka KRaft, Bridge, and MongoDB Raw Ingestion

**What to build:** Wire Mosquitto MQTT broker, Apache Kafka (KRaft mode) with auto-initialized topics, the Python MQTT-to-Kafka bridge, and the Kafka-to-MongoDB raw telemetry consumer into `docker-compose.yml` with healthchecks and memory limits.

**Blocked by:** 02: Campus Network Telemetry Simulator and Injection Ledger

**Status:** resolved

- [x] `docker compose up -d` starts Mosquitto, Kafka, MySQL, MongoDB, Postgres, Simulator, Bridge, and Mongo Consumer cleanly with healthy status
- [x] Telemetry Events flow end-to-end from Simulator -> MQTT -> Bridge -> Kafka -> MongoDB `raw_telemetry` (verified: backfill=234747, stream=222+)
- [x] `aislop` manual/rule review passes on bridge and consumer code
