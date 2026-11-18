# 04: Spark Structured Streaming KPIs, Alerts, and Signature Challenge

**What to build:** Build the Spark Structured Streaming job (with pre-baked Kafka & PostgreSQL JARs in Docker image) that consumes Kafka telemetry, deduplicates and normalizes dirty data in-stream, computes 1-minute and 5-minute windowed building aggregates and WiFi client counts, detects real-time SLA Violations (p95 latency > 100 ms, packet loss > 2%), Heartbeat Timeouts (> 2 min), and 1-hour moving-average +/- 3 std Traffic Anomalies, and writes results to PostgreSQL `streaming.*` tables.

**Blocked by:** 03: MQTT, Kafka KRaft, Bridge, and MongoDB Raw Ingestion

**Status:** resolved

- [x] Unit tests verify streaming transformation/anomaly logic (3/3 passed)
- [x] Spark container builds and runs under `docker compose up` without manual JAR downloads
- [x] PostgreSQL `streaming.window_metrics_1m`, `streaming.window_metrics_5m`, `streaming.device_latest_state` receive live rows
- [x] W3 Checkpoint 1 verified end-to-end
