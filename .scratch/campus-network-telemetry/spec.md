Status: ready-for-agent

# Campus Network Telemetry Pipeline Specification

## Problem Statement

Campus IT operations needs an end-to-end data engineering pipeline to monitor campus network health in real time, detect SLA Violations, Traffic Anomalies, and Heartbeat Timeouts before users are impacted, and analyze 7-day historical capacity and Daily SLA Availability across campus Buildings from a single `docker compose up` command.

## Solution

Build a local, reproducible Docker Compose system for Topic 5 Variant B (Campus Network Telemetry) comprising:
- A seeded Python simulator generating 24 Network Devices across 4 Buildings (~1 msg/s/device live via MQTT + 7-day 1-minute backfill to MongoDB), with realistic lecture-hour/night cycles, triggerable Flash Crowd events, and controlled Dirty Data Injection across 5 anomaly types logged to an Injection Ledger.
- An MQTT-to-Kafka bridge forwarding Telemetry Events into Kafka, plus a Kafka-to-MongoDB raw storage consumer.
- A Spark Structured Streaming job computing 1-minute and 5-minute windowed building KPIs, WiFi client counts, real-time SLA Violation alerts, Heartbeat Timeout alerts, and 1-hour moving-average Traffic Anomaly detection, writing results to PostgreSQL `streaming` schema.
- MySQL master data (`buildings`, `devices`, `sla_thresholds`) and an Airflow scheduled DAG running Pandas ETL + data quality reconciliation (`disisipkan vs tertangkap`) to populate a PostgreSQL `warehouse` star schema (`fact_device_metric_hourly`, `fact_sla_window`, `fact_alert`, `dim_device`, `dim_building`, `dim_time`, `dim_alert_type`, plus `dq_reconciliation`).
- Provisioned Grafana dashboards (Indonesian UI labels, WIB display timezone over UTC storage) for both real-time streaming monitoring and 7-day historical analytics.

## User Stories

1. As a campus network operator, I want 24 simulated Network Devices (8 switches, 12 access points, 4 latency probes) grouped across 4 Buildings emitting valid Telemetry Events at ~1 message/second/device, so that the pipeline receives realistic continuous telemetry.
2. As a campus network operator, I want network traffic to follow high-lecture-hour and low-nighttime patterns plus a scheduled or manually triggerable Flash Crowd event (KRS registration), so that I can observe and demo system behavior under load surges.
3. As a data engineer, I want controlled, seeded Dirty Data Injection across 5 types (`counter_reset`, `inconsistent_unit`, `out_of_range`, `missing_heartbeat`, `duplicate_message`) with an authoritative Injection Ledger (`disisipkan`), so that I can verify pipeline detection and reporting accuracy.
4. As a data engineer, I want MQTT messages bridged into Kafka and persisted raw into MongoDB, so that raw Telemetry Events are decoupled and durably stored for batch ETL.
5. As a campus network operator, I want Spark Structured Streaming to compute 1-minute and 5-minute windowed metrics (average/max per Building, WiFi client count per Building) in real time, so that I can monitor live campus network load.
6. As a campus network operator, I want Spark Structured Streaming to emit real-time alerts when 5-minute p95 latency > 100 ms, packet loss > 2%, or a device misses heartbeat > 2 minutes, so that I can respond to SLA Violations and outages immediately.
7. As a campus network operator, I want Spark Structured Streaming to flag Traffic Anomalies outside the 1-hour moving-average band (mean +/- 3 std), so that abnormal traffic surges are surfaced automatically.
8. As a data engineer, I want MySQL to store master reference data for Buildings, Network Devices, and SLA thresholds, so that batch ETL and enrichment use a single source of truth.
9. As a data engineer, I want a scheduled Airflow DAG executing Pandas ETL to clean raw MongoDB telemetry, detect and handle all 5 dirty data types, reconcile caught vs. injected counts (`disisipkan vs tertangkap`), and load the PostgreSQL star schema, so that historical data is trustworthy.
10. As an IT manager, I want the star schema warehouse to answer which Building was most problematic over 7 days and the daily SLA Availability trend per Building, so that I can make capacity and reliability decisions.
11. As a lecturer/reviewer, I want the entire stack to start cleanly with `docker compose up` using `.env.example` defaults, automatic topic/schema/DAG/dashboard initialization, and reasonable memory limits, so that I can evaluate it without manual fixes.
12. As a student team, I want `docs/proposal.md`, `README.md`, `laporan.md`, `slides.pdf` outline, `video-demo/` 5-minute script, and empty `refleksi/NIM_Nama.md` templates, so that all W1/W3/W5/W6 deliverables are complete.

## Implementation Decisions

- **Device Topology (24 devices across 4 buildings)**: `BLD-FIK`, `BLD-FT`, `BLD-FEB`, `BLD-RECT` (6 devices each: 2 switches, 3 access points, 1 latency probe).
- **Message Schema**: Strict JSON `{device_id, device_type, building_id, timestamp (ISO 8601 UTC), data: {...}}`.
  - `switch`: `port_in_mbps`, `port_out_mbps`, `errors`, `uptime_s`
  - `access_point`: `clients`, `rssi_avg_dbm`, `channel_util_pct`
  - `latency_probe`: `latency_ms`, `packet_loss_pct`, `target`
- **Dirty Data Rules**:
  - `counter_reset`: reset `errors`/`uptime_s` to 0 mid-stream; detected by negative delta (`curr < prev`), handled by delta reset.
  - `inconsistent_unit`: multiply `port_in_mbps`/`port_out_mbps` by `1_000_000` (bps instead of Mbps); detected by threshold `> 10_000`, handled by dividing by `1_000_000`.
  - `out_of_range`: `packet_loss_pct > 100`, `latency_ms < 0`, `channel_util_pct > 100`, `rssi_avg_dbm > 0`, or `clients < 0`; detected by physical bounds, handled by nullifying/quarantining invalid field.
  - `missing_heartbeat`: suppress device emissions for 3–4 minutes; detected by gap > 2 minutes, handled by emitting `HEARTBEAT_MISSING` alert and recording outage episode.
  - `duplicate_message`: re-emit identical `(device_id, timestamp)` payload; detected and handled by deduplicating on `(device_id, timestamp)`.
- **Storage & Schema**:
  - MySQL: `buildings`, `devices`, `sla_thresholds`.
  - MongoDB: `raw_telemetry` collection (indexed on `timestamp`, `(device_id, timestamp)`).
  - PostgreSQL (`campus_telemetry` DB): `streaming` schema (`window_metrics_1m`, `window_metrics_5m`, `realtime_alerts`, `traffic_anomalies`, `device_latest_state`) and `warehouse` schema (`dim_building`, `dim_device`, `dim_time`, `dim_alert_type`, `fact_device_metric_hourly`, `fact_sla_window`, `fact_alert`, `dq_reconciliation`).
- **Timezones**: All ingestion, storage, and processing use UTC; Grafana dashboards display in `Asia/Jakarta` (WIB).
- **Language**: Code, variables, comments, and Git commits in English; Grafana dashboard UI, alert labels, `README.md`, and `laporan.md` in Indonesian.

## Testing Decisions

- **Pure-logic unit tests (pytest)**: Test the highest seams without requiring external brokers/DBs for fast TDD:
  1. Simulator payload schema, diurnal/flash-crowd profile, deterministic seeded dirty-data injection, and Injection Ledger accuracy.
  2. Data Quality & Pandas ETL cleaning functions (all 5 dirty data types: detection, handling, and exact count reconciliation).
  3. Signature challenge calculations (SLA 5-min compliance, moving-average band anomaly detection, daily SLA availability %).
- **Integration / E2E verification**: `docker compose up -d --build`, `docker compose ps` (all services healthy), MongoDB document count check, Kafka topic message check, PostgreSQL `streaming.*` and `warehouse.*` query checks, Airflow DAG run success check, and Grafana HTTP/API dashboard & screenshot check.

## Out of Scope

- Cloud deployment or paid external services.
- Machine learning models (rule-based/statistical methods are sufficient per brief; bonus automated tests & alert notification webhook/log may be added if time permits).
- Writing personal reflections on behalf of team members (`refleksi/` contains empty templates only).

## Further Notes

- Follow commit prefixes: `sim:`, `bridge:`, `spark:`, `dags:`, `dash:`, `wh:`, `docs:`.
- Never commit `.env`, database volumes, or virtualenvs.
