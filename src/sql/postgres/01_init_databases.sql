-- PostgreSQL Initialization: Airflow DB + Campus Telemetry (Streaming & Warehouse Schemas)

CREATE DATABASE airflow;

\connect campus_telemetry;

CREATE SCHEMA IF NOT EXISTS streaming;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ==========================================
-- STREAMING SCHEMA (Spark Structured Streaming Sink)
-- ==========================================

CREATE TABLE IF NOT EXISTS streaming.window_metrics_1m (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    building_id VARCHAR(16) NOT NULL,
    avg_port_in_mbps DOUBLE PRECISION,
    max_port_in_mbps DOUBLE PRECISION,
    avg_port_out_mbps DOUBLE PRECISION,
    max_port_out_mbps DOUBLE PRECISION,
    total_wifi_clients INTEGER,
    avg_channel_util_pct DOUBLE PRECISION,
    avg_latency_ms DOUBLE PRECISION,
    p95_latency_ms DOUBLE PRECISION,
    avg_packet_loss_pct DOUBLE PRECISION,
    max_packet_loss_pct DOUBLE PRECISION,
    record_count INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, building_id)
);

CREATE TABLE IF NOT EXISTS streaming.window_metrics_5m (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    building_id VARCHAR(16) NOT NULL,
    avg_port_in_mbps DOUBLE PRECISION,
    max_port_in_mbps DOUBLE PRECISION,
    avg_port_out_mbps DOUBLE PRECISION,
    max_port_out_mbps DOUBLE PRECISION,
    total_wifi_clients INTEGER,
    avg_channel_util_pct DOUBLE PRECISION,
    avg_latency_ms DOUBLE PRECISION,
    p95_latency_ms DOUBLE PRECISION,
    avg_packet_loss_pct DOUBLE PRECISION,
    max_packet_loss_pct DOUBLE PRECISION,
    sla_compliant BOOLEAN NOT NULL DEFAULT TRUE,
    record_count INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, building_id)
);

CREATE TABLE IF NOT EXISTS streaming.realtime_alerts (
    id BIGSERIAL PRIMARY KEY,
    alert_time TIMESTAMPTZ NOT NULL,
    building_id VARCHAR(16) NOT NULL,
    device_id VARCHAR(32) NOT NULL,
    alert_type VARCHAR(32) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    metric_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (alert_time, device_id, alert_type)
);

CREATE TABLE IF NOT EXISTS streaming.traffic_anomalies (
    id BIGSERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL,
    building_id VARCHAR(16) NOT NULL,
    device_id VARCHAR(32) NOT NULL,
    port_in_mbps DOUBLE PRECISION NOT NULL,
    moving_avg_1h DOUBLE PRECISION NOT NULL,
    moving_std_1h DOUBLE PRECISION NOT NULL,
    upper_band DOUBLE PRECISION NOT NULL,
    lower_band DOUBLE PRECISION NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (detected_at, device_id)
);

CREATE TABLE IF NOT EXISTS streaming.device_latest_state (
    device_id VARCHAR(32) PRIMARY KEY,
    device_type VARCHAR(24) NOT NULL,
    building_id VARCHAR(16) NOT NULL,
    last_seen_utc TIMESTAMPTZ NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'ONLINE',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==========================================
-- WAREHOUSE SCHEMA (Star Schema)
-- ==========================================

CREATE TABLE IF NOT EXISTS warehouse.dim_building (
    building_key SERIAL PRIMARY KEY,
    building_id VARCHAR(16) NOT NULL UNIQUE,
    building_name VARCHAR(100) NOT NULL,
    faculty VARCHAR(100) NOT NULL,
    floor_count INT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_device (
    device_key SERIAL PRIMARY KEY,
    device_id VARCHAR(32) NOT NULL UNIQUE,
    device_type VARCHAR(24) NOT NULL,
    building_id VARCHAR(16) NOT NULL,
    max_bandwidth_mbps INT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_key INTEGER PRIMARY KEY,
    full_timestamp_utc TIMESTAMPTZ NOT NULL UNIQUE,
    date_utc DATE NOT NULL,
    date_wib DATE NOT NULL,
    hour_utc SMALLINT NOT NULL,
    hour_wib SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_lecture_hour BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_alert_type (
    alert_type_key SERIAL PRIMARY KEY,
    alert_code VARCHAR(32) NOT NULL UNIQUE,
    alert_name VARCHAR(100) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    description TEXT NOT NULL
);

INSERT INTO warehouse.dim_alert_type (alert_code, alert_name, severity, description) VALUES
('SLA_LATENCY_P95', 'Pelanggaran SLA Latensi p95 (> 100 ms)', 'CRITICAL', 'Latensi p95 window 5 menit melebihi 100 ms'),
('SLA_PACKET_LOSS', 'Pelanggaran SLA Packet Loss (> 2%)', 'CRITICAL', 'Packet loss window 5 menit melebihi 2%'),
('HEARTBEAT_MISSING', 'Perangkat Tanpa Heartbeat (> 2 menit)', 'WARNING', 'Perangkat tidak mengirim telemetri lebih dari 2 menit'),
('TRAFFIC_ANOMALY', 'Anomali Trafik (Moving Avg +/- 3 Std)', 'WARNING', 'Trafik keluar dari pita rata-rata bergerak 1 jam +/- 3 standar deviasi')
ON CONFLICT (alert_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS warehouse.fact_device_metric_hourly (
    id BIGSERIAL PRIMARY KEY,
    time_key INTEGER NOT NULL REFERENCES warehouse.dim_time(time_key),
    device_key INTEGER NOT NULL REFERENCES warehouse.dim_device(device_key),
    building_key INTEGER NOT NULL REFERENCES warehouse.dim_building(building_key),
    avg_port_in_mbps DOUBLE PRECISION,
    max_port_in_mbps DOUBLE PRECISION,
    avg_port_out_mbps DOUBLE PRECISION,
    total_errors_delta BIGINT,
    avg_uptime_s DOUBLE PRECISION,
    avg_clients DOUBLE PRECISION,
    max_clients INTEGER,
    avg_rssi_dbm DOUBLE PRECISION,
    avg_channel_util_pct DOUBLE PRECISION,
    avg_latency_ms DOUBLE PRECISION,
    p95_latency_ms DOUBLE PRECISION,
    avg_packet_loss_pct DOUBLE PRECISION,
    sample_count INTEGER NOT NULL,
    UNIQUE (time_key, device_key)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_sla_window (
    id BIGSERIAL PRIMARY KEY,
    window_start_utc TIMESTAMPTZ NOT NULL,
    date_wib DATE NOT NULL,
    building_key INTEGER NOT NULL REFERENCES warehouse.dim_building(building_key),
    p95_latency_ms DOUBLE PRECISION,
    max_packet_loss_pct DOUBLE PRECISION,
    avg_channel_util_pct DOUBLE PRECISION,
    is_sla_compliant BOOLEAN NOT NULL,
    violation_reason VARCHAR(128),
    UNIQUE (window_start_utc, building_key)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_alert (
    id BIGSERIAL PRIMARY KEY,
    alert_time_utc TIMESTAMPTZ NOT NULL,
    time_key INTEGER NOT NULL REFERENCES warehouse.dim_time(time_key),
    building_key INTEGER NOT NULL REFERENCES warehouse.dim_building(building_key),
    device_key INTEGER NOT NULL REFERENCES warehouse.dim_device(device_key),
    alert_type_key INTEGER NOT NULL REFERENCES warehouse.dim_alert_type(alert_type_key),
    metric_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    message TEXT NOT NULL,
    UNIQUE (alert_time_utc, device_key, alert_type_key)
);

CREATE TABLE IF NOT EXISTS warehouse.dq_reconciliation (
    id BIGSERIAL PRIMARY KEY,
    run_timestamp_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    batch_window VARCHAR(64) NOT NULL,
    anomaly_type VARCHAR(32) NOT NULL,
    disisipkan_count INTEGER NOT NULL,
    tertangkap_count INTEGER NOT NULL,
    ditangani_count INTEGER NOT NULL,
    capture_rate_pct DOUBLE PRECISION NOT NULL,
    UNIQUE (batch_window, anomaly_type)
);
