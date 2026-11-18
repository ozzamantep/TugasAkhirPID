-- Master Data Schema & Seed for Campus Network Telemetry (Topic 5 Variant B)

CREATE DATABASE IF NOT EXISTS campus_master;
USE campus_master;

CREATE TABLE IF NOT EXISTS buildings (
    building_id VARCHAR(16) PRIMARY KEY,
    building_name VARCHAR(100) NOT NULL,
    faculty VARCHAR(100) NOT NULL,
    floor_count INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(32) PRIMARY KEY,
    device_type ENUM('switch', 'access_point', 'latency_probe') NOT NULL,
    building_id VARCHAR(16) NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    target_host VARCHAR(100) NULL,
    max_bandwidth_mbps INT NULL,
    status ENUM('active', 'maintenance') NOT NULL DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (building_id) REFERENCES buildings(building_id)
);

CREATE TABLE IF NOT EXISTS sla_thresholds (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(64) NOT NULL,
    device_type ENUM('switch', 'access_point', 'latency_probe', 'all') NOT NULL,
    warning_threshold DOUBLE NOT NULL,
    critical_threshold DOUBLE NOT NULL,
    unit VARCHAR(16) NOT NULL,
    window_minutes INT NOT NULL DEFAULT 5,
    description VARCHAR(255) NOT NULL,
    UNIQUE KEY uq_metric_device (metric_name, device_type)
);

-- Seed 4 Campus Buildings
INSERT INTO buildings (building_id, building_name, faculty, floor_count) VALUES
('BLD-FIK', 'Gedung Fakultas Ilmu Komputer', 'Ilmu Komputer', 6),
('BLD-FT', 'Gedung Fakultas Teknik', 'Teknik', 5),
('BLD-FEB', 'Gedung Fakultas Ekonomi & Bisnis', 'Ekonomi & Bisnis', 4),
('BLD-RECT', 'Gedung Rektorat & Layanan Pusat', 'Administrasi Pusat', 7)
ON DUPLICATE KEY UPDATE building_name = VALUES(building_name);

-- Seed 24 Network Devices (8 Switches, 12 Access Points, 4 Latency Probes)
INSERT INTO devices (device_id, device_type, building_id, ip_address, target_host, max_bandwidth_mbps) VALUES
-- BLD-FIK (6)
('SW-FIK-01', 'switch', 'BLD-FIK', '10.10.1.1', NULL, 1000),
('SW-FIK-02', 'switch', 'BLD-FIK', '10.10.1.2', NULL, 1000),
('AP-FIK-01', 'access_point', 'BLD-FIK', '10.10.1.11', NULL, 300),
('AP-FIK-02', 'access_point', 'BLD-FIK', '10.10.1.12', NULL, 300),
('AP-FIK-03', 'access_point', 'BLD-FIK', '10.10.1.13', NULL, 300),
('PRB-FIK-01', 'latency_probe', 'BLD-FIK', '10.10.1.21', 'core-dc.campus.ac.id', NULL),
-- BLD-FT (6)
('SW-FT-01', 'switch', 'BLD-FT', '10.10.2.1', NULL, 1000),
('SW-FT-02', 'switch', 'BLD-FT', '10.10.2.2', NULL, 1000),
('AP-FT-01', 'access_point', 'BLD-FT', '10.10.2.11', NULL, 300),
('AP-FT-02', 'access_point', 'BLD-FT', '10.10.2.12', NULL, 300),
('AP-FT-03', 'access_point', 'BLD-FT', '10.10.2.13', NULL, 300),
('PRB-FT-01', 'latency_probe', 'BLD-FT', '10.10.2.21', 'core-dc.campus.ac.id', NULL),
-- BLD-FEB (6)
('SW-FEB-01', 'switch', 'BLD-FEB', '10.10.3.1', NULL, 1000),
('SW-FEB-02', 'switch', 'BLD-FEB', '10.10.3.2', NULL, 1000),
('AP-FEB-01', 'access_point', 'BLD-FEB', '10.10.3.11', NULL, 300),
('AP-FEB-02', 'access_point', 'BLD-FEB', '10.10.3.12', NULL, 300),
('AP-FEB-03', 'access_point', 'BLD-FEB', '10.10.3.13', NULL, 300),
('PRB-FEB-01', 'latency_probe', 'BLD-FEB', '10.10.3.21', 'sia-krs.campus.ac.id', NULL),
-- BLD-RECT (6)
('SW-RECT-01', 'switch', 'BLD-RECT', '10.10.4.1', NULL, 10000),
('SW-RECT-02', 'switch', 'BLD-RECT', '10.10.4.2', NULL, 10000),
('AP-RECT-01', 'access_point', 'BLD-RECT', '10.10.4.11', NULL, 600),
('AP-RECT-02', 'access_point', 'BLD-RECT', '10.10.4.12', NULL, 600),
('AP-RECT-03', 'access_point', 'BLD-RECT', '10.10.4.13', NULL, 600),
('PRB-RECT-01', 'latency_probe', 'BLD-RECT', '10.10.4.21', 'sia-krs.campus.ac.id', NULL)
ON DUPLICATE KEY UPDATE building_id = VALUES(building_id);

-- Seed SLA & Operational Thresholds
INSERT INTO sla_thresholds (metric_name, device_type, warning_threshold, critical_threshold, unit, window_minutes, description) VALUES
('latency_p95_ms', 'latency_probe', 50.0, 100.0, 'ms', 5, 'Pelanggaran SLA bila latensi p95 window 5 menit > 100 ms'),
('packet_loss_pct', 'latency_probe', 1.0, 2.0, '%', 5, 'Pelanggaran SLA bila packet loss > 2%'),
('switch_util_pct', 'switch', 70.0, 90.0, '%', 5, 'Batas utilisasi port switch normal < 70%'),
('heartbeat_gap_s', 'all', 90.0, 120.0, 's', 2, 'Alert perangkat tanpa heartbeat > 2 menit (120 detik)')
ON DUPLICATE KEY UPDATE critical_threshold = VALUES(critical_threshold);
