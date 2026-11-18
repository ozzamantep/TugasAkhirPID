# Proposal Proyek Akhir PID — Topik 5 Varian B: Jaringan Kampus (Network Telemetry)

## 1. Identitas Topik & Varian

- **Topik**: 5 — Data Center / Network Telemetry
- **Varian**: **Varian B — Jaringan Kampus**
- **Tujuan**: Membangun *pipeline* data *end-to-end* lokal berbasis Docker Compose untuk memantau kesehatan jaringan kampus secara *real-time*, mendeteksi pelanggaran SLA, anomali trafik, dan perangkat *offline* (*missing heartbeat*), serta menganalisis ketersediaan SLA harian dan gedung paling bermasalah selama 7 hari terakhir melalui *data warehouse* *star schema*.

## 2. Diagram Arsitektur

```text
+--------------------+     MQTT      +-------------------+     Kafka      +------------------------------+
|  Python Simulator  | ------------> |  MQTT-Kafka       | -------------> |  Spark Structured Streaming  |
|  (24 Perangkat,    |  (Mosquitto)  |  Bridge           |  (KRaft)       |  - Window 1m & 5m per Gedung |
|   Dirty Data +     |               +-------------------+                |  - SLA & Heartbeat Alerts    |
|   Injection Ledger)|                         |                          |  - 1h Moving-Avg +/- 3 std   |
+--------------------+                         |                          +------------------------------+
         |                                     v                                         |
         | (7-day Backfill)          +-------------------+                               v
         +-------------------------> |  MongoDB (Raw)    |                +------------------------------+
         |                           +-------------------+                |  PostgreSQL:                 |
         |                                     |                          |  - schema `streaming`        |
         v                                     v                          |  - schema `warehouse` (Star) |
+--------------------+               +-------------------+                +------------------------------+
|  MySQL (Master)    | ------------> |  Airflow DAG      | ----------------------------^ |
|  - buildings       |               |  (ETL Pandas +    |                               |
|  - devices         |               |   Data Quality)   |                               v
|  - sla_thresholds  |               +-------------------+                +------------------------------+
+--------------------+                                                    |  Grafana Dashboard           |
                                                                          |  (Real-time & Historis, WIB) |
                                                                          +------------------------------+
```

## 3. Daftar Perangkat (24 Perangkat, 4 Gedung) & Skema Pesan

### 3.1 Pembagian Perangkat per Gedung

| `building_id` | Nama Gedung | Switch (2/gedung) | Access Point (3/gedung) | Probe Latensi (1/gedung) | Total |
|---|---|---|---|---|---|
| `BLD-FIK` | Gedung Fakultas Ilmu Komputer | `SW-FIK-01`, `SW-FIK-02` | `AP-FIK-01`, `AP-FIK-02`, `AP-FIK-03` | `PRB-FIK-01` | 6 |
| `BLD-FT` | Gedung Fakultas Teknik | `SW-FT-01`, `SW-FT-02` | `AP-FT-01`, `AP-FT-02`, `AP-FT-03` | `PRB-FT-01` | 6 |
| `BLD-FEB` | Gedung Fakultas Ekonomi & Bisnis | `SW-FEB-01`, `SW-FEB-02` | `AP-FEB-01`, `AP-FEB-02`, `AP-FEB-03` | `PRB-FEB-01` | 6 |
| `BLD-RECT` | Gedung Rektorat & Layanan Pusat | `SW-RECT-01`, `SW-RECT-02` | `AP-RECT-01`, `AP-RECT-02`, `AP-RECT-03` | `PRB-RECT-01` | 6 |
| **Total** | **4 Gedung** | **8 Switch** | **12 Access Point** | **4 Probe Latensi** | **24** |

### 3.2 Skema Pesan JSON

Setiap pesan memiliki field wajib `device_id`, `device_type`, `building_id`, `timestamp` (ISO 8601 UTC), dan `data`:

```json
{
  "device_id": "SW-FIK-01",
  "device_type": "switch",
  "building_id": "BLD-FIK",
  "timestamp": "2026-11-18T10:15:02Z",
  "data": { "port_in_mbps": 412.5, "port_out_mbps": 389.1, "errors": 4, "uptime_s": 86400 }
}
```

- **Switch (`switch`)**: `port_in_mbps`, `port_out_mbps`, `errors`, `uptime_s` (utilisasi normal < 70%).
- **Access Point (`access_point`)**: `clients`, `rssi_avg_dbm`, `channel_util_pct`.
- **Probe Latensi (`latency_probe`)**: `latency_ms`, `packet_loss_pct`, `target` (normal: latensi < 50 ms, loss < 1%).

## 4. Pembagian Peran (3 Anggota, 4 Peran)

| Anggota (NIM & Nama) | Peran | Tanggung Jawab Utama | Komponen & Prefix Commit |
|---|---|---|---|
| `245150301111019` — **Muhammad Dzakwan Ikram** | **Ingestion & Streaming** | Simulator Python (live + backfill + dirty data + ledger), Mosquitto, Bridge MQTT-Kafka, Spark Structured Streaming | `sim:`, `bridge:`, `spark:` |
| `245150307111048` — **Ryan Fadhlurrahman** | **Storage & Warehouse + Orchestration & Data Quality** | Master data MySQL, raw storage MongoDB, star schema PostgreSQL, Airflow DAG, ETL Pandas, pemeriksaan & rekonsiliasi 5 jenis data kotor | `wh:`, `dags:` |
| `245150300111038` — **Samuel Fransjemima** | **Dashboard & Integration** | Provisioning Grafana (real-time & historis WIB), `docker-compose.yml`, integrasi *end-to-end*, dokumentasi | `dash:`, `docs:` |
