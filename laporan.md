# Laporan Proyek Akhir PID — Topik 5 Varian B: Jaringan Kampus (Network Telemetry)

**Anggota Kelompok:**
- `245150301111019` — Muhammad Dzakwan Ikram
- `245150307111048` — Ryan Fadhlurrahman
- `245150300111038` — Samuel Fransjemima

## 1. Diagram Arsitektur

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
         v                                     |                          |  - schema `streaming`        |
+--------------------+               +-------------------+                |  - schema `warehouse` (Star) |
|  MySQL (Master)    | ------------> |  Airflow DAG      | ----------------------------^ |
|  - buildings       |               |  (ETL Pandas +    |                               |
|  - devices         |               |   Data Quality)   |                               v
|  - sla_thresholds  |               +-------------------+                +------------------------------+
+--------------------+                                                    |  Grafana Dashboard           |
                                                                          |  (Real-time & Historis, WIB) |
                                                                          +------------------------------+
```

## 2. Skema Pesan JSON

Setiap perangkat (total **24 perangkat** di **4 gedung**: `BLD-FIK`, `BLD-FT`, `BLD-FEB`, `BLD-RECT`) mengirim pesan JSON ke MQTT dengan *timestamp* ISO 8601 UTC:

```json
{
  "device_id": "SW-FIK-01",
  "device_type": "switch",
  "building_id": "BLD-FIK",
  "timestamp": "2026-11-18T10:15:02Z",
  "data": { "port_in_mbps": 412.5, "port_out_mbps": 389.1, "errors": 4, "uptime_s": 86400 }
}
```

- **Switch (8 unit)**: `port_in_mbps`, `port_out_mbps`, `errors`, `uptime_s`
- **Access Point (12 unit)**: `clients`, `rssi_avg_dbm`, `channel_util_pct`
- **Probe Latensi (4 unit)**: `latency_ms`, `packet_loss_pct`, `target`

## 3. Star Schema Data Warehouse (`warehouse.*` di PostgreSQL)

- **Dimensi**:
  - `warehouse.dim_building` (`building_key`, `building_id`, `building_name`, `faculty`, `floor_count`)
  - `warehouse.dim_device` (`device_key`, `device_id`, `device_type`, `building_id`, `max_bandwidth_mbps`)
  - `warehouse.dim_time` (`time_key`, `full_timestamp_utc`, `date_utc`, `date_wib`, `hour_utc`, `hour_wib`, `day_of_week`, `is_lecture_hour`)
  - `warehouse.dim_alert_type` (`alert_type_key`, `alert_code`, `alert_name`, `severity`, `description`)
- **Fakta & Audit**:
  - `warehouse.fact_device_metric_hourly`: agregasi metrik per jam per perangkat (**4.056 baris** dari *backfill* 7 hari).
  - `warehouse.fact_sla_window`: kepatuhan SLA per window 5 menit per gedung (**8.076 baris**).
  - `warehouse.fact_alert`: riwayat seluruh alert SLA dan *missing heartbeat* (**3.678 baris**).
  - `warehouse.dq_reconciliation`: rekonsiliasi jumlah data kotor yang disisipkan vs tertangkap.

## 4. Ringkasan Data Quality (*Disisipkan vs Tertangkap*)

Berdasarkan eksekusi aktual Airflow DAG `campus_telemetry_etl` pada dataset *backfill* 7 hari (**MongoDB `raw_telemetry`** + *Injection Ledger* simulator):

| Jenis Data Kotor | Disisipkan (*Ledger*) | Tertangkap | Ditangani | Tingkat Tangkapan (%) | Penanganan di Pipeline |
|---|---:|---:|---:|---:|---|
| `counter_reset` | 1.024 | 1.021 | 1.021 | 99,71% | Deteksi selisih negatif `errors`/`uptime_s`; hitung `errors_delta` dari baseline reset |
| `duplicate_message` | 3.614 | 3.614 | 3.614 | 100,00% | Deduplikasi berdasarkan `(device_id, timestamp)` di Spark & Pandas ETL |
| `inconsistent_unit` | 1.024 | 1.024 | 1.024 | 100,00% | Deteksi nilai `> 10.000` (bps); konversi `/ 1.000.000` ke Mbps |
| `missing_heartbeat` | 3.603 | 3.535 | 3.535 | 98,11% | Deteksi jeda `> 120 detik` per perangkat; terbitkan alert `HEARTBEAT_MISSING` |
| `out_of_range` | 2.652 | 2.651 | 2.651 | 99,96% | Validasi batas fisik (`loss` 0–100%, `latency >= 0`, `channel_util` 0–100%); karantina ke `NaN` |

## 5. Hasil Tantangan Signature (Pelanggaran SLA & Anomali Trafik)

- **Deteksi SLA Real-Time (Spark Structured Streaming)**: Window 5 menit mengevaluasi `p95_latency_ms > 100 ms` dan `max_packet_loss_pct > 2%`. Saat pemicuan *flash crowd* KRS, sistem menerbitkan alert `SLA_LATENCY_P95`, `SLA_PACKET_LOSS`, dan `HEARTBEAT_MISSING` (40 alert *heartbeat*, 43 anomali trafik) ke `streaming.realtime_alerts`.
- **Anomali Trafik (*Moving-Average Band*)**: Rata-rata bergerak 1 jam ± 3 standar deviasi berhasil menandai **43 kejadian anomali trafik** pada *switch* saat lonjakan *flash crowd* ke `streaming.traffic_anomalies`.
- **Laporan Ketersediaan SLA Harian**: Dihitung di `warehouse.fact_sla_window` dan divisualisasikan pada dasbor Grafana historis (WIB).

## 6. Tiga Temuan Berbasis Data (Query Warehouse Nyata)

1. **Beban Jam Kuliah 2,45x Lebih Tinggi dari Luar Jam Kuliah**:
   - *Query*: `SELECT t.is_lecture_hour, ROUND(AVG(f.avg_port_in_mbps)::numeric, 2) AS avg_mbps, ROUND(AVG(f.avg_clients)::numeric, 1) AS avg_clients_per_ap FROM warehouse.fact_device_metric_hourly f JOIN warehouse.dim_time t ON f.time_key = t.time_key GROUP BY t.is_lecture_hour;`
   - *Hasil*: Jam kuliah (`t`) = **1.386,06 Mbps** (**32,4 klien/AP**); luar jam kuliah (`f`) = **564,91 Mbps** (**12,9 klien/AP**).
2. **`BLD-FEB` dan `BLD-RECT` Menjadi Gedung Paling Bermasalah Selama 7 Hari**:
   - *Query*: `WITH alert_counts AS (SELECT building_key, COUNT(*) AS total_alert FROM warehouse.fact_alert GROUP BY building_key) SELECT b.building_id AS kode_gedung, COUNT(*) FILTER (WHERE NOT f.is_sla_compliant) AS window_melanggar, (COUNT(*) FILTER (WHERE NOT f.is_sla_compliant) * 5) AS durasi_menit, ROUND(100.0 * SUM(CASE WHEN f.is_sla_compliant THEN 1 ELSE 0 END) / COUNT(*), 2) AS sla_patuh_pct, COALESCE(a.total_alert, 0) AS total_alert, ROUND(AVG(f.p95_latency_ms)::numeric, 2) AS rata_p95_ms FROM warehouse.fact_sla_window f JOIN warehouse.dim_building b ON f.building_key = b.building_key LEFT JOIN alert_counts a ON a.building_key = b.building_key GROUP BY b.building_id, a.total_alert ORDER BY window_melanggar DESC, total_alert DESC, rata_p95_ms DESC;`
   - *Hasil*: `BLD-FEB` peringkat 1 bermasalah (**31 window / 155 menit** melanggar, SLA **98,46%**, **955 alert**, rata-rata p95 **37,48 ms**), diikuti `BLD-RECT` (**18 window / 90 menit**, SLA **99,11%**, **933 alert**), `BLD-FT` (**13 window / 65 menit**, SLA **99,36%**, **894 alert**), dan `BLD-FIK` (**10 window / 50 menit**, SLA **99,50%**, **897 alert**). Trafik `BLD-RECT` (~2.500 Mbps) ~10x gedung fakultas (~227–256 Mbps) karena kapasitas *switch* inti `SW-RECT-01/02` adalah **10 Gbps** (`10.000` Mbps) dibanding **1 Gbps** (`1.000` Mbps) pada fakultas (bukan bug satuan bps/Mbps).
3. **Dampak *Flash Crowd* KRS dan Kongesti Lokal terhadap Tren SLA Harian**:
   - *Query*: `SELECT f.date_wib, b.building_id, COUNT(*) FILTER (WHERE NOT f.is_sla_compliant) AS pelanggaran, ROUND(100.0 * SUM(CASE WHEN f.is_sla_compliant THEN 1 ELSE 0 END) / COUNT(*), 2) AS sla_pct FROM warehouse.fact_sla_window f JOIN warehouse.dim_building b ON f.building_key = b.building_key GROUP BY f.date_wib, b.building_id ORDER BY f.date_wib, b.building_id;`
   - *Hasil*: Penurunan serentak pada **2026-11-14** dan **2026-11-17** (SLA turun ke **98,26%** di semua gedung, dan **96,88%** di `BLD-RECT` pada 11-14) disebabkan simulasi *flash crowd* pendaftaran KRS, sedangkan penurunan terpisah pada `BLD-FEB` (11-13, 11-15, 11-16: **97,57%**) dan `BLD-FT` (11-15: **98,96%**) berasal dari kongesti lokal jam sibuk.

## 7. Pernyataan Penggunaan AI Generatif

Proyek ini dikembangkan dengan bantuan alat AI berikut sesuai kebijakan mata kuliah:
- **Alat yang digunakan**: **Antigravity** (model **Gemini 4 Hydrogen Medium**), *skill* Matt Pocock (`setup-matt-pocock-skills`, `grill-with-docs`, `to-spec`, `to-tickets`, `implement`), **`codebase-memory` MCP**, dan **`aislop` CLI** (`npx aislop scan src --json`, skor kualitas meningkat dari 53 menjadi 94 dengan 0 *error* dan 2 *warning* non-kritis).
- **Bagian yang dibantu AI**: Perancangan arsitektur dan *star schema* (`CONTEXT.md`, `docs/adr/`, `src/sql/`), implementasi kode Python (simulator, bridge MQTT-Kafka, Spark Structured Streaming, Airflow ETL DAG), konfigurasi `docker-compose.yml` dan *provisioning* Grafana, penulisan *unit test* (`tests/`), serta penyusunan dokumentasi (`README.md`, `laporan.md`). Seluruh hasil telah diverifikasi secara langsung melalui *unit test* dan eksekusi *pipeline* Docker Compose.
