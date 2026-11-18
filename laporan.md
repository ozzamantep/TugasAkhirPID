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

Berdasarkan eksekusi aktual Airflow DAG `campus_telemetry_etl` pada dataset *backfill* 7 hari (**234.747 dokumen mentah** di MongoDB + *Injection Ledger* simulator):

| Jenis Data Kotor | Disisipkan (*Ledger*) | Tertangkap | Ditangani | Tingkat Tangkapan (%) | Penanganan di Pipeline |
|---|---:|---:|---:|---:|---|
| `counter_reset` | 1.051 | 1.048 | 1.048 | 99,71% | Deteksi selisih negatif `errors`/`uptime_s`; hitung `errors_delta` dari baseline reset |
| `duplicate_message` | 3.696 | 3.695 | 3.695 | 99,97% | Deduplikasi berdasarkan `(device_id, timestamp)` di Spark & Pandas ETL |
| `inconsistent_unit` | 1.052 | 1.052 | 1.052 | 100,00% | Deteksi nilai `> 10.000` (bps); konversi `/ 1.000.000` ke Mbps |
| `missing_heartbeat` | 3.676 | 3.598 | 3.598 | 97,88% | Deteksi jeda `> 120 detik` per perangkat; terbitkan alert `HEARTBEAT_MISSING` |
| `out_of_range` | 2.705 | 2.705 | 2.705 | 100,00% | Validasi batas fisik (`loss` 0–100%, `latency >= 0`, `channel_util` 0–100%); karantina ke `NaN` |

## 5. Hasil Tantangan Signature (Pelanggaran SLA & Anomali Trafik)

- **Deteksi SLA Real-Time (Spark Structured Streaming)**: Window 5 menit mengevaluasi `p95_latency_ms > 100 ms` dan `max_packet_loss_pct > 2%`. Saat pemicuan *flash crowd* KRS, sistem menerbitkan alert `SLA_LATENCY_P95`, `SLA_PACKET_LOSS`, dan `HEARTBEAT_MISSING` (40 alert *heartbeat*, 43 anomali trafik) ke `streaming.realtime_alerts`.
- **Anomali Trafik (*Moving-Average Band*)**: Rata-rata bergerak 1 jam ± 3 standar deviasi berhasil menandai **43 kejadian anomali trafik** pada *switch* saat lonjakan *flash crowd* ke `streaming.traffic_anomalies`.
- **Laporan Ketersediaan SLA Harian**: Dihitung di `warehouse.fact_sla_window` dan divisualisasikan pada dasbor Grafana historis (WIB).

## 6. Tiga Temuan Berbasis Data (Query Warehouse Nyata)

1. **Beban Jam Kuliah 2,43x Lebih Tinggi dari Luar Jam Kuliah**:
   - *Query*: `SELECT t.is_lecture_hour, ROUND(AVG(f.avg_port_in_mbps)::numeric, 2) AS avg_mbps, ROUND(AVG(f.avg_clients)::numeric, 1) AS avg_clients_per_ap FROM warehouse.fact_device_metric_hourly f JOIN warehouse.dim_time t ON f.time_key = t.time_key GROUP BY t.is_lecture_hour;`
   - *Hasil*: Jam kuliah (`t`) = **1.382,62 Mbps** (**32,3 klien/AP**); luar jam kuliah (`f`) = **568,30 Mbps** (**12,9 klien/AP**).
2. **Gedung `BLD-FEB` dan `BLD-RECT` Mengalami Tekanan Tertinggi**:
   - *Query*: `SELECT b.building_id, ROUND(AVG(f.avg_latency_ms)::numeric, 2) AS avg_lat_ms, ROUND(AVG(f.p95_latency_ms)::numeric, 2) AS avg_p95_ms, ROUND(AVG(f.avg_clients)::numeric, 1) AS avg_clients FROM warehouse.fact_device_metric_hourly f JOIN warehouse.dim_building b ON f.building_key = b.building_key GROUP BY b.building_id ORDER BY avg_p95_ms DESC;`
   - *Hasil*: `BLD-FEB` mencatat rata-rata latensi p95 tertinggi (**38,82 ms**; rata-rata **32,92 ms**) dibanding `BLD-FT` (**37,72 ms**), `BLD-RECT` (**38,26 ms**), dan `BLD-FIK` (**36,91 ms**), sedangkan `BLD-RECT` memiliki kepadatan klien WiFi tertinggi (**24,7 klien/AP**).
3. **Dampak Terukur *Flash Crowd* terhadap Ketersediaan SLA Harian**:
   - *Query*: `SELECT f.date_wib, COUNT(*) AS total_windows, COUNT(*) FILTER (WHERE NOT f.is_sla_compliant) AS violations, ROUND(100.0 * SUM(CASE WHEN f.is_sla_compliant THEN 1 ELSE 0 END) / COUNT(*), 2) AS sla_pct FROM warehouse.fact_sla_window f GROUP BY f.date_wib ORDER BY f.date_wib;`
   - *Hasil*: Hari normal mencapai **100,00%** (0 pelanggaran dari 1.152 window/hari). Pada dua hari simulasi *flash crowd* KRS (`2026-11-14` dan `2026-11-17` WIB), terjadi masing-masing **20 pelanggaran SLA**, menurunkan ketersediaan SLA harian menjadi **98,26%** (total 7 hari **99,50%**).

## 7. Pernyataan Penggunaan AI Generatif

Proyek ini dikembangkan dengan bantuan alat AI berikut sesuai kebijakan mata kuliah:
- **Alat yang digunakan**: **Antigravity** (model **Gemini 4 Hydrogen Medium**), *skill* Matt Pocock (`setup-matt-pocock-skills`, `grill-with-docs`, `to-spec`, `to-tickets`, `implement`), **`codebase-memory` MCP**, dan **`aislop` CLI** (`npx aislop scan src --json`, skor kualitas meningkat dari 53 menjadi 94 dengan 0 *error* dan 2 *warning* non-kritis).
- **Bagian yang dibantu AI**: Perancangan arsitektur dan *star schema* (`CONTEXT.md`, `docs/adr/`, `src/sql/`), implementasi kode Python (simulator, bridge MQTT-Kafka, Spark Structured Streaming, Airflow ETL DAG), konfigurasi `docker-compose.yml` dan *provisioning* Grafana, penulisan *unit test* (`tests/`), serta penyusunan dokumentasi (`README.md`, `laporan.md`). Seluruh hasil telah diverifikasi secara langsung melalui *unit test* dan eksekusi *pipeline* Docker Compose.
