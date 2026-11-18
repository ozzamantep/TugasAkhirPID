# Skrip Demo Video 5 Menit — Topik 5 Varian B (Jaringan Kampus)

> **Persiapan sebelum merekam/demo**: Setelah `docker compose up -d --build`, tunggu **~3 menit** hingga seluruh layanan sehat, Spark selesai *warm-up*, dan Airflow DAG `campus_telemetry_etl` berstatus `success`.

1. **[00:00 – 00:45] Pembukaan & Arsitektur**:
   - Perkenalkan tim, Topik 5 Varian B (Jaringan Kampus, 24 perangkat di 4 gedung).
   - Tunjukkan `docker compose ps` (semua service aktif dan *healthy*).
2. **[00:45 – 01:45] Ingestion & Raw Storage**:
   - Tunjukkan log `pid-simulator` (live stream + backfill 7 hari + ledger) dan `pid-bridge`.
   - Tunjukkan jumlah dokumen di MongoDB (`raw_telemetry`) dan data master di MySQL (`buildings`, `devices`, `sla_thresholds`).
3. **[01:45 – 03:00] Spark Structured Streaming & Tantangan Signature**:
   - Buka Grafana Dashboard **Pemantauan Jaringan Kampus — Real-Time (WIB)**.
   - Picu *flash crowd* (`touch /shared/ledger/flash_crowd.flag`), tunjukkan lonjakan trafik, klien WiFi, pelanggaran SLA (latensi p95 > 100 ms / *packet loss* > 2%), serta deteksi anomali trafik (*moving-average* 1 jam ± 3 std).
4. **[03:00 – 04:15] Airflow ETL, Data Quality, & Data Warehouse**:
   - Buka Airflow UI, tunjukkan DAG `campus_telemetry_etl` berstatus `success`.
   - Buka Grafana Dashboard **Analitik Historis & SLA Jaringan Kampus (7 Hari, WIB)**: tunjukkan gedung paling bermasalah selama 7 hari, tren ketersediaan SLA harian, dan tabel rekonsiliasi *Disisipkan vs Tertangkap*.
5. **[04:15 – 05:00] Penutup & Kesimpulan**:
   - Ringkas 3 temuan berbasis data dan tutup presentasi.
