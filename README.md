# Proyek Akhir PID — Topik 5 Varian B: Jaringan Kampus (Network Telemetry)

Sistem *pipeline* data *end-to-end* lokal berbasis Docker Compose untuk pemantauan telemetri jaringan kampus (*switch*, *access point* WiFi, dan *latency probe*) secara *real-time* dan historis.

## Anggota Kelompok

| NIM | Nama | Peran Tim | Peran Teknis & Komponen |
|---|---|---|---|
| `245150300111038` | Samuel Fransjemima | Eksekutor | Ingestion & Streaming (`src/simulator`, `src/bridge`, `src/spark`) |
| `245150307111048` | Ryan Fadhlurrahman | Kreatif / Pencetus Ide | Dashboard & Integration (`src/dashboard`) |
| `245150301111019` | Muhammad Dzakwan Ikram | Finisher | Storage & Warehouse + Orchestration & Data Quality (`src/sql`, `src/dags`, `docker-compose.yml`, `README.md`, `laporan.md`, `slides.pdf`) |

## Arsitektur Singkat

1. **Simulator Python** (`src/simulator/`): 24 perangkat di 4 gedung (`BLD-FIK`, `BLD-FT`, `BLD-FEB`, `BLD-RECT`), ~1 pesan/detik/perangkat ke **Mosquitto MQTT**, ditambah mode *backfill* 7 hari ke **MongoDB** serta penyisipan 5 jenis data kotor terkontrol (*injection ledger*).
2. **Bridge & Raw Storage** (`src/bridge/`): Bridge MQTT → **Apache Kafka (KRaft)** → Consumer ke **MongoDB** (`campus_raw.raw_telemetry`).
3. **Stream Processing** (`src/spark/`): **Spark Structured Streaming** menghitung agregat window 1 & 5 menit per gedung, jumlah klien WiFi, alert SLA (*latency* p95 > 100 ms, *packet loss* > 2%), alert *heartbeat* > 2 menit, dan anomali trafik (*moving average* 1 jam ± 3 std) ke **PostgreSQL** (`streaming.*`).
4. **Master Data & Batch ETL** (`src/sql/`, `src/dags/`): **MySQL** menyimpan data referensi perangkat, gedung, dan ambang SLA. **Airflow DAG** menjalankan ETL Pandas + pemeriksaan *data quality* (*disisipkan vs tertangkap*) dan mengisi *star schema* **PostgreSQL** (`warehouse.*`).
5. **Dashboard** (`src/dashboard/`): **Grafana** ter-provision otomatis menampilkan dasbor *real-time* dan historis (zona waktu tampilan WIB / `Asia/Jakarta`, penyimpanan UTC).

## Prasyarat

- Docker Engine & Docker Compose v2+
- RAM bebas minimal 6 GB (disarankan 8 GB)

## Cara Menjalankan dari Nol

1. Salin berkas konfigurasi lingkungan:
   ```bash
   cp .env.example .env
   ```
2. Jalankan seluruh sistem dengan satu perintah:
   ```bash
   docker compose up -d --build
   ```
3. Tunggu **~3 menit** setelah `docker compose up` agar seluruh layanan `healthy`, *micro-batch* Spark Structured Streaming terisi, *baseline* *moving average* terbentuk, dan DAG Airflow selesai berjalan:
   ```bash
   docker compose ps
   ```
4. Sebelum demo dasbor *real-time*, picu *flash crowd* agar alert SLA dan panel **Anomali Trafik** terisi (lihat bagian *Simulasi Flash Crowd* di bawah).
5. Akses antarmuka web:
   - **Grafana**: http://localhost:3000 (`admin` / `admin`) → Folder **Jaringan Kampus (PID Topik 5)**
     - *Pemantauan Jaringan Kampus — Real-Time (WIB)*
     - *Analitik Historis & SLA Jaringan Kampus (7 Hari, WIB)*
   - **Airflow UI**: http://localhost:8080 (`admin` / `admin`) → DAG `campus_telemetry_etl`

## Simulasi Flash Crowd (Pendaftaran KRS)

Simulator mode *live* memiliki jadwal *flash crowd* otomatis yang dapat diatur lewat `.env` (`FLASH_CROWD_EVERY_MIN=20`, `FLASH_CROWD_DURATION_MIN=5`; isi `0` untuk mematikan). Setelah *warm-up* baseline normal, *flash crowd* otomatis aktif di akhir tiap siklus sehingga alert SLA dan panel **Anomali Trafik** terisi tanpa pemicu manual.

Untuk memicu *flash crowd* secara manual kapan saja pada simulator *live*:
```bash
docker exec pid-simulator touch /shared/ledger/flash_crowd.flag
```
Untuk menghentikannya kembali:
```bash
docker exec pid-simulator rm -f /shared/ledger/flash_crowd.flag
```

## Menjalankan Pengujian Otomatis

```bash
python -m unittest discover -s tests -v
```

## Menghentikan & Membersihkan Volume

```bash
docker compose down -v
```

## Video Demo

- Link video demo: **(ISI SETELAH UPLOAD)** (maksimal 5 menit, cadangan bila demo langsung gagal).
- Panduan skrip demo tersedia di [video-demo/script-demo-5menit.md](video-demo/script-demo-5menit.md).

