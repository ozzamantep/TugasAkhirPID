# 06: Provisioned Grafana Real-Time and Historical Dashboards

**What to build:** Provision Grafana datasources (`Postgres-Streaming`, `Postgres-Warehouse`, `MySQL-Master`) and Indonesian-labeled dashboards (displaying WIB / `Asia/Jakarta` time) covering both real-time streaming KPIs/alerts/anomalies and 7-day historical star-schema analytics (SLA availability trend, most problematic building, data quality `disisipkan vs tertangkap`).

**Blocked by:** 05: Airflow Scheduled ETL, Data Quality Reconciliation, and Star Schema Load

**Status:** resolved

- [x] Grafana auto-provisions `Postgres-Campus` datasource and both dashboards on `docker compose up`
- [x] Real-time dashboard panels show live 1m/5m metrics, WiFi clients, SLA/heartbeat alerts, and moving-average traffic anomalies
- [x] Historical dashboard panels show 7-day problematic buildings, daily SLA availability trend, and DQ `disisipkan vs tertangkap`
- [x] W5 Checkpoint 2 verified via Grafana API and live PostgreSQL queries
