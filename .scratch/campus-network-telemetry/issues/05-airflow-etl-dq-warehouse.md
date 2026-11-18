# 05: Airflow Scheduled ETL, Data Quality Reconciliation, and Star Schema Load

**What to build:** Configure Airflow (LocalExecutor, auto-initialized connections & user) and a scheduled DAG running Pandas ETL from MongoDB `raw_telemetry` + MySQL master data, detecting/handling all 5 dirty data types, reconciling caught counts against the simulator's Injection Ledger (`disisipkan vs tertangkap`), computing Daily SLA Availability per building, and loading the PostgreSQL `warehouse` star schema (`dim_*`, `fact_*`, `dq_reconciliation`).

**Blocked by:** 04: Spark Structured Streaming KPIs, Alerts, and Signature Challenge

**Status:** resolved

- [x] Unit tests verify all 5 dirty data detectors/handlers, ledger reconciliation, and star schema SLA logic (2/2 passed)
- [x] 7-day backfill + Airflow DAG `campus_telemetry_etl` run completes with `success` state
- [x] Warehouse SQL queries answer problematic building (7 days), daily SLA trend, and `disisipkan vs tertangkap`
- [x] `aislop` manual/rule review passes on ETL and DAG code
