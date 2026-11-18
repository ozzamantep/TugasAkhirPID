# 01: Proposal, Environment Config, and Master/Warehouse Schemas

**What to build:** Establish the W1 proposal (`docs/proposal.md` with architecture diagram, 24-device list, message schema, 4-role team split, Variant B), `.env.example`, MySQL master schema + seed (`buildings`, `devices`, `sla_thresholds`), and PostgreSQL `streaming` + `warehouse` star schema DDL (`dim_building`, `dim_device`, `dim_time`, `dim_alert_type`, `fact_device_metric_hourly`, `fact_sla_window`, `fact_alert`, `dq_reconciliation`).

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] `docs/proposal.md` covers all 4 W1 items (architecture diagram, 24-device inventory + JSON schema, 4-role division, Variant B)
- [x] `.env.example` defines all required credentials and simulator/pipeline defaults with no real secrets committed
- [x] MySQL init SQL creates and seeds 4 buildings, 24 devices, and SLA thresholds
- [x] PostgreSQL init SQL creates `airflow` DB, `campus_telemetry` DB, `streaming` schema tables, and `warehouse` star schema tables
