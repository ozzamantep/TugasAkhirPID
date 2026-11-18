# Use Single PostgreSQL Instance with Streaming and Warehouse Schemas

**Status:** accepted

We need both real-time streaming outputs (1-minute/5-minute windowed KPIs, alerts, anomalies) and a historical star schema warehouse queryable by Grafana with zero custom plugins. We decided to run a single PostgreSQL 16 instance hosting an `airflow` metadata database and a `campus_telemetry` database split into `streaming` (Spark Structured Streaming sink) and `warehouse` (Airflow batch star schema) schemas, rather than running separate Postgres instances or an extra time-series store. This keeps memory usage low and lets Grafana query both real-time and historical views through native PostgreSQL datasources.
