# Use 1-Minute Backfill Resolution and Shared Injection Ledger

**Status:** accepted

The pipeline requires 7 days of historical data for the Airflow ETL and star schema, plus an exact reconciliation of injected vs. caught dirty data (`disisipkan vs tertangkap`). Live simulation runs at ~1 message/second/device, while the 7-day backfill generates 1-minute resolution telemetry (~241,920 events) directly into MongoDB alongside a shared JSONL Injection Ledger on a mounted Docker volume. This keeps backfill execution under 15 seconds while preserving realistic daily patterns, a flash crowd event, and deterministic seeded dirty-data counts for reconciliation.
