# 02: Campus Network Telemetry Simulator and Injection Ledger

**What to build:** Build the Python simulator supporting live MQTT publishing (~1 msg/s/device for 24 devices across 4 buildings), realistic lecture-hour/night traffic cycles, scheduled/triggerable Flash Crowd (KRS) events, 7-day 1-minute historical backfill mode into MongoDB, and controlled seeded Dirty Data Injection across 5 types (`counter_reset`, `inconsistent_unit`, `out_of_range`, `missing_heartbeat`, `duplicate_message`) with an authoritative JSONL Injection Ledger.

**Blocked by:** 01: Proposal, Environment Config, and Master/Warehouse Schemas

**Status:** resolved

- [x] Unit tests (pytest/unittest) verify JSON message schema, 24-device topology, diurnal + flash crowd profiles, deterministic seeded dirty-data injection, and exact ledger counts
- [x] Simulator runs in live MQTT mode and 7-day backfill mode cleanly
- [x] `aislop` manual/rule review passes on simulator code
