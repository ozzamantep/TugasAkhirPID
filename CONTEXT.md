# Campus Network Telemetry

Domain glossary for the Campus Network Telemetry data engineering pipeline (PID Final Project, Topic 5 Variant B).

## Language

**Telemetry Event**:
A single JSON observation emitted by a Network Device containing `device_id`, `device_type`, `building_id`, `timestamp` (ISO 8601 UTC), and `data`.
_Avoid_: Sensor reading, log message, packet

**Network Device**:
One of 24 simulated campus endpoints: `switch` (8), `access_point` (12), or `latency_probe` (4).
_Avoid_: Sensor, node, hardware

**Building**:
A physical campus building identified by `building_id` (at least 3 buildings) that groups Network Devices.
_Avoid_: Rack, site, zone, location

**Flash Crowd**:
A scheduled or manually triggerable surge in campus network traffic and WiFi clients (simulating KRS course registration).
_Avoid_: Traffic spike, DDoS, overload

**Dirty Data Injection**:
Controlled, seeded injection of five anomaly types (`counter_reset`, `inconsistent_unit`, `out_of_range`, `missing_heartbeat`, `duplicate_message`) by the simulator.
_Avoid_: Noise, corrupt data, bad rows

**Injection Ledger**:
The authoritative record written by the simulator counting every Dirty Data Injection event per anomaly type (`disisipkan`).
_Avoid_: Error log, audit trail

**SLA Violation**:
A 5-minute window breach where latency p95 exceeds 100 ms or packet loss exceeds 2%.
_Avoid_: Alert, breach, incident

**Traffic Anomaly**:
A telemetry observation whose throughput falls outside the 1-hour moving-average band (mean +/- 3 standard deviations).
_Avoid_: Outlier, spike

**Heartbeat Timeout**:
Absence of Telemetry Events from a Network Device for more than 2 minutes.
_Avoid_: Disconnect, downtime

**Daily SLA Availability**:
The percentage of compliant 5-minute SLA windows per Building per day.
_Avoid_: Uptime, SLA score
