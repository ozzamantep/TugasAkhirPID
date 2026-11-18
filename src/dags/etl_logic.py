from collections import Counter
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

ANOMALY_TYPES = (
    "counter_reset",
    "inconsistent_unit",
    "out_of_range",
    "missing_heartbeat",
    "duplicate_message",
)


def _flatten_raw_docs(raw_docs: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for doc in raw_docs:
        data = doc.get("data") or {}
        rows.append(
            {
                "device_id": doc.get("device_id"),
                "device_type": doc.get("device_type"),
                "building_id": doc.get("building_id"),
                "timestamp": doc.get("timestamp"),
                "port_in_mbps": data.get("port_in_mbps"),
                "port_out_mbps": data.get("port_out_mbps"),
                "errors": data.get("errors"),
                "uptime_s": data.get("uptime_s"),
                "clients": data.get("clients"),
                "rssi_avg_dbm": data.get("rssi_avg_dbm"),
                "channel_util_pct": data.get("channel_util_pct"),
                "latency_ms": data.get("latency_ms"),
                "packet_loss_pct": data.get("packet_loss_pct"),
            }
        )
    df = pd.DataFrame(rows)
    df["ts_utc"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df.dropna(subset=["device_id", "building_id", "ts_utc"]).copy()


def clean_and_audit_telemetry(
    raw_docs: List[Dict[str, Any]],
) -> Tuple[pd.DataFrame, Dict[str, int], List[Dict[str, Any]]]:
    """Detect, count, and clean all 5 dirty data types from raw MongoDB telemetry."""
    if not raw_docs:
        return pd.DataFrame(), {k: 0 for k in ANOMALY_TYPES}, []

    df = _flatten_raw_docs(raw_docs)
    counts: Dict[str, int] = {k: 0 for k in ANOMALY_TYPES}

    # 1. Duplicate messages
    dup_mask = df.duplicated(subset=["device_id", "timestamp"], keep="first")
    counts["duplicate_message"] = int(dup_mask.sum())
    df = df.loc[~dup_mask].copy()

    df = df.sort_values(["device_id", "ts_utc"]).reset_index(drop=True)

    # 2. Missing heartbeat (> 120s gap between consecutive readings of same device)
    df["gap_s"] = df.groupby("device_id")["ts_utc"].diff().dt.total_seconds()
    hb_mask = df["gap_s"] > 120.0
    counts["missing_heartbeat"] = int(hb_mask.sum())
    hb_alerts: List[Dict[str, Any]] = []
    for _, row in df.loc[hb_mask].iterrows():
        hb_alerts.append(
            {
                "alert_time_utc": row["ts_utc"],
                "building_id": row["building_id"],
                "device_id": row["device_id"],
                "alert_code": "HEARTBEAT_MISSING",
                "metric_value": float(row["gap_s"]),
                "threshold_value": 120.0,
                "message": f"Perangkat {row['device_id']} tanpa heartbeat selama {int(row['gap_s'])} detik",
            }
        )

    # 3. Inconsistent unit (bps instead of Mbps, > 10,000 Mbps)
    unit_mask = (df["port_in_mbps"].fillna(0) > 10_000.0) | (df["port_out_mbps"].fillna(0) > 10_000.0)
    counts["inconsistent_unit"] = int(unit_mask.sum())
    df.loc[df["port_in_mbps"] > 10_000.0, "port_in_mbps"] /= 1_000_000.0
    df.loc[df["port_out_mbps"] > 10_000.0, "port_out_mbps"] /= 1_000_000.0

    # 4. Counter reset (errors or uptime_s drops vs previous reading for switch)
    is_switch = df["device_type"] == "switch"
    err_diff = df.groupby("device_id")["errors"].diff()
    up_diff = df.groupby("device_id")["uptime_s"].diff()
    reset_mask = is_switch & ((err_diff < 0) | (up_diff < 0))
    counts["counter_reset"] = int(reset_mask.sum())
    df["errors_delta"] = np.where(reset_mask, df["errors"].fillna(0), err_diff.clip(lower=0).fillna(0))

    # 5. Out-of-range physical values
    oor_loss = (df["packet_loss_pct"] < 0) | (df["packet_loss_pct"] > 100)
    oor_lat = df["latency_ms"] < 0
    oor_ch = (df["channel_util_pct"] < 0) | (df["channel_util_pct"] > 100)
    oor_rssi = df["rssi_avg_dbm"] > 0
    oor_clients = df["clients"] < 0
    oor_mask = oor_loss | oor_lat | oor_ch | oor_rssi | oor_clients
    counts["out_of_range"] = int(oor_mask.sum())

    df.loc[oor_loss, "packet_loss_pct"] = np.nan
    df.loc[oor_lat, "latency_ms"] = np.nan
    df.loc[oor_ch, "channel_util_pct"] = np.nan
    df.loc[oor_rssi, "rssi_avg_dbm"] = np.nan
    df.loc[oor_clients, "clients"] = np.nan

    return df, counts, hb_alerts


def reconcile_with_ledger(
    ledger_entries: List[Dict[str, Any]],
    caught_counts: Dict[str, int],
) -> pd.DataFrame:
    """Compare injected dirty data counts (disisipkan) vs caught/handled counts (tertangkap)."""
    injected = Counter(e.get("anomaly_type") for e in ledger_entries if e.get("anomaly_type"))
    rows = []
    for atype in ANOMALY_TYPES:
        disisipkan = int(injected.get(atype, 0))
        tertangkap = int(caught_counts.get(atype, 0))
        rate = round((min(tertangkap, disisipkan) / disisipkan) * 100.0, 2) if disisipkan > 0 else 100.0
        rows.append(
            {
                "anomaly_type": atype,
                "disisipkan_count": disisipkan,
                "tertangkap_count": tertangkap,
                "ditangani_count": tertangkap,
                "capture_rate_pct": rate,
            }
        )
    return pd.DataFrame(rows)


def compute_sla_windows(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cleaned telemetry into 5-minute building SLA compliance windows."""
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["window_start_utc"] = work["ts_utc"].dt.floor("5min")
    work["date_wib"] = (work["window_start_utc"] + pd.Timedelta(hours=7)).dt.date

    grouped = (
        work.groupby(["window_start_utc", "date_wib", "building_id"], as_index=False)
        .agg(
            p95_latency_ms=("latency_ms", lambda s: float(np.nanpercentile(s.dropna(), 95)) if s.notna().any() else np.nan),
            max_packet_loss_pct=("packet_loss_pct", "max"),
            avg_channel_util_pct=("channel_util_pct", "mean"),
        )
    )
    lat_bad = grouped["p95_latency_ms"].fillna(0) > 100.0
    loss_bad = grouped["max_packet_loss_pct"].fillna(0) > 2.0
    grouped["is_sla_compliant"] = ~(lat_bad | loss_bad)
    grouped["violation_reason"] = np.where(
        lat_bad & loss_bad,
        "LATENCY_AND_PACKET_LOSS",
        np.where(lat_bad, "LATENCY_P95_GT_100MS", np.where(loss_bad, "PACKET_LOSS_GT_2PCT", None)),
    )
    return grouped
