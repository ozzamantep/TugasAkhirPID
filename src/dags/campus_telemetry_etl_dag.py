import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import mysql.connector
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.dags.etl_logic import (
    clean_and_audit_telemetry,
    compute_sla_windows,
    reconcile_with_ledger,
)

logger = logging.getLogger(__name__)


def _pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "campus_telemetry"),
        user=os.getenv("POSTGRES_USER", "pid_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pid_secret_password"),
    )


def _mysql_conn() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "campus_master"),
        user=os.getenv("MYSQL_USER", "pid_mysql"),
        password=os.getenv("MYSQL_PASSWORD", "pid_mysql_secret"),
    )


def sync_dimensions_from_mysql(**_context: Any) -> None:
    """Sync buildings and devices from MySQL master data into warehouse dimensions."""
    mconn = _mysql_conn()
    mcur = mconn.cursor(dictionary=True)
    mcur.execute("SELECT building_id, building_name, faculty, floor_count FROM buildings")
    buildings = mcur.fetchall()
    mcur.execute("SELECT device_id, device_type, building_id, max_bandwidth_mbps FROM devices")
    devices = mcur.fetchall()
    mcur.close()
    mconn.close()

    with _pg_conn() as pconn, pconn.cursor() as pcur:
        execute_values(
            pcur,
            """
            INSERT INTO warehouse.dim_building (building_id, building_name, faculty, floor_count)
            VALUES %s
            ON CONFLICT (building_id) DO UPDATE SET
                building_name = EXCLUDED.building_name,
                faculty = EXCLUDED.faculty,
                floor_count = EXCLUDED.floor_count;
            """,
            [(b["building_id"], b["building_name"], b["faculty"], b["floor_count"]) for b in buildings],
        )
        execute_values(
            pcur,
            """
            INSERT INTO warehouse.dim_device (device_id, device_type, building_id, max_bandwidth_mbps)
            VALUES %s
            ON CONFLICT (device_id) DO UPDATE SET
                device_type = EXCLUDED.device_type,
                building_id = EXCLUDED.building_id,
                max_bandwidth_mbps = EXCLUDED.max_bandwidth_mbps;
            """,
            [(d["device_id"], d["device_type"], d["building_id"], d["max_bandwidth_mbps"]) for d in devices],
        )
        pconn.commit()


def run_batch_etl_and_dq(**_context: Any) -> None:
    """Extract raw MongoDB telemetry, run DQ + reconciliation, and load warehouse facts."""
    mongo_uri = f"mongodb://{os.getenv('MONGO_HOST', 'mongodb')}:{os.getenv('MONGO_PORT', '27017')}/"
    mongo_db = os.getenv("MONGO_DB", "campus_raw")
    mongo_col = os.getenv("MONGO_COLLECTION", "raw_telemetry")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    raw_docs = list(client[mongo_db][mongo_col].find({}, {"_id": 0}))
    if not raw_docs:
        logger.info("No raw telemetry found in MongoDB.")
        return

    cleaned_df, dq_counts, hb_alerts = clean_and_audit_telemetry(raw_docs)

    # Reconcile against Injection Ledger
    ledger_path = Path(os.getenv("SIM_LEDGER_PATH", "/shared/ledger/injection_ledger.jsonl"))
    ledger_entries: List[Dict[str, Any]] = []
    if ledger_path.exists():
        with ledger_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    ledger_entries.append(json.loads(line))
    dq_df = reconcile_with_ledger(ledger_entries, dq_counts)

    # Build dim_time rows
    cleaned_df["hour_utc_ts"] = cleaned_df["ts_utc"].dt.floor("h")
    unique_hours = cleaned_df["hour_utc_ts"].drop_duplicates().sort_values()
    time_rows = []
    for ts in unique_hours:
        wib = ts + pd.Timedelta(hours=7)
        time_key = int(ts.strftime("%Y%m%d%H"))
        time_rows.append(
            (
                time_key,
                ts.to_pydatetime(),
                ts.date(),
                wib.date(),
                int(ts.hour),
                int(wib.hour),
                int(wib.dayofweek),
                bool(8 <= wib.hour <= 17 and wib.dayofweek < 5),
            )
        )

    cleaned_df["time_key"] = cleaned_df["hour_utc_ts"].dt.strftime("%Y%m%d%H").astype(int)

    # Hourly device metrics
    hourly = (
        cleaned_df.groupby(["time_key", "device_id", "building_id"], as_index=False)
        .agg(
            avg_port_in_mbps=("port_in_mbps", "mean"),
            max_port_in_mbps=("port_in_mbps", "max"),
            avg_port_out_mbps=("port_out_mbps", "mean"),
            total_errors_delta=("errors_delta", "sum"),
            avg_uptime_s=("uptime_s", "mean"),
            avg_clients=("clients", "mean"),
            max_clients=("clients", "max"),
            avg_rssi_dbm=("rssi_avg_dbm", "mean"),
            avg_channel_util_pct=("channel_util_pct", "mean"),
            avg_latency_ms=("latency_ms", "mean"),
            p95_latency_ms=("latency_ms", lambda s: float(np.nanpercentile(s.dropna(), 95)) if s.notna().any() else np.nan),
            avg_packet_loss_pct=("packet_loss_pct", "mean"),
            sample_count=("ts_utc", "count"),
        )
    )

    sla_df = compute_sla_windows(cleaned_df)

    with _pg_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO warehouse.dim_time (
                time_key, full_timestamp_utc, date_utc, date_wib,
                hour_utc, hour_wib, day_of_week, is_lecture_hour
            ) VALUES %s
            ON CONFLICT (time_key) DO NOTHING;
            """,
            time_rows,
        )

        cur.execute("SELECT building_id, building_key FROM warehouse.dim_building;")
        b_map = dict(cur.fetchall())
        cur.execute("SELECT device_id, device_key FROM warehouse.dim_device;")
        d_map = dict(cur.fetchall())
        cur.execute("SELECT alert_code, alert_type_key FROM warehouse.dim_alert_type;")
        a_map = dict(cur.fetchall())

        # Upsert dq_reconciliation
        batch_window = f"{unique_hours.min().strftime('%Y-%m-%dT%H')}..{unique_hours.max().strftime('%Y-%m-%dT%H')}"
        execute_values(
            cur,
            """
            INSERT INTO warehouse.dq_reconciliation (
                batch_window, anomaly_type, disisipkan_count, tertangkap_count, ditangani_count, capture_rate_pct
            ) VALUES %s
            ON CONFLICT (batch_window, anomaly_type) DO UPDATE SET
                run_timestamp_utc = NOW(),
                disisipkan_count = EXCLUDED.disisipkan_count,
                tertangkap_count = EXCLUDED.tertangkap_count,
                ditangani_count = EXCLUDED.ditangani_count,
                capture_rate_pct = EXCLUDED.capture_rate_pct;
            """,
            [
                (
                    batch_window,
                    r["anomaly_type"],
                    int(r["disisipkan_count"]),
                    int(r["tertangkap_count"]),
                    int(r["ditangani_count"]),
                    float(r["capture_rate_pct"]),
                )
                for _, r in dq_df.iterrows()
            ],
        )

        # Upsert fact_device_metric_hourly
        def _flt(v: Any) -> Any:
            return None if pd.isna(v) else float(v)

        def _int_or_none(v: Any) -> Any:
            return None if pd.isna(v) else int(v)

        fact_hourly_rows = []
        for _, r in hourly.iterrows():
            b_key = b_map.get(r["building_id"])
            d_key = d_map.get(r["device_id"])
            if b_key is None or d_key is None:
                continue
            fact_hourly_rows.append(
                (
                    int(r["time_key"]),
                    d_key,
                    b_key,
                    _flt(r["avg_port_in_mbps"]),
                    _flt(r["max_port_in_mbps"]),
                    _flt(r["avg_port_out_mbps"]),
                    int(r["total_errors_delta"]),
                    _flt(r["avg_uptime_s"]),
                    _flt(r["avg_clients"]),
                    _int_or_none(r["max_clients"]),
                    _flt(r["avg_rssi_dbm"]),
                    _flt(r["avg_channel_util_pct"]),
                    _flt(r["avg_latency_ms"]),
                    _flt(r["p95_latency_ms"]),
                    _flt(r["avg_packet_loss_pct"]),
                    int(r["sample_count"]),
                )
            )

        execute_values(
            cur,
            """
            INSERT INTO warehouse.fact_device_metric_hourly (
                time_key, device_key, building_key,
                avg_port_in_mbps, max_port_in_mbps, avg_port_out_mbps,
                total_errors_delta, avg_uptime_s, avg_clients, max_clients,
                avg_rssi_dbm, avg_channel_util_pct, avg_latency_ms,
                p95_latency_ms, avg_packet_loss_pct, sample_count
            ) VALUES %s
            ON CONFLICT (time_key, device_key) DO UPDATE SET
                avg_port_in_mbps = EXCLUDED.avg_port_in_mbps,
                max_port_in_mbps = EXCLUDED.max_port_in_mbps,
                avg_port_out_mbps = EXCLUDED.avg_port_out_mbps,
                total_errors_delta = EXCLUDED.total_errors_delta,
                avg_clients = EXCLUDED.avg_clients,
                max_clients = EXCLUDED.max_clients,
                avg_latency_ms = EXCLUDED.avg_latency_ms,
                p95_latency_ms = EXCLUDED.p95_latency_ms,
                avg_packet_loss_pct = EXCLUDED.avg_packet_loss_pct,
                sample_count = EXCLUDED.sample_count;
            """,
            fact_hourly_rows,
        )

        # Upsert fact_sla_window
        sla_rows = []
        alert_rows = []
        for _, r in sla_df.iterrows():
            b_key = b_map.get(r["building_id"])
            if b_key is None:
                continue
            sla_rows.append(
                (
                    r["window_start_utc"].to_pydatetime(),
                    r["date_wib"],
                    b_key,
                    _flt(r["p95_latency_ms"]),
                    _flt(r["max_packet_loss_pct"]),
                    _flt(r["avg_channel_util_pct"]),
                    bool(r["is_sla_compliant"]),
                    r["violation_reason"],
                )
            )
            if not bool(r["is_sla_compliant"]):
                probe_id = f"PRB-{r['building_id'].split('-')[1]}-01"
                d_key = d_map.get(probe_id)
                t_key = int(r["window_start_utc"].strftime("%Y%m%d%H"))
                if d_key is not None:
                    if not pd.isna(r["p95_latency_ms"]) and r["p95_latency_ms"] > 100.0:
                        alert_rows.append(
                            (
                                r["window_start_utc"].to_pydatetime(),
                                t_key,
                                b_key,
                                d_key,
                                a_map["SLA_LATENCY_P95"],
                                float(r["p95_latency_ms"]),
                                100.0,
                                f"Pelanggaran SLA latensi p95 di {r['building_id']}: {r['p95_latency_ms']:.1f} ms",
                            )
                        )
                    if not pd.isna(r["max_packet_loss_pct"]) and r["max_packet_loss_pct"] > 2.0:
                        alert_rows.append(
                            (
                                r["window_start_utc"].to_pydatetime(),
                                t_key,
                                b_key,
                                d_key,
                                a_map["SLA_PACKET_LOSS"],
                                float(r["max_packet_loss_pct"]),
                                2.0,
                                f"Pelanggaran SLA packet loss di {r['building_id']}: {r['max_packet_loss_pct']:.2f}%",
                            )
                        )

        execute_values(
            cur,
            """
            INSERT INTO warehouse.fact_sla_window (
                window_start_utc, date_wib, building_key,
                p95_latency_ms, max_packet_loss_pct, avg_channel_util_pct,
                is_sla_compliant, violation_reason
            ) VALUES %s
            ON CONFLICT (window_start_utc, building_key) DO UPDATE SET
                p95_latency_ms = EXCLUDED.p95_latency_ms,
                max_packet_loss_pct = EXCLUDED.max_packet_loss_pct,
                is_sla_compliant = EXCLUDED.is_sla_compliant,
                violation_reason = EXCLUDED.violation_reason;
            """,
            sla_rows,
        )

        # Add heartbeat missing alerts
        for hba in hb_alerts:
            b_key = b_map.get(hba["building_id"])
            d_key = d_map.get(hba["device_id"])
            if b_key and d_key:
                t_key = int(hba["alert_time_utc"].strftime("%Y%m%d%H"))
                alert_rows.append(
                    (
                        hba["alert_time_utc"].to_pydatetime(),
                        t_key,
                        b_key,
                        d_key,
                        a_map["HEARTBEAT_MISSING"],
                        hba["metric_value"],
                        hba["threshold_value"],
                        hba["message"],
                    )
                )

        if alert_rows:
            execute_values(
                cur,
                """
                INSERT INTO warehouse.fact_alert (
                    alert_time_utc, time_key, building_key, device_key,
                    alert_type_key, metric_value, threshold_value, message
                ) VALUES %s
                ON CONFLICT (alert_time_utc, device_key, alert_type_key) DO NOTHING;
                """,
                alert_rows,
            )

        conn.commit()
    logger.info(
        "Loaded %d hourly facts, %d SLA windows, %d alerts.",
        len(fact_hourly_rows),
        len(sla_rows),
        len(alert_rows),
    )


default_args = {
    "owner": "pid-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="campus_telemetry_etl",
    default_args=default_args,
    description="Batch ETL Pandas + Data Quality + Star Schema Loader (Varian B)",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["pid", "campus-telemetry", "etl"],
) as dag:
    t_sync_dims = PythonOperator(
        task_id="sync_dimensions_from_mysql",
        python_callable=sync_dimensions_from_mysql,
    )
    t_etl_dq = PythonOperator(
        task_id="run_batch_etl_and_dq",
        python_callable=run_batch_etl_and_dq,
    )
    t_sync_dims >> t_etl_dq
