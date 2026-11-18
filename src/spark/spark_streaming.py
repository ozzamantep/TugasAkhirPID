import os
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict, Tuple

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from src.spark.streaming_logic import detect_moving_avg_anomaly

TELEMETRY_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("building_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField(
            "data",
            StructType(
                [
                    StructField("port_in_mbps", DoubleType(), True),
                    StructField("port_out_mbps", DoubleType(), True),
                    StructField("errors", LongType(), True),
                    StructField("uptime_s", LongType(), True),
                    StructField("clients", IntegerType(), True),
                    StructField("rssi_avg_dbm", DoubleType(), True),
                    StructField("channel_util_pct", DoubleType(), True),
                    StructField("latency_ms", DoubleType(), True),
                    StructField("packet_loss_pct", DoubleType(), True),
                    StructField("target", StringType(), True),
                ]
            ),
            False,
        ),
    ]
)

SWITCH_HISTORY: Dict[str, Deque[Tuple[datetime, float]]] = defaultdict(lambda: deque(maxlen=3600))


def get_pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "campus_telemetry"),
        user=os.getenv("POSTGRES_USER", "pid_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pid_secret_password"),
    )


def build_window_agg(df: DataFrame, window_duration: str) -> DataFrame:
    return (
        df.groupBy(F.window("event_time", window_duration), "building_id")
        .agg(
            F.avg("port_in_mbps").alias("avg_port_in_mbps"),
            F.max("port_in_mbps").alias("max_port_in_mbps"),
            F.avg("port_out_mbps").alias("avg_port_out_mbps"),
            F.max("port_out_mbps").alias("max_port_out_mbps"),
            F.coalesce(
                F.round(
                    F.avg("clients")
                    * F.approx_count_distinct(F.when(F.col("device_type") == "access_point", F.col("device_id"))),
                    0,
                ).cast("int"),
                F.lit(0),
            ).alias("total_wifi_clients"),
            F.avg("channel_util_pct").alias("avg_channel_util_pct"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
            F.avg("packet_loss_pct").alias("avg_packet_loss_pct"),
            F.max("packet_loss_pct").alias("max_packet_loss_pct"),
            F.count("*").alias("record_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "building_id",
            "avg_port_in_mbps",
            "max_port_in_mbps",
            "avg_port_out_mbps",
            "max_port_out_mbps",
            "total_wifi_clients",
            "avg_channel_util_pct",
            "avg_latency_ms",
            "p95_latency_ms",
            "avg_packet_loss_pct",
            "max_packet_loss_pct",
            "record_count",
        )
    )


def write_window_1m(batch_df: DataFrame, _batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    sql = """
        INSERT INTO streaming.window_metrics_1m (
            window_start, window_end, building_id,
            avg_port_in_mbps, max_port_in_mbps, avg_port_out_mbps, max_port_out_mbps,
            total_wifi_clients, avg_channel_util_pct,
            avg_latency_ms, p95_latency_ms, avg_packet_loss_pct, max_packet_loss_pct,
            record_count, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (window_start, building_id) DO UPDATE SET
            avg_port_in_mbps = EXCLUDED.avg_port_in_mbps,
            max_port_in_mbps = EXCLUDED.max_port_in_mbps,
            avg_port_out_mbps = EXCLUDED.avg_port_out_mbps,
            max_port_out_mbps = EXCLUDED.max_port_out_mbps,
            total_wifi_clients = EXCLUDED.total_wifi_clients,
            avg_channel_util_pct = EXCLUDED.avg_channel_util_pct,
            avg_latency_ms = EXCLUDED.avg_latency_ms,
            p95_latency_ms = EXCLUDED.p95_latency_ms,
            avg_packet_loss_pct = EXCLUDED.avg_packet_loss_pct,
            max_packet_loss_pct = EXCLUDED.max_packet_loss_pct,
            record_count = EXCLUDED.record_count,
            updated_at = NOW();
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        for r in rows:
            cur.execute(
                sql,
                (
                    r.window_start, r.window_end, r.building_id,
                    r.avg_port_in_mbps, r.max_port_in_mbps, r.avg_port_out_mbps, r.max_port_out_mbps,
                    r.total_wifi_clients, r.avg_channel_util_pct,
                    r.avg_latency_ms, r.p95_latency_ms, r.avg_packet_loss_pct, r.max_packet_loss_pct,
                    r.record_count,
                ),
            )
        conn.commit()


def write_window_5m_and_sla(batch_df: DataFrame, _batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    win_sql = """
        INSERT INTO streaming.window_metrics_5m (
            window_start, window_end, building_id,
            avg_port_in_mbps, max_port_in_mbps, avg_port_out_mbps, max_port_out_mbps,
            total_wifi_clients, avg_channel_util_pct,
            avg_latency_ms, p95_latency_ms, avg_packet_loss_pct, max_packet_loss_pct,
            sla_compliant, record_count, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (window_start, building_id) DO UPDATE SET
            avg_port_in_mbps = EXCLUDED.avg_port_in_mbps,
            max_port_in_mbps = EXCLUDED.max_port_in_mbps,
            avg_port_out_mbps = EXCLUDED.avg_port_out_mbps,
            max_port_out_mbps = EXCLUDED.max_port_out_mbps,
            total_wifi_clients = EXCLUDED.total_wifi_clients,
            avg_channel_util_pct = EXCLUDED.avg_channel_util_pct,
            avg_latency_ms = EXCLUDED.avg_latency_ms,
            p95_latency_ms = EXCLUDED.p95_latency_ms,
            avg_packet_loss_pct = EXCLUDED.avg_packet_loss_pct,
            max_packet_loss_pct = EXCLUDED.max_packet_loss_pct,
            sla_compliant = EXCLUDED.sla_compliant,
            record_count = EXCLUDED.record_count,
            updated_at = NOW();
    """
    alert_sql = """
        INSERT INTO streaming.realtime_alerts (
            alert_time, building_id, device_id, alert_type, severity,
            metric_value, threshold_value, message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (alert_time, device_id, alert_type) DO NOTHING;
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        for r in rows:
            p95_ok = r.p95_latency_ms is None or r.p95_latency_ms <= 100.0
            loss_ok = r.max_packet_loss_pct is None or r.max_packet_loss_pct <= 2.0
            compliant = p95_ok and loss_ok
            cur.execute(
                win_sql,
                (
                    r.window_start, r.window_end, r.building_id,
                    r.avg_port_in_mbps, r.max_port_in_mbps, r.avg_port_out_mbps, r.max_port_out_mbps,
                    r.total_wifi_clients, r.avg_channel_util_pct,
                    r.avg_latency_ms, r.p95_latency_ms, r.avg_packet_loss_pct, r.max_packet_loss_pct,
                    compliant, r.record_count,
                ),
            )
            probe_id = f"PRB-{r.building_id.split('-')[1]}-01"
            if not p95_ok:
                cur.execute(
                    alert_sql,
                    (
                        r.window_end, r.building_id, probe_id, "SLA_LATENCY_P95", "CRITICAL",
                        round(r.p95_latency_ms, 2), 100.0,
                        f"Pelanggaran SLA di {r.building_id}: latensi p95 {r.p95_latency_ms:.1f} ms > 100 ms",
                    ),
                )
            if not loss_ok:
                cur.execute(
                    alert_sql,
                    (
                        r.window_end, r.building_id, probe_id, "SLA_PACKET_LOSS", "CRITICAL",
                        round(r.max_packet_loss_pct, 2), 2.0,
                        f"Pelanggaran SLA di {r.building_id}: packet loss {r.max_packet_loss_pct:.2f}% > 2%",
                    ),
                )
        conn.commit()


def process_device_heartbeat_and_anomalies(batch_df: DataFrame, _batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    latest_devices = (
        batch_df.groupBy("device_id", "device_type", "building_id")
        .agg(F.max("event_time").alias("last_seen_utc"))
        .collect()
    )

    switch_points = (
        batch_df.filter((F.col("device_type") == "switch") & F.col("port_in_mbps").isNotNull())
        .select("device_id", "building_id", "event_time", "port_in_mbps")
        .collect()
    )

    with get_pg_conn() as conn, conn.cursor() as cur:
        for d in latest_devices:
            cur.execute(
                """
                INSERT INTO streaming.device_latest_state (device_id, device_type, building_id, last_seen_utc, status, updated_at)
                VALUES (%s, %s, %s, %s, 'ONLINE', NOW())
                ON CONFLICT (device_id) DO UPDATE SET
                    last_seen_utc = GREATEST(streaming.device_latest_state.last_seen_utc, EXCLUDED.last_seen_utc),
                    status = 'ONLINE',
                    updated_at = NOW();
                """,
                (d.device_id, d.device_type, d.building_id, d.last_seen_utc),
            )

        # Check devices silent for > 2 minutes
        cur.execute(
            """
            SELECT device_id, building_id, last_seen_utc,
                   EXTRACT(EPOCH FROM (NOW() - last_seen_utc)) AS gap_s
            FROM streaming.device_latest_state
            WHERE last_seen_utc < NOW() - INTERVAL '2 minutes' AND status = 'ONLINE';
            """
        )
        for device_id, building_id, last_seen, gap_s in cur.fetchall():
            now_utc = datetime.now(timezone.utc)
            cur.execute(
                "UPDATE streaming.device_latest_state SET status = 'OFFLINE', updated_at = NOW() WHERE device_id = %s;",
                (device_id,),
            )
            cur.execute(
                """
                INSERT INTO streaming.realtime_alerts (
                    alert_time, building_id, device_id, alert_type, severity,
                    metric_value, threshold_value, message
                ) VALUES (%s, %s, %s, 'HEARTBEAT_MISSING', 'WARNING', %s, 120.0, %s)
                ON CONFLICT (alert_time, device_id, alert_type) DO NOTHING;
                """,
                (
                    now_utc, building_id, device_id, round(float(gap_s), 1),
                    f"Perangkat {device_id} di {building_id} tanpa heartbeat selama {int(gap_s)} detik (terakhir {last_seen})",
                ),
            )

        # 1-hour moving-average band anomaly detection
        for pt in switch_points:
            ev_time = pt.event_time
            if ev_time.tzinfo is None:
                ev_time = ev_time.replace(tzinfo=timezone.utc)
            hist = SWITCH_HISTORY[pt.device_id]
            cutoff = ev_time.timestamp() - 3600.0
            while hist and hist[0][0].timestamp() < cutoff:
                hist.popleft()

            values = [v for _, v in hist]
            anomaly = detect_moving_avg_anomaly(float(pt.port_in_mbps), values)
            hist.append((ev_time, float(pt.port_in_mbps)))

            if anomaly is not None:
                cur.execute(
                    """
                    INSERT INTO streaming.traffic_anomalies (
                        detected_at, building_id, device_id, port_in_mbps,
                        moving_avg_1h, moving_std_1h, upper_band, lower_band, z_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (detected_at, device_id) DO NOTHING;
                    """,
                    (
                        ev_time, pt.building_id, pt.device_id,
                        anomaly["port_in_mbps"], anomaly["moving_avg_1h"],
                        anomaly["moving_std_1h"], anomaly["upper_band"],
                        anomaly["lower_band"], anomaly["z_score"],
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO streaming.realtime_alerts (
                        alert_time, building_id, device_id, alert_type, severity,
                        metric_value, threshold_value, message
                    ) VALUES (%s, %s, %s, 'TRAFFIC_ANOMALY', 'WARNING', %s, %s, %s)
                    ON CONFLICT (alert_time, device_id, alert_type) DO NOTHING;
                    """,
                    (
                        ev_time, pt.building_id, pt.device_id,
                        anomaly["port_in_mbps"], anomaly["upper_band"],
                        f"Anomali trafik pada {pt.device_id} ({pt.building_id}): {anomaly['port_in_mbps']} Mbps di luar pita [{anomaly['lower_band']}, {anomaly['upper_band']}]",
                    ),
                )

        conn.commit()


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "campus-telemetry")

    spark = (
        SparkSession.builder.appName("CampusNetworkTelemetryStreaming")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream.select(F.from_json(F.col("value").cast("string"), TELEMETRY_SCHEMA).alias("j"))
        .select("j.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull() & F.col("device_id").isNotNull())
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["device_id", "timestamp"])
    )

    cleaned = (
        parsed.withColumn(
            "port_in_mbps",
            F.when(F.col("data.port_in_mbps") > 10000.0, F.col("data.port_in_mbps") / 1_000_000.0)
            .otherwise(F.col("data.port_in_mbps")),
        )
        .withColumn(
            "port_out_mbps",
            F.when(F.col("data.port_out_mbps") > 10000.0, F.col("data.port_out_mbps") / 1_000_000.0)
            .otherwise(F.col("data.port_out_mbps")),
        )
        .withColumn("clients", F.when(F.col("data.clients") >= 0, F.col("data.clients")))
        .withColumn(
            "channel_util_pct",
            F.when((F.col("data.channel_util_pct") >= 0) & (F.col("data.channel_util_pct") <= 100), F.col("data.channel_util_pct")),
        )
        .withColumn("latency_ms", F.when(F.col("data.latency_ms") >= 0, F.col("data.latency_ms")))
        .withColumn(
            "packet_loss_pct",
            F.when((F.col("data.packet_loss_pct") >= 0) & (F.col("data.packet_loss_pct") <= 100), F.col("data.packet_loss_pct")),
        )
    )

    q_1m = (
        build_window_agg(cleaned, "1 minute")
        .writeStream.outputMode("update")
        .foreachBatch(write_window_1m)
        .option("checkpointLocation", "/tmp/spark-checkpoints/win1m")
        .trigger(processingTime="15 seconds")
        .start()
    )

    q_5m = (
        build_window_agg(cleaned, "5 minutes")
        .writeStream.outputMode("update")
        .foreachBatch(write_window_5m_and_sla)
        .option("checkpointLocation", "/tmp/spark-checkpoints/win5m")
        .trigger(processingTime="20 seconds")
        .start()
    )

    q_hb_anom = (
        cleaned.writeStream.outputMode("append")
        .foreachBatch(process_device_heartbeat_and_anomalies)
        .option("checkpointLocation", "/tmp/spark-checkpoints/hb_anom")
        .trigger(processingTime="15 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()
    q_1m.stop()
    q_5m.stop()
    q_hb_anom.stop()


if __name__ == "__main__":
    main()
