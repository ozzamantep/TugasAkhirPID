import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import paho.mqtt.client as mqtt
from pymongo import MongoClient

from src.simulator.generator import TelemetrySimulator, WIB_OFFSET, is_flash_crowd_active


def _append_ledger(ledger_path: Path, entries: List[Dict[str, Any]]) -> None:
    if not entries:
        return
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


def run_backfill(
    days: int,
    seed: int,
    dirty_rate: float,
    ledger_path: Path,
) -> None:
    """Populate 7 days of 1-minute resolution historical telemetry into MongoDB."""
    mongo_uri = f"mongodb://{os.getenv('MONGO_HOST', 'mongodb')}:{os.getenv('MONGO_PORT', '27017')}/"
    mongo_db = os.getenv("MONGO_DB", "campus_raw")
    mongo_col = os.getenv("MONGO_COLLECTION", "raw_telemetry")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    collection = client[mongo_db][mongo_col]
    collection.create_index("timestamp")
    collection.create_index([("device_id", 1), ("timestamp", 1)])

    existing = collection.count_documents({"source": "backfill"})
    if existing > 0:
        print(f"[backfill] Found {existing} existing backfill docs; skipping duplicate backfill.")
        return

    sim = TelemetrySimulator(seed=seed, dirty_rate=dirty_rate)
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_utc = now_utc - timedelta(days=days)
    total_minutes = days * 24 * 60

    batch: List[Dict[str, Any]] = []
    ledger_batch: List[Dict[str, Any]] = []
    inserted = 0

    for m in range(total_minutes):
        ts = start_utc + timedelta(minutes=m)
        wib = ts + WIB_OFFSET
        day_idx = m // 1440
        # Campus-wide KRS flash crowd on day 2 & day 5 (10:00-10:25 WIB)
        is_flash = (day_idx in (2, 5)) and (wib.hour == 10 and wib.minute < 25)
        # Building-specific localized congestion so rankings & daily SLA trends differ realistically
        stressed: List[str] = []
        if day_idx in (1, 3, 4) and wib.hour == 13 and wib.minute < 35:
            stressed.append("BLD-FEB")
        if day_idx in (2, 4) and wib.hour == 14 and wib.minute < 20:
            stressed.append("BLD-RECT")
        if day_idx == 3 and wib.hour == 11 and wib.minute < 15:
            stressed.append("BLD-FT")
        events, ledger = sim.generate_tick(
            ts, flash_crowd=is_flash, mode="backfill", stressed_buildings=tuple(stressed)
        )
        for ev in events:
            ev["source"] = "backfill"
            batch.append(ev)
        ledger_batch.extend(ledger)

        if len(batch) >= 5000:
            collection.insert_many(batch)
            inserted += len(batch)
            batch.clear()

    if batch:
        collection.insert_many(batch)
        inserted += len(batch)

    _append_ledger(ledger_path, ledger_batch)
    print(f"[backfill] Inserted {inserted} docs and {len(ledger_batch)} ledger entries ({days} days).")


def run_stream(
    interval_sec: float,
    seed: int,
    dirty_rate: float,
    ledger_path: Path,
) -> None:
    """Continuously publish live telemetry to Mosquitto MQTT."""
    mqtt_host = os.getenv("MQTT_HOST", "mosquitto")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    flag_path = Path(os.getenv("FLASH_CROWD_FLAG", "/shared/ledger/flash_crowd.flag"))
    every_min = float(os.getenv("FLASH_CROWD_EVERY_MIN", "20"))
    duration_min = float(os.getenv("FLASH_CROWD_DURATION_MIN", "5"))

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="campus-telemetry-simulator")
    while True:
        try:
            client.connect(mqtt_host, mqtt_port, keepalive=60)
            break
        except Exception as exc:
            print(f"[stream] Waiting for MQTT {mqtt_host}:{mqtt_port} ({exc})...")
            time.sleep(2)

    client.loop_start()
    sim = TelemetrySimulator(seed=seed + 1000, dirty_rate=dirty_rate)
    start_mono = time.monotonic()
    print(
        f"[stream] Publishing 24 devices every {interval_sec}s to {mqtt_host}:{mqtt_port} "
        f"(flash_crowd: every={every_min}m, dur={duration_min}m)"
    )

    try:
        while True:
            ts = datetime.now(timezone.utc)
            elapsed_sec = time.monotonic() - start_mono
            flash = is_flash_crowd_active(elapsed_sec, ts, flag_path, every_min, duration_min)
            events, ledger = sim.generate_tick(ts, flash_crowd=flash, mode="stream")
            for ev in events:
                topic = f"campus/telemetry/{ev['building_id']}/{ev['device_id']}"
                client.publish(topic, json.dumps(ev), qos=0)
            _append_ledger(ledger_path, ledger)
            time.sleep(interval_sec)
    finally:
        client.loop_stop()
        client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description="Campus Network Telemetry Simulator")
    parser.add_argument("--mode", choices=["backfill", "stream", "both"], default="both")
    args = parser.parse_args()

    seed = int(os.getenv("SIM_SEED", "42"))
    dirty_rate = float(os.getenv("SIM_DIRTY_RATE", "0.05"))
    interval_sec = float(os.getenv("SIM_INTERVAL_SEC", "1.0"))
    backfill_days = int(os.getenv("SIM_BACKFILL_DAYS", "7"))
    ledger_path = Path(os.getenv("SIM_LEDGER_PATH", "/shared/ledger/injection_ledger.jsonl"))

    if args.mode in ("backfill", "both"):
        run_backfill(backfill_days, seed, dirty_rate, ledger_path)
    if args.mode in ("stream", "both"):
        run_stream(interval_sec, seed, dirty_rate, ledger_path)


if __name__ == "__main__":
    main()
