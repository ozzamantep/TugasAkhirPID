import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import paho.mqtt.client as mqtt
from pymongo import MongoClient

from src.simulator.generator import TelemetrySimulator, WIB_OFFSET


def _append_ledger(ledger_path: Path, entries: List[Dict[str, Any]]) -> None:
    if not entries:
        return
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


def _is_scheduled_flash_crowd(ts_utc: datetime, flag_path: Path) -> bool:
    """Trigger flash crowd during 10:00-10:15 WIB (KRS simulation) or when flag file exists."""
    if flag_path.exists():
        return True
    wib = ts_utc + WIB_OFFSET
    return wib.hour == 10 and 0 <= wib.minute < 15


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
        # Simulate KRS flash crowd on day 3 & day 6 at 10:00-10:25 WIB
        is_flash = (m // 1440 in (2, 5)) and (wib.hour == 10 and wib.minute < 25)
        events, ledger = sim.generate_tick(ts, flash_crowd=is_flash, mode="backfill")
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
    print(f"[stream] Publishing 24 devices every {interval_sec}s to {mqtt_host}:{mqtt_port}")

    try:
        while True:
            ts = datetime.now(timezone.utc)
            flash = _is_scheduled_flash_crowd(ts, flag_path)
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
