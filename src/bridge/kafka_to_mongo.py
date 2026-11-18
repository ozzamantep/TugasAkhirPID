import json
import os
import time
from typing import Any, Dict, List

from kafka import KafkaConsumer
from pymongo import MongoClient


def create_kafka_consumer(bootstrap_servers: str, topic: str) -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers.split(","),
                group_id="mongo-raw-sink",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=2000,
            )
        except Exception as exc:
            print(f"[mongo-consumer] Waiting for Kafka {bootstrap_servers}: {exc}")
            time.sleep(3)


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "campus-telemetry")
    mongo_uri = f"mongodb://{os.getenv('MONGO_HOST', 'mongodb')}:{os.getenv('MONGO_PORT', '27017')}/"
    mongo_db = os.getenv("MONGO_DB", "campus_raw")
    mongo_col = os.getenv("MONGO_COLLECTION", "raw_telemetry")

    mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    collection = mongo_client[mongo_db][mongo_col]
    collection.create_index("timestamp")
    collection.create_index([("device_id", 1), ("timestamp", 1)])

    consumer = create_kafka_consumer(kafka_bootstrap, kafka_topic)
    print(f"[mongo-consumer] Listening on {kafka_topic} -> {mongo_db}.{mongo_col}")

    batch: List[Dict[str, Any]] = []
    last_flush = time.monotonic()

    while True:
        for msg in consumer:
            doc = msg.value
            if isinstance(doc, dict):
                doc["source"] = "stream"
                batch.append(doc)
            if len(batch) >= 100 or (batch and time.monotonic() - last_flush >= 2.0):
                collection.insert_many(batch)
                batch.clear()
                last_flush = time.monotonic()

        if batch:
            collection.insert_many(batch)
            batch.clear()
            last_flush = time.monotonic()


if __name__ == "__main__":
    main()
