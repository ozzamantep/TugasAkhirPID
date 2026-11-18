import json
import os
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


REQUIRED_FIELDS = ("device_id", "device_type", "building_id", "timestamp", "data")


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                linger_ms=20,
            )
            return producer
        except Exception as exc:
            print(f"[bridge] Waiting for Kafka at {bootstrap_servers}: {exc}")
            time.sleep(3)


def main() -> None:
    mqtt_host = os.getenv("MQTT_HOST", "mosquitto")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_topic = os.getenv("MQTT_TOPIC", "campus/telemetry/#")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "campus-telemetry")

    producer = create_kafka_producer(kafka_bootstrap)

    def on_connect(client: mqtt.Client, _userdata: object, _flags: object, rc: int, _props: object = None) -> None:
        if rc == 0:
            print(f"[bridge] Connected to MQTT {mqtt_host}:{mqtt_port}, subscribing to {mqtt_topic}")
            client.subscribe(mqtt_topic)
        else:
            print(f"[bridge] MQTT connection failed with code {rc}")

    def on_message(_client: mqtt.Client, _userdata: object, msg: mqtt.MQTTMessage) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return
        if not all(field in payload for field in REQUIRED_FIELDS):
            return
        producer.send(kafka_topic, key=str(payload["device_id"]), value=payload)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge")
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(mqtt_host, mqtt_port, keepalive=60)
            break
        except Exception as exc:
            print(f"[bridge] Waiting for MQTT {mqtt_host}:{mqtt_port}: {exc}")
            time.sleep(2)

    client.loop_forever()


if __name__ == "__main__":
    main()
