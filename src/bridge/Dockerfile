FROM python:3.11-slim

WORKDIR /app
COPY src/bridge/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/bridge /app/src/bridge
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.bridge.mqtt_to_kafka"]
