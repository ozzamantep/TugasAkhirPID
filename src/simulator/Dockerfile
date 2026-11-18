FROM python:3.11-slim

WORKDIR /app
COPY src/simulator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/__init__.py /app/src/__init__.py
COPY src/simulator /app/src/simulator

ENV PYTHONPATH=/app
CMD ["python", "-m", "src.simulator.main", "--mode", "both"]
