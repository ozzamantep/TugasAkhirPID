FROM apache/airflow:2.9.3-python3.11

COPY src/dags/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root src/__init__.py /opt/airflow/src/__init__.py
COPY --chown=airflow:root src/dags /opt/airflow/dags
COPY --chown=airflow:root src/dags /opt/airflow/src/dags

ENV PYTHONPATH=/opt/airflow
