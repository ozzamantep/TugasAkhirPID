FROM apache/spark:3.5.3-python3

USER root
RUN mkdir -p /opt/spark/extra-jars && \
    curl -fsSL -o /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL -o /opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL -o /opt/spark/extra-jars/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -fsSL -o /opt/spark/extra-jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o /opt/spark/extra-jars/lz4-java-1.8.0.jar \
      https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    curl -fsSL -o /opt/spark/extra-jars/snappy-java-1.1.10.5.jar \
      https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar

WORKDIR /app
COPY src/spark/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/__init__.py /app/src/__init__.py
COPY src/spark /app/src/spark

ENV PYTHONPATH=/app
CMD ["/opt/spark/bin/spark-submit", "--jars", "/opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/extra-jars/kafka-clients-3.5.1.jar,/opt/spark/extra-jars/commons-pool2-2.11.1.jar,/opt/spark/extra-jars/lz4-java-1.8.0.jar,/opt/spark/extra-jars/snappy-java-1.1.10.5.jar", "/app/src/spark/spark_streaming.py"]
