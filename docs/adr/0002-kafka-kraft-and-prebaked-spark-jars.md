# Use Kafka KRaft Mode and Pre-Baked Spark Connector JARs

**Status:** accepted

The entire pipeline must start reliably on a lecturer's machine with a single `docker compose up` command and reasonable memory limits. We run Apache Kafka in KRaft mode (eliminating Zookeeper) and build a custom Spark image that pre-downloads the Kafka and PostgreSQL connector JARs during `docker build` rather than fetching them from Maven at container startup. This avoids startup race conditions, cuts container memory overhead, and prevents transient network failures during live demos.
