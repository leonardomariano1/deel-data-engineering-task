# One image for master, workers and the driver (spark-submit in client mode)
FROM apache/spark:3.5.1-python3

USER root

ARG MAVEN=https://repo1.maven.org/maven2
# Kafka source + Postgres JDBC baked in: no --packages resolution at runtime
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

RUN pip install --no-cache-dir psycopg2-binary==2.9.9 pytest==8.3.3

COPY conf/ /opt/spark/conf/
COPY app/ /opt/app/
COPY tests/ /opt/app/tests/
RUN mkdir -p /checkpoints /opt/spark/work /opt/spark/logs \
    && chown -R spark:spark /checkpoints /opt/spark/work /opt/spark/logs /opt/app

ENV PYTHONPATH=/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    SPARK_NO_DAEMONIZE=true
WORKDIR /opt/app
USER spark
