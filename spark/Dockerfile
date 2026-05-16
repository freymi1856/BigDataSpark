FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG CLICKHOUSE_JDBC_VERSION=0.4.6

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar /opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar

RUN chmod 644 /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar

ENV PATH="/opt/spark/bin:${PATH}"

USER spark
