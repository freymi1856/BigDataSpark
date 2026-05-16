import os


CSV_INPUT_PATH = os.getenv("CSV_INPUT_PATH", "/data/MOCK_DATA*.csv")

POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/labdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "lab")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lab")

CLICKHOUSE_JDBC_URL = os.getenv("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "lab")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lab")

POSTGRES_OPTIONS = {
    "url": POSTGRES_JDBC_URL,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_OPTIONS = {
    "url": CLICKHOUSE_JDBC_URL,
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}
