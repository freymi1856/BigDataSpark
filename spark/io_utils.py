from pyspark.sql import SparkSession

from settings import CLICKHOUSE_OPTIONS, POSTGRES_OPTIONS


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_postgres_table(spark: SparkSession, table_name: str):
    return spark.read.format("jdbc").options(**POSTGRES_OPTIONS).option("dbtable", table_name).load()


def write_postgres_table(df, table_name: str, mode: str = "overwrite") -> None:
    (
        df.write.format("jdbc")
        .options(**POSTGRES_OPTIONS)
        .option("dbtable", table_name)
        .mode(mode)
        .save()
    )


def write_clickhouse_table(df, table_name: str, order_by: str) -> None:
    (
        df.write.format("jdbc")
        .options(**CLICKHOUSE_OPTIONS)
        .option("dbtable", table_name)
        .option("createTableOptions", f"ENGINE = MergeTree ORDER BY ({order_by}) SETTINGS allow_nullable_key = 1")
        .mode("overwrite")
        .save()
    )
