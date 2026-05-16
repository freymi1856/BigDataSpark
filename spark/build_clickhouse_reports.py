from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    corr,
    count,
    countDistinct,
    dense_rank,
    lag,
    lit,
    rank,
    round as spark_round,
    sum as spark_sum,
    when,
)

from io_utils import read_postgres_table, spark_session, write_clickhouse_table


def sales_mart(spark):
    fact = read_postgres_table(spark, "dwh.fact_sales").alias("f")
    products = read_postgres_table(spark, "dwh.dim_product").alias("p")
    customers = read_postgres_table(spark, "dwh.dim_customer").alias("c")
    stores = read_postgres_table(spark, "dwh.dim_store").alias("s")
    suppliers = read_postgres_table(spark, "dwh.dim_supplier").alias("sup")
    dates = read_postgres_table(spark, "dwh.dim_date").alias("d")

    return (
        fact.join(products, "product_key")
        .join(customers, "customer_key")
        .join(stores, "store_key")
        .join(suppliers, "supplier_key")
        .join(dates, "date_key")
        .select(
            "sale_key",
            "date_key",
            "product_key",
            "customer_key",
            "store_key",
            "supplier_key",
            "sale_quantity",
            "sale_total_price",
            "product_name",
            "product_category",
            "product_brand",
            "product_price",
            "product_rating",
            "product_reviews",
            "customer_full_name",
            "customer_email",
            "customer_country",
            "store_name",
            "store_city",
            "store_country",
            "supplier_name",
            "supplier_country",
            "year",
            "month",
            "month_name",
        )
        .cache()
    )


def product_report(mart):
    by_product = mart.groupBy("product_key", "product_name", "product_category", "product_brand").agg(
        count("*").alias("orders_count"),
        spark_sum("sale_quantity").alias("sold_quantity"),
        spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
        spark_round(avg("product_rating"), 2).alias("avg_rating"),
        spark_round(avg("product_reviews"), 2).alias("avg_reviews"),
    )
    category_window = Window.partitionBy("product_category")
    rank_window = Window.orderBy(col("sold_quantity").desc(), col("total_revenue").desc())

    return (
        by_product.withColumn("category_total_revenue", spark_round(spark_sum("total_revenue").over(category_window), 2))
        .withColumn("product_sales_rank", rank().over(rank_window))
        .withColumn("is_top_10_by_quantity", col("product_sales_rank") <= 10)
    )


def customer_report(mart):
    by_customer = mart.groupBy(
        "customer_key", "customer_full_name", "customer_email", "customer_country"
    ).agg(
        count("*").alias("orders_count"),
        countDistinct("product_key").alias("unique_products_count"),
        spark_sum("sale_quantity").alias("purchased_items"),
        spark_round(spark_sum("sale_total_price"), 2).alias("total_purchase_amount"),
    )
    country_window = Window.partitionBy("customer_country")
    rank_window = Window.orderBy(col("total_purchase_amount").desc(), col("orders_count").desc())

    return (
        by_customer.withColumn(
            "avg_check",
            spark_round(col("total_purchase_amount") / when(col("orders_count") == 0, 1).otherwise(col("orders_count")), 2),
        )
        .withColumn("customers_in_country", count("*").over(country_window))
        .withColumn("customer_spend_rank", rank().over(rank_window))
        .withColumn("is_top_10_by_amount", col("customer_spend_rank") <= 10)
    )


def time_report(mart):
    monthly = mart.groupBy("year", "month", "month_name").agg(
        count("*").alias("orders_count"),
        spark_sum("sale_quantity").alias("sold_quantity"),
        spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
    )
    yearly_window = Window.partitionBy("year")
    chronological_window = Window.orderBy("year", "month")

    return (
        monthly.withColumn(
            "avg_order_amount",
            spark_round(col("total_revenue") / when(col("orders_count") == 0, 1).otherwise(col("orders_count")), 2),
        )
        .withColumn("year_total_revenue", spark_round(spark_sum("total_revenue").over(yearly_window), 2))
        .withColumn("previous_month_revenue", coalesce(lag("total_revenue").over(chronological_window), lit(0)))
        .withColumn("month_revenue_delta", spark_round(col("total_revenue") - col("previous_month_revenue"), 2))
    )


def store_report(mart):
    by_store = mart.groupBy("store_key", "store_name", "store_city", "store_country").agg(
        count("*").alias("orders_count"),
        spark_sum("sale_quantity").alias("sold_quantity"),
        spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
    )
    geo_window = Window.partitionBy("store_country", "store_city")
    rank_window = Window.orderBy(col("total_revenue").desc(), col("orders_count").desc())

    return (
        by_store.withColumn(
            "avg_check",
            spark_round(col("total_revenue") / when(col("orders_count") == 0, 1).otherwise(col("orders_count")), 2),
        )
        .withColumn("city_total_revenue", spark_round(spark_sum("total_revenue").over(geo_window), 2))
        .withColumn("store_revenue_rank", rank().over(rank_window))
        .withColumn("is_top_5_by_revenue", col("store_revenue_rank") <= 5)
    )


def supplier_report(mart):
    by_supplier = mart.groupBy("supplier_key", "supplier_name", "supplier_country").agg(
        count("*").alias("orders_count"),
        countDistinct("product_key").alias("products_count"),
        spark_round(avg("product_price"), 2).alias("avg_product_price"),
        spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
    )
    country_window = Window.partitionBy("supplier_country")
    rank_window = Window.orderBy(col("total_revenue").desc(), col("orders_count").desc())

    return (
        by_supplier.withColumn("supplier_country_total_revenue", spark_round(spark_sum("total_revenue").over(country_window), 2))
        .withColumn("supplier_revenue_rank", rank().over(rank_window))
        .withColumn("is_top_5_by_revenue", col("supplier_revenue_rank") <= 5)
    )


def quality_report(mart):
    by_product = mart.groupBy("product_key", "product_name", "product_category", "product_brand").agg(
        spark_round(avg("product_rating"), 2).alias("avg_rating"),
        spark_round(avg("product_reviews"), 2).alias("avg_reviews"),
        spark_sum("sale_quantity").alias("sold_quantity"),
        spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
    )
    correlation = by_product.agg(
        spark_round(corr("avg_rating", "sold_quantity"), 4).alias("rating_sales_correlation")
    )

    high_rating_window = Window.orderBy(col("avg_rating").desc(), col("avg_reviews").desc())
    low_rating_window = Window.orderBy(col("avg_rating").asc(), col("avg_reviews").desc())
    review_window = Window.orderBy(col("avg_reviews").desc(), col("sold_quantity").desc())

    return (
        by_product.crossJoin(correlation)
        .withColumn("high_rating_rank", dense_rank().over(high_rating_window))
        .withColumn("low_rating_rank", dense_rank().over(low_rating_window))
        .withColumn("review_count_rank", dense_rank().over(review_window))
        .withColumn(
            "rating_group",
            when(col("avg_rating") >= 4.5, "excellent")
            .when(col("avg_rating") >= 3.5, "good")
            .when(col("avg_rating") >= 2.5, "average")
            .otherwise("low"),
        )
    )


def main() -> None:
    spark = spark_session("postgres-star-to-clickhouse-reports")
    mart = sales_mart(spark)

    reports = {
        "product_sales_report": (product_report(mart), "product_sales_rank, product_key"),
        "customer_sales_report": (customer_report(mart), "customer_spend_rank, customer_key"),
        "time_sales_report": (time_report(mart), "year, month"),
        "store_sales_report": (store_report(mart), "store_revenue_rank, store_key"),
        "supplier_sales_report": (supplier_report(mart), "supplier_revenue_rank, supplier_key"),
        "product_quality_report": (quality_report(mart), "high_rating_rank, product_key"),
    }

    for table_name, (df, order_by) in reports.items():
        write_clickhouse_table(df, table_name, order_by)
        print(f"Saved {df.count()} rows into ClickHouse table {table_name}")

    mart.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
