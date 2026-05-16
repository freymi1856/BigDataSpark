from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    dayofmonth,
    month,
    quarter,
    sha2,
    to_date,
    year,
)

from io_utils import read_postgres_table, spark_session, write_postgres_table


def entity_hash(*columns):
    return sha2(concat_ws("|", *[col(name).cast("string") for name in columns]), 256)


def main() -> None:
    spark = spark_session("postgres-raw-to-star-schema")
    raw = read_postgres_table(spark, "raw.mock_data").cache()

    with_keys = (
        raw.withColumn("sale_key", entity_hash("source_file", "id"))
        .withColumn("customer_key", entity_hash("source_file", "sale_customer_id"))
        .withColumn("seller_key", entity_hash("source_file", "sale_seller_id"))
        .withColumn("product_key", entity_hash("source_file", "sale_product_id"))
        .withColumn("store_key", entity_hash("source_file", "store_name", "store_email", "store_phone"))
        .withColumn("supplier_key", entity_hash("source_file", "supplier_name", "supplier_email", "supplier_phone"))
        .withColumn("date_key", date_format(col("sale_date"), "yyyyMMdd").cast("int"))
    )

    dim_customer = with_keys.select(
        "customer_key",
        col("sale_customer_id").alias("customer_source_id"),
        "customer_first_name",
        "customer_last_name",
        concat_ws(" ", "customer_first_name", "customer_last_name").alias("customer_full_name"),
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
        "source_file",
    ).dropDuplicates(["customer_key"])

    dim_seller = with_keys.select(
        "seller_key",
        col("sale_seller_id").alias("seller_source_id"),
        "seller_first_name",
        "seller_last_name",
        concat_ws(" ", "seller_first_name", "seller_last_name").alias("seller_full_name"),
        "seller_email",
        "seller_country",
        "seller_postal_code",
        "source_file",
    ).dropDuplicates(["seller_key"])

    dim_product = with_keys.select(
        "product_key",
        col("sale_product_id").alias("product_source_id"),
        "product_name",
        "product_category",
        "pet_category",
        "product_price",
        col("product_quantity").alias("product_stock_quantity"),
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
        "source_file",
    ).dropDuplicates(["product_key"])

    dim_store = with_keys.select(
        "store_key",
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
        "source_file",
    ).dropDuplicates(["store_key"])

    dim_supplier = with_keys.select(
        "supplier_key",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
        "source_file",
    ).dropDuplicates(["supplier_key"])

    dim_date = with_keys.select(
        "date_key",
        to_date(col("sale_date")).alias("date_value"),
        year("sale_date").alias("year"),
        quarter("sale_date").alias("quarter"),
        month("sale_date").alias("month"),
        date_format("sale_date", "MMMM").alias("month_name"),
        dayofmonth("sale_date").alias("day"),
    ).dropDuplicates(["date_key"])

    fact_sales = with_keys.select(
        "sale_key",
        "date_key",
        "customer_key",
        "seller_key",
        "product_key",
        "store_key",
        "supplier_key",
        col("id").alias("source_row_id"),
        "sale_quantity",
        "sale_total_price",
        "source_file",
    )

    outputs = {
        "dwh.dim_customer": dim_customer,
        "dwh.dim_seller": dim_seller,
        "dwh.dim_product": dim_product,
        "dwh.dim_store": dim_store,
        "dwh.dim_supplier": dim_supplier,
        "dwh.dim_date": dim_date,
        "dwh.fact_sales": fact_sales,
    }

    for table_name, df in outputs.items():
        write_postgres_table(df, table_name)
        print(f"Saved {df.count()} rows into {table_name}")

    raw.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
