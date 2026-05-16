from pyspark.sql.functions import col, input_file_name, regexp_extract, to_date
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from io_utils import spark_session, write_postgres_table
from settings import CSV_INPUT_PATH


RAW_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("customer_first_name", StringType()),
        StructField("customer_last_name", StringType()),
        StructField("customer_age", IntegerType()),
        StructField("customer_email", StringType()),
        StructField("customer_country", StringType()),
        StructField("customer_postal_code", StringType()),
        StructField("customer_pet_type", StringType()),
        StructField("customer_pet_name", StringType()),
        StructField("customer_pet_breed", StringType()),
        StructField("seller_first_name", StringType()),
        StructField("seller_last_name", StringType()),
        StructField("seller_email", StringType()),
        StructField("seller_country", StringType()),
        StructField("seller_postal_code", StringType()),
        StructField("product_name", StringType()),
        StructField("product_category", StringType()),
        StructField("product_price", DecimalType(12, 2)),
        StructField("product_quantity", IntegerType()),
        StructField("sale_date", StringType()),
        StructField("sale_customer_id", IntegerType()),
        StructField("sale_seller_id", IntegerType()),
        StructField("sale_product_id", IntegerType()),
        StructField("sale_quantity", IntegerType()),
        StructField("sale_total_price", DecimalType(14, 2)),
        StructField("store_name", StringType()),
        StructField("store_location", StringType()),
        StructField("store_city", StringType()),
        StructField("store_state", StringType()),
        StructField("store_country", StringType()),
        StructField("store_phone", StringType()),
        StructField("store_email", StringType()),
        StructField("pet_category", StringType()),
        StructField("product_weight", DecimalType(12, 2)),
        StructField("product_color", StringType()),
        StructField("product_size", StringType()),
        StructField("product_brand", StringType()),
        StructField("product_material", StringType()),
        StructField("product_description", StringType()),
        StructField("product_rating", DecimalType(4, 2)),
        StructField("product_reviews", IntegerType()),
        StructField("product_release_date", StringType()),
        StructField("product_expiry_date", StringType()),
        StructField("supplier_name", StringType()),
        StructField("supplier_contact", StringType()),
        StructField("supplier_email", StringType()),
        StructField("supplier_phone", StringType()),
        StructField("supplier_address", StringType()),
        StructField("supplier_city", StringType()),
        StructField("supplier_country", StringType()),
    ]
)


def main() -> None:
    spark = spark_session("load-raw-csv-to-postgres")

    raw = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .schema(RAW_SCHEMA)
        .csv(CSV_INPUT_PATH)
        .withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy").cast(DateType()))
        .withColumn("product_release_date", to_date(col("product_release_date"), "M/d/yyyy").cast(DateType()))
        .withColumn("product_expiry_date", to_date(col("product_expiry_date"), "M/d/yyyy").cast(DateType()))
        .withColumn("source_file", regexp_extract(input_file_name(), r"([^/\\]+\.csv)$", 1))
    )

    write_postgres_table(raw, "raw.mock_data")
    print(f"Loaded {raw.count()} rows into raw.mock_data")

    spark.stop()


if __name__ == "__main__":
    main()
