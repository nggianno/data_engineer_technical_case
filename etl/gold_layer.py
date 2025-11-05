# etl/gold_layer.py

"""
GOLD LAYER ETL
--------------
This module reads the clean Silver data, applies business transformations,
and writes the business-ready datasets (Sales and Customers) into the Gold layer.

Responsibilities:
- Split Silver data into Sales and Customer datasets
- Compute customer-level order metrics (1, 6, 12 months, total)
- Partition by order date for Sales, overwrite Customers each run
"""

from pyspark.sql.functions import col, countDistinct, when, lit, to_date, split, trim, size
from pyspark.sql.types import DateType
from pyspark.sql.functions import current_timestamp, input_file_name
from datetime import datetime, timedelta
from utils.dq_checks import run_gold_quality_checks
from utils.logger import get_logger
logger = get_logger(__name__)

def generate_gold_datasets(
    spark,
    silver_path: str,
    gold_sales_path: str,
    gold_customers_path: str,
):
    logger.info(f"🔄 Reading Silver data from: {silver_path}")
    df = spark.read.parquet(silver_path)

    # Ensure order_date is a proper date
    df = df.withColumn("order_date", col("order_date").cast(DateType()))

    #------------------- SALES DATASET -------------------
    sales_df = df.select(
        "order_id",
        "order_date",
        "order_year",
        "order_month",
        "order_day",
        "ship_date",
        "ship_mode",
        "city",
        "file_path",
        "execution_datetime",
    ).dropDuplicates(["order_id"])

    sales_df = sales_df.withColumn("file_path", lit(silver_path))
    sales_df = sales_df.withColumn("execution_datetime", current_timestamp())
    logger.info(f"💾 Writing Sales dataset to: {gold_sales_path}")

    run_gold_quality_checks(sales_df, logger)

    (
        sales_df.write
        .option("mergeSchema", "true")
        .mode("overwrite")
        .partitionBy("order_year", "order_month", "order_day")
        .parquet(gold_sales_path)
    )

    # ------------------- CUSTOMER DATASET -------------------
    logger.info(f"🔄 Creating Customer dataset...")

    # Split first and last names
    df = df.withColumn("customer_first_name", trim(split(col("customer_name"), " ").getItem(0)))
    df = df.withColumn(
        "customer_last_name",
        when(size(split(col("customer_name"), " ")) > 1, trim(split(col("customer_name"), " ").getItem(1)))
        .otherwise(lit(None))
    )

    # Define date window boundaries
    reference_date = datetime(2018, 12, 30)
    last_month = reference_date - timedelta(days=30)
    last_6_months = reference_date - timedelta(days=180)
    last_12_months = reference_date - timedelta(days=365)

    customer_agg = (
        df.groupBy(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "segment",
            "country"
        ).agg(
            countDistinct(
                when(col("order_date") >= lit(last_month), col("order_id"))
            ).alias("orders_last_month"),
            countDistinct(
                when(col("order_date") >= lit(last_6_months), col("order_id"))
            ).alias("orders_last_6_months"),
            countDistinct(
                when(col("order_date") >= lit(last_12_months), col("order_id"))
            ).alias("orders_last_12_months"),
            countDistinct(col("order_id")).alias("orders_all_time")
        )
    )


    customer_agg = customer_agg.withColumn("file_path", lit(silver_path))
    customer_agg = customer_agg.withColumn("execution_datetime", current_timestamp())

    logger.info(f"💾 Writing Customer dataset to: {gold_customers_path}")

    # run_gold_quality_checks(customer_agg, logger)

    (
        customer_agg.write
        .option("mergeSchema", "true")
        .mode("overwrite")
        .partitionBy("segment")
        .parquet(gold_customers_path)
    )

    logger.info("✅ Gold layer successfully created.")

    return sales_df, customer_agg
