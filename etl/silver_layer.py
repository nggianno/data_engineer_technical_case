"""
SILVER LAYER ETL
----------------
This module reads the raw (Bronze) dataset, performs cleaning and standardization,
and outputs a clean, typed, and consistent dataset in the Silver layer.

Responsibilities:
- Read Bronze Parquet data
- Enforce schema and proper data types
- Handle nulls and duplicates
- Standardize key fields (dates, country, postal code, etc.)
- Preserve metadata (file_path, execution_datetime)
- Partition by order_date (year, month, day)
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,trim, upper, current_timestamp, input_file_name, regexp_replace
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType, TimestampType, IntegerType
from utils.dq_checks import run_silver_quality_checks
from utils.logger import get_logger
logger = get_logger(__name__)

def incremental_merge_silver(spark, new_df, silver_path):
    try:
        existing_df = spark.read.parquet(silver_path)
        merged = (
            existing_df.unionByName(new_df, allowMissingColumns=True)
                       .orderBy(col("execution_datetime").desc())
                       .dropDuplicates(["row_id"])
        )
        (merged.write.mode("overwrite").option("mergeSchema", "true").
         partitionBy("order_year", "order_month", "order_day").parquet(silver_path))

        return merged

    except Exception:

        (new_df.write.mode("overwrite").option("mergeSchema", "true").
         partitionBy("order_year", "order_month", "order_day").parquet(silver_path))

        return new_df

def transform_to_silver(spark, bronze_path: str, silver_path: str) -> DataFrame:
    """
    Transform Bronze data into Silver layer (clean and standardized).
    """

    logger.info(f"🔄 Reading Bronze data from: {bronze_path}")
    df = (
        spark.read.parquet(bronze_path)
        .withColumn("file_path", input_file_name())
        .withColumn("execution_datetime", current_timestamp())
    )

    # --- Step 1. Ensure proper data types (cast if needed) ---
    # Since Bronze layer already has proper types, we mainly need to cast 'sales' from String to Double
    df = (
        df.withColumn("sales", col("sales").cast(DoubleType()))
          .withColumn("order_date", col("order_date").cast(DateType()))
          .withColumn("ship_date", col("ship_date").cast(DateType()))
          .withColumn("execution_datetime", col("execution_datetime").cast(TimestampType()))
          .withColumn("order_year", col("order_year").cast(IntegerType()))
          .withColumn("order_month", col("order_month").cast(IntegerType()))
          .withColumn("order_day", col("order_day").cast(IntegerType()))
    )

    # --- Step 2. Clean & standardize columns ---
    df = (
        df.withColumn("country", upper(trim(col("country"))))
          .withColumn("state", trim(col("state")))
          .withColumn("city", trim(col("city")))
          .withColumn("segment", trim(col("segment")))
          .withColumn("category", trim(col("category")))
          .withColumn("sub_category", trim(col("sub_category")))
          .withColumn("product_name", trim(col("product_name")))
    )

    # --- Step 3. Remove duplicates and invalid rows ---
    df = df.dropDuplicates(["row_id", "order_id"])

    string_cols = [c for c, t in df.dtypes if t == "string"]
    for c in string_cols:
        df = df.withColumn(c, regexp_replace(col(c), r"[\n\r\t,]", " "))

    run_silver_quality_checks(df, logger)

    # --- Step 4. Write to Silver layer ---
    logger.info(f"💾 Writing clean Silver data to: {silver_path}")
    incremental_merge_silver(spark, df, silver_path)

    logger.info("✅ Silver layer transformation completed successfully.")

    return df
