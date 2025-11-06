from pyspark.sql.functions import current_timestamp, input_file_name
import re
from pyspark.sql.functions import to_date, year, month, dayofmonth, lit
from utils.logger import get_logger
from utils.dq_checks import run_bronze_quality_checks
import pyspark.sql.functions as F

logger = get_logger(__name__)


def incremental_merge_parquet(spark, new_df, output_path):
    try:
        existing_df = spark.read.parquet(output_path)

        # Union with preference for new data (based on latest execution_datetime)
        merged_df = (
            existing_df.unionByName(new_df, allowMissingColumns=True)
            .orderBy(F.col("execution_datetime").desc())
            .dropDuplicates(["row_id"])
        )

        merged_df.write \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("order_year", "order_month", "order_day") \
            .parquet(output_path)

        return merged_df

    except Exception:
        # First load
        new_df.write \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("order_year", "order_month", "order_day") \
            .parquet(output_path)
        return new_df

def to_snake_case(col_name: str) -> str:

    col_name = re.sub(r'[^0-9a-zA-Z]+', '_', col_name)  # Replace non-alphanumerics with _
    col_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col_name)  # Handle camelCase
    col_name = re.sub(r'_+', '_', col_name)  # Remove double underscores
    return col_name.strip('_').lower()

def ingest_bronze_layer(spark, input_path: str, output_path: str):

    logger.info(f"🔄 Reading CSV file with sales data from: {input_path}")

    spark.conf.set("spark.sql.parquet.mergeSchema", "true")  # Allow schema evolution

    # Read CSV
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # Rename columns
    for old_col in df.columns:
        df = df.withColumnRenamed(old_col, to_snake_case(old_col))

    # Add metadata
    df = (
        df.withColumn("file_path", input_file_name().cast("string"))
        .withColumn("execution_datetime", current_timestamp().cast("timestamp"))
    )

    # Convert order_date and ship_date from string to date
    df = df.withColumn("order_date", to_date(df["order_date"], "dd/MM/yyyy")) \
        .withColumn("ship_date", to_date(df["ship_date"], "dd/MM/yyyy"))

    # Add partition columns
    df = df.withColumn("order_year", year(df["order_date"])) \
        .withColumn("order_month", month(df["order_date"])) \
        .withColumn("order_day", dayofmonth(df["order_date"]))

    run_bronze_quality_checks(df, logger)

    # Write to Bronze (Parquet)
    incremental_merge_parquet(spark, df, output_path)

    logger.info("✅ Bronze layer ingestion completed successfully.")


