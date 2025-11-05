from utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from utils.logger import get_logger
import os
import argparse

logger = get_logger(__name__)


def parquet_to_csv(
        spark: SparkSession,
        parquet_path: str,
        csv_output_path: str,
        header: bool = True,
        delimiter: str = ",",
        preview_rows: int = 10
) -> DataFrame:
    """
    Read a Parquet file/directory and export it to a single CSV file.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    parquet_path : str
        Full path to the Parquet file or directory
    csv_output_path : str
        Full path where the CSV file will be saved (e.g., "output/data.csv")
    header : bool, default=True
        Whether to include column headers in the CSV
    delimiter : str, default=","
        Field delimiter for the CSV file
    preview_rows : int, default=10
        Number of rows to preview in console

    Returns
    -------
    DataFrame
        The Spark DataFrame that was converted

    Examples
    --------
    """

    logger.info(f"📂 Reading Parquet data from: {parquet_path}")

    try:
        # Read the Parquet file
        df = spark.read.parquet(parquet_path)

        # Get dataset statistics
        row_count = df.count()
        col_count = len(df.columns)

        logger.info(f"✅ Successfully loaded Parquet data")
        logger.info(f"📊 Dataset: {row_count:,} rows × {col_count} columns")
        logger.info(f"📝 Columns: {', '.join(df.columns)}")

        # Preview data
        if preview_rows > 0:
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Preview - First {preview_rows} rows:")
            logger.info(f"{'=' * 80}")
            df.show(preview_rows, truncate=False)

        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(csv_output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"📁 Created output directory: {output_dir}")

        # Use a temporary directory for Spark's CSV writer
        temp_csv_dir = csv_output_path + "_temp"

        # Write using PySpark's native CSV writer (avoids pandas/distutils dependency)
        logger.info(f"💾 Converting to CSV format...")

        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", str(header).lower()) \
            .option("delimiter", delimiter) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("quoteAll", True) \
            .csv(temp_csv_dir)

        # Find the generated CSV file (Spark creates part-*.csv files)
        import glob
        csv_files = glob.glob(os.path.join(temp_csv_dir, "part-*.csv"))

        if csv_files:
            # Move the CSV file to the desired location
            import shutil
            shutil.move(csv_files[0], csv_output_path)

            # Clean up the temporary directory
            shutil.rmtree(temp_csv_dir)

            # Get file size
            file_size_mb = os.path.getsize(csv_output_path) / (1024 * 1024)

            logger.info(f"✅ CSV file successfully created!")
            logger.info(f"📄 Location: {csv_output_path}")
            logger.info(f"📦 File size: {file_size_mb:.2f} MB")
        else:
            logger.error(f"❌ No CSV file generated in temporary directory")
            raise FileNotFoundError("CSV conversion failed - no output file generated")

        return df

    except Exception as e:
        logger.error(f"❌ Error converting Parquet to CSV: {str(e)}", exc_info=True)
        raise


spark = get_spark_session()

# Convert bronze layer to CSV
parquet_to_csv(
    spark=spark,
    parquet_path="data/bronze/",
    csv_output_path="exports/bronze_data.csv"
)

# Convert silver layer with custom settings
parquet_to_csv(
    spark=spark,
    parquet_path="data/silver/",
    csv_output_path="exports/silver_data.csv",
)

# Convert gold layer
parquet_to_csv(
    spark=spark,
    parquet_path="data/gold/sales/",
    csv_output_path="exports/gold_sales.csv"
)

parquet_to_csv(
    spark=spark,
    parquet_path="data/gold/customers/",
    csv_output_path="exports/gold_customers.csv"
)

spark.stop()
