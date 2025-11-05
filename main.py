import os, sys, yaml
from utils.spark_utils import get_spark_session
from utils.logger import get_logger
from etl.bronze_layer import ingest_bronze_layer
from etl.silver_layer import transform_to_silver
from etl.gold_layer import generate_gold_datasets

# Ensure same Python interpreter for driver & workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def load_config(path: str = "config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

if __name__ == "__main__":

    config = load_config()
    logger = get_logger(name="hema_etl",level="INFO",log_dir="logs",log_to_file=True)

    spark = get_spark_session(
        app_name=config["spark"]["app_name"],
        local=True,
        disable_crc=config["spark"]["disable_crc"]
    )

    logger.info("🚀...Starting HEMA ETL pipeline")

    # Paths
    raw = config["data_paths"]["raw"]
    bronze = config["data_paths"]["bronze"]
    silver = config["data_paths"]["silver"]
    gold_sales = config["data_paths"]["gold_sales"]
    gold_customers = config["data_paths"]["gold_customers"]

    try:
        # --- Bronze ---
        logger.info("🏗️ Running Bronze layer ingestion")
        ingest_bronze_layer(spark, raw, bronze)

        # --- Silver ---
        logger.info("⚙️ Running Silver layer transformation")
        transform_to_silver(spark, bronze, silver)

        # --- Gold ---
        logger.info("🥇 Generating Gold layer datasets")
        generate_gold_datasets(spark, silver, gold_sales, gold_customers)

        logger.info("✅ ETL pipeline completed successfully!")

    except Exception as e:
        logger.error(f"❌ ETL pipeline failed with error: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")
