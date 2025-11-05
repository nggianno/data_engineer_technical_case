
"""
Quick utility to inspect any dataset in the HEMA ETL pipeline.
"""

import argparse
from utils.spark_utils import get_spark_session
from utils.debug_utils import debug_dataset
from utils.logger import get_logger
import yaml

def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HEMA ETL Debug Tool")
    parser.add_argument("--layer", type=str, help="bronze | silver | gold_sales | gold_customers", default="silver")
    parser.add_argument("--path", type=str, help="Custom dataset path", default=None)
    parser.add_argument("--rows", type=int, help="Number of rows to preview", default=50)
    args = parser.parse_args()

    config = load_config()
    logger = get_logger("debug_tool", "INFO")
    spark = get_spark_session(app_name="HEMA Debugger", local=True)


    # Select layer path
    if args.path:
        path = args.path
    else:
        path = config["data_paths"].get(args.layer)
        if not path:
            raise ValueError(f"Unknown layer: {args.layer}")

    logger.info(f"🔍 Debugging dataset: {args.layer} ({path})")

    debug_dataset(spark, path, n=args.rows)

    spark.stop()



