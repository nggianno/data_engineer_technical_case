# utils/debug_utils.py

"""
DEBUG UTILITIES
---------------
Helper functions for inspecting PySpark datasets quickly.
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def debug_dataset(spark: SparkSession, path: str, n: int = 5) -> None:
    """
    Load a dataset (CSV or Parquet) and print:
      - First few rows
      - Number of rows and columns
      - Schema (optional)

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    path : str
        Path to the dataset (folder or file)
    n : int, optional
        Number of rows to display (default = 5)
    """

    print(f"\nInspecting dataset at: {path}")

    # Try to detect file format automatically
    if path.endswith(".csv"):
        df = spark.read.option("header", True).csv(path)
    else:
        df = spark.read.parquet(path)

    # Print schema
    print("\n🧩 Schema:")
    df.printSchema()

    # Show sample rows
    print(f"\nShowing first {n} rows:")
    df.show(n, truncate=False)

    # Count rows and columns
    row_count = df.count()
    col_count = len(df.columns)

    print(df.columns)

    print(f"\nShape: {row_count:,} rows × {col_count} columns\n")

    return df

