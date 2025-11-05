from pyspark.sql.functions import col, countDistinct, current_date, isnan

def run_bronze_quality_checks(df, logger) -> None:
    """
    Run basic data quality checks for the Bronze layer.
    Logs any warnings but does not stop execution (to remain flexible).
    """

    logger.info("---Running Bronze layer data quality checks---")

    # Check for duplicate Row IDs
    if "row_id" in df.columns:
        total_rows = df.count()
        unique_rows = df.select(countDistinct("row_id")).collect()[0][0]
        if total_rows != unique_rows:
            logger.warning(f"⚠️ Duplicate row_id values found: {total_rows - unique_rows}")
        else:
            logger.info("✅ No duplicate row_id values found.")

    # Check for missing critical fields
    critical_columns = [c for c in ["order_id", "customer_id", "product_id", "sales"] if c in df.columns]
    for col_name in critical_columns:
        missing = df.filter(col(col_name).isNull() | isnan(col(col_name)) | (col(col_name) == "")).count()
        if missing > 0:
            logger.warning(f"⚠️ Column '{col_name}' has {missing} missing values.")
        else:
            logger.info(f"✅ Column '{col_name}' has no missing values.")

    # Log column summary for schema evolution tracking
    logger.info(f"📊 Current schema columns ({len(df.columns)}): {', '.join(df.columns)}")

    logger.info("---Bronze layer data quality checks completed.---")

def run_silver_quality_checks(df, logger):

    logger.info("---Running Silver layer data quality checks---")

    # Date consistency
    if set(["order_date", "ship_date"]).issubset(df.columns):
        invalid = df.filter(col("ship_date") < col("order_date")).count()
        if invalid > 0:
            logger.warning(f"⚠️ {invalid} records with ship_date earlier than order_date.")
        else:
            logger.info("✅ All ship_date values are on or after order_date.")

    # Sales sanity
    if "sales" in df.columns:
        invalid_sales = df.filter(col("sales") <= 0).count()
        if invalid_sales > 0:
            logger.warning(f"⚠️ {invalid_sales} records have non-positive sales values.")
        else:
            logger.info("✅ All sales values are positive.")


    # Missing values summary
    key_columns = ["order_id", "customer_id", "product_id"]
    for c in key_columns:
        missing = df.filter(col(c).isNull()).count()
        if missing > 0:
            logger.warning(f"⚠️ Column '{c}' has {missing} null values.")
        else:
            logger.info(f"✅ Column '{c}' has no missing values.")

    logger.info("---Silver layer data quality checks completed.---")

def run_gold_quality_checks(df, logger):

    logger.info(f"---Running Data Quality Checks for gold layer---")

    # Duplicate order_id
    dup_count = (
        df.groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    if dup_count > 0:
        logger.warning(f"⚠️ Found {dup_count} duplicate order_id records in gold layer.")

    # Null checks
    null_count = (
        df.filter(col("order_id").isNull() | col("customer_id").isNull()).count()
        if "customer_id" in df.columns
        else df.filter(col("order_id").isNull()).count()
    )
    if null_count > 0:
        logger.warning(f"⚠️ Found {null_count} null values in key columns for gold layer.")

    logger.info("✅ Data Quality Checks completed.\n")
