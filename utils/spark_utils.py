from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "HEMA ETL", local: bool = True, disable_crc: bool = True):
    """
    Create and configure a SparkSession for ETL jobs.

    Parameters
    ----------
    app_name : str
        Name of the Spark application.
    local : bool
        If True, runs in local mode. In production, set to False for cluster execution.
    disable_crc : bool
        If True, disables Hadoop CRC and success marker files (useful for local testing).

    Returns
    -------
    SparkSession
    """

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
    )

    if local:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()

    # Optional clean-up configs for local dev
    if disable_crc:
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        spark.conf.set("dfs.client.write.checksum", "false")
        spark.conf.set("spark.hadoop.mapreduce.output.fileoutputformat.compress", "false")

    # Reduce Spark verbosity
    spark.sparkContext.setLogLevel("WARN")

    return spark
