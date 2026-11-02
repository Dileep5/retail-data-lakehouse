from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from lakehouse.config import Settings


def get_spark(settings: Settings, app_name: str = "retail-lakehouse") -> SparkSession:
    # Python workers must run the same interpreter as the driver; otherwise Spark picks
    # whatever `python3` is on PATH and fails with PYTHON_VERSION_MISMATCH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master(settings.spark_master)
        .appName(app_name)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", "false")  # malformed values become NULL -> quarantined
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
