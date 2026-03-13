"""
SparkSession factory and path helpers.
All Delta Lake configuration lives here.
"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
DELTA_DIR = os.path.join(DATA_DIR, "delta")


def get_spark(app_name: str = "WellProductionAnalytics"):
    """
    Creates or reuses a local SparkSession with Delta Lake 3.1.0 extensions.
    Uses configure_spark_with_delta_pip for reliable local JAR injection.
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def bronze_path(table_name: str) -> str:
    return os.path.join(DELTA_DIR, "bronze", table_name)


def silver_path(table_name: str) -> str:
    return os.path.join(DELTA_DIR, "silver", table_name)


def gold_path(table_name: str) -> str:
    return os.path.join(DELTA_DIR, "gold", table_name)
