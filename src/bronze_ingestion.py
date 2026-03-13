"""
Bronze Layer: Raw ingestion into Delta Lake tables.
- No business transformations; raw values preserved as strings
- Adds metadata columns: load_timestamp, source_file, data_source
- Production table partitioned by ingest_year
"""
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.spark_session import bronze_path, RAW_DIR


def _sanitize_columns(df: DataFrame) -> DataFrame:
    """Replace spaces and special chars in column names — Delta Lake requirement."""
    import re
    new_cols = [re.sub(r"[ ,;{}()\n\t=]+", "_", c).strip("_").lower()
                for c in df.columns]
    return df.toDF(*new_cols)


def _add_metadata(df: DataFrame, source_file: str, data_source: str) -> DataFrame:
    df = _sanitize_columns(df)
    return (
        df
        .withColumn("load_timestamp", F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("data_source", F.lit(data_source))
    )


def ingest_flaring(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "shell_hackathon_github")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_flaring")))
    return df.count()


def ingest_operator_well_counts(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "shell_hackathon_github")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_operator_well_counts")))
    return df.count()


def ingest_shale_production(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "shell_hackathon_github")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_shale_production")))
    return df.count()


def ingest_well_metadata(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "shell_hackathon_github")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_well_metadata")))
    return df.count()


def ingest_well_production(spark: SparkSession, csv_path: str) -> int:
    """
    Ingest synthetic (+ optionally real chemical_production) well production data.
    Partitioned by ingest_year for time-series scalability.
    """
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "synthetic_arps_model")
    df = df.withColumn("ingest_year", F.year(F.col("load_timestamp")).cast("string"))
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .partitionBy("ingest_year")
       .save(bronze_path("bronze_well_production")))
    return df.count()


def run_bronze(spark: SparkSession, raw_dir: str = None) -> dict:
    """Run all Bronze ingestions. Returns {table: row_count}."""
    if raw_dir is None:
        raw_dir = RAW_DIR
    counts = {}

    def _path(fname):
        return os.path.join(raw_dir, fname)

    print("\n[BRONZE] Ingesting raw data into Delta tables...")

    if os.path.exists(_path("flaring.csv")):
        n = ingest_flaring(spark, _path("flaring.csv"))
        counts["bronze_flaring"] = n
        print(f"  bronze_flaring                : {n:>6,} rows")

    if os.path.exists(_path("operator_well_counts.csv")):
        n = ingest_operator_well_counts(spark, _path("operator_well_counts.csv"))
        counts["bronze_operator_well_counts"] = n
        print(f"  bronze_operator_well_counts   : {n:>6,} rows")

    if os.path.exists(_path("shale_play_production.csv")):
        n = ingest_shale_production(spark, _path("shale_play_production.csv"))
        counts["bronze_shale_production"] = n
        print(f"  bronze_shale_production       : {n:>6,} rows")

    if os.path.exists(_path("well_metadata.csv")):
        n = ingest_well_metadata(spark, _path("well_metadata.csv"))
        counts["bronze_well_metadata"] = n
        print(f"  bronze_well_metadata          : {n:>6,} rows")

    if os.path.exists(_path("synthetic_well_production.csv")):
        n = ingest_well_production(spark, _path("synthetic_well_production.csv"))
        counts["bronze_well_production"] = n
        print(f"  bronze_well_production        : {n:>6,} rows")

    return counts
