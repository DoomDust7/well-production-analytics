"""
Bronze Layer: Raw ingestion into Delta Lake tables.
- No business transformations; raw values preserved as strings
- Adds metadata columns: load_timestamp, source_file, data_source
- Production tables partitioned by Year for time-series scalability
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


# ── Legacy hackathon sources ──────────────────────────────────────────────────

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
    """Ingest synthetic well production data. Kept for backward compatibility."""
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "synthetic_arps_model")
    df = df.withColumn("ingest_year", F.year(F.col("load_timestamp")).cast("string"))
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .partitionBy("ingest_year")
       .save(bronze_path("bronze_well_production")))
    return df.count()


# ── Wells-Dataset real data sources ──────────────────────────────────────────

def ingest_wellheader(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).option("multiLine", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_wellheader")))
    return df.count()


def ingest_production_real(spark: SparkSession, csv_path: str) -> int:
    """
    Ingest real per-well monthly production. Filters to North Dakota wells
    (API prefix '33') to keep scope manageable. Partitioned by year.
    """
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    # Restrict to North Dakota (API starts with 33) to bound data volume
    api_col = next((c for c in df.columns if c in ("api_number", "api_number_")), None)
    if api_col:
        df = df.filter(F.col(api_col).startswith("33"))
    # Partition by the year column present in the raw data
    year_col = next((c for c in df.columns if c == "year"), None)
    if year_col:
        (df.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .partitionBy(year_col)
           .save(bronze_path("bronze_production_real")))
    else:
        (df.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .save(bronze_path("bronze_production_real")))
    return df.count()


def ingest_production_flaring_real(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    api_col = next((c for c in df.columns if c in ("api_number", "api_number_")), None)
    if api_col:
        df = df.filter(F.col(api_col).startswith("33"))
    year_col = next((c for c in df.columns if c == "year"), None)
    if year_col:
        (df.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .partitionBy(year_col)
           .save(bronze_path("bronze_production_flaring_real")))
    else:
        (df.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .save(bronze_path("bronze_production_flaring_real")))
    return df.count()


def ingest_water_production(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    api_col = next((c for c in df.columns if c in ("api_number", "api_number_")), None)
    if api_col:
        df = df.filter(F.col(api_col).startswith("33"))
    year_col = next((c for c in df.columns if c == "year"), None)
    if year_col:
        (df.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .partitionBy(year_col)
           .save(bronze_path("bronze_water_production")))
    else:
        (df.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .save(bronze_path("bronze_water_production")))
    return df.count()


def ingest_welleur(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_welleur")))
    return df.count()


def ingest_initialproduction(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_initialproduction")))
    return df.count()


def ingest_economicscost(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_economicscost")))
    return df.count()


def ingest_eur(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_eur")))
    return df.count()


def ingest_prices(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_prices")))
    return df.count()


def ingest_operator(spark: SparkSession, csv_path: str) -> int:
    raw = spark.read.option("header", True).csv(csv_path)
    df = _add_metadata(raw, os.path.basename(csv_path), "doomdust7_wells_dataset")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(bronze_path("bronze_operator")))
    return df.count()


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_bronze(spark: SparkSession, raw_dir: str = None, use_synthetic: bool = False) -> dict:
    """Run all Bronze ingestions. Returns {table: row_count}."""
    if raw_dir is None:
        raw_dir = RAW_DIR
    counts = {}

    def _path(fname):
        return os.path.join(raw_dir, fname)

    print("\n[BRONZE] Ingesting raw data into Delta tables...")

    # Wells-Dataset real data (primary sources)
    if os.path.exists(_path("WELLHEADER.csv")):
        n = ingest_wellheader(spark, _path("WELLHEADER.csv"))
        counts["bronze_wellheader"] = n
        print(f"  bronze_wellheader              : {n:>8,} rows")

    if os.path.exists(_path("PRODUCTION.csv")):
        n = ingest_production_real(spark, _path("PRODUCTION.csv"))
        counts["bronze_production_real"] = n
        print(f"  bronze_production_real         : {n:>8,} rows")

    if os.path.exists(_path("PRODUCTIONFLARING.csv")):
        n = ingest_production_flaring_real(spark, _path("PRODUCTIONFLARING.csv"))
        counts["bronze_production_flaring_real"] = n
        print(f"  bronze_production_flaring_real : {n:>8,} rows")

    if os.path.exists(_path("WATERPRODUCTION.csv")):
        n = ingest_water_production(spark, _path("WATERPRODUCTION.csv"))
        counts["bronze_water_production"] = n
        print(f"  bronze_water_production        : {n:>8,} rows")

    if os.path.exists(_path("WELLEUR.csv")):
        n = ingest_welleur(spark, _path("WELLEUR.csv"))
        counts["bronze_welleur"] = n
        print(f"  bronze_welleur                 : {n:>8,} rows")

    if os.path.exists(_path("INITIALPRODUCTION.csv")):
        n = ingest_initialproduction(spark, _path("INITIALPRODUCTION.csv"))
        counts["bronze_initialproduction"] = n
        print(f"  bronze_initialproduction       : {n:>8,} rows")

    if os.path.exists(_path("ECONOMICSCOST.csv")):
        n = ingest_economicscost(spark, _path("ECONOMICSCOST.csv"))
        counts["bronze_economicscost"] = n
        print(f"  bronze_economicscost           : {n:>8,} rows")

    if os.path.exists(_path("EUR.csv")):
        n = ingest_eur(spark, _path("EUR.csv"))
        counts["bronze_eur"] = n
        print(f"  bronze_eur                     : {n:>8,} rows")

    if os.path.exists(_path("PRICES.csv")):
        n = ingest_prices(spark, _path("PRICES.csv"))
        counts["bronze_prices"] = n
        print(f"  bronze_prices                  : {n:>8,} rows")

    if os.path.exists(_path("OPERATOR.csv")):
        n = ingest_operator(spark, _path("OPERATOR.csv"))
        counts["bronze_operator"] = n
        print(f"  bronze_operator                : {n:>8,} rows")

    # Legacy hackathon files (kept for backward compat / enrichment)
    if os.path.exists(_path("flaring.csv")):
        n = ingest_flaring(spark, _path("flaring.csv"))
        counts["bronze_flaring"] = n
        print(f"  bronze_flaring                 : {n:>8,} rows")

    if os.path.exists(_path("operator_well_counts.csv")):
        n = ingest_operator_well_counts(spark, _path("operator_well_counts.csv"))
        counts["bronze_operator_well_counts"] = n
        print(f"  bronze_operator_well_counts    : {n:>8,} rows")

    if os.path.exists(_path("shale_play_production.csv")):
        n = ingest_shale_production(spark, _path("shale_play_production.csv"))
        counts["bronze_shale_production"] = n
        print(f"  bronze_shale_production        : {n:>8,} rows")

    if os.path.exists(_path("well_metadata.csv")):
        n = ingest_well_metadata(spark, _path("well_metadata.csv"))
        counts["bronze_well_metadata"] = n
        print(f"  bronze_well_metadata           : {n:>8,} rows")

    # Synthetic fallback (only when explicitly requested)
    if use_synthetic and os.path.exists(_path("synthetic_well_production.csv")):
        n = ingest_well_production(spark, _path("synthetic_well_production.csv"))
        counts["bronze_well_production"] = n
        print(f"  bronze_well_production (synth) : {n:>8,} rows")

    return counts
