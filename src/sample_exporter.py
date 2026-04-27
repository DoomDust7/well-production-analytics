"""
Sample Exporter: Generate data/sample/*.csv files for Streamlit Cloud deployment.
These CSVs are the fallback data source when Delta Lake is not available.
"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import gold_path, silver_path


def _write_csv(df: DataFrame, path: str, label: str) -> int:
    """Collect DataFrame to single CSV file."""
    pdf = df.toPandas()
    pdf.to_csv(path, index=False)
    n = len(pdf)
    print(f"  {label:<45}: {n:>8,} rows -> {os.path.basename(path)}")
    return n


def export_gold_table(spark: SparkSession, table_name: str, output_dir: str,
                      max_rows: int = None) -> int:
    """Export a Gold Delta table to CSV sample."""
    delta_p = gold_path(table_name)
    if not os.path.exists(delta_p):
        print(f"  {table_name:<45}: SKIPPED (not found)")
        return 0
    df = spark.read.format("delta").load(delta_p)
    if max_rows:
        df = df.limit(max_rows)
    dest = os.path.join(output_dir, f"{table_name}.csv")
    return _write_csv(df, dest, table_name)


def export_silver_production_sample(spark: SparkSession, output_dir: str,
                                    max_wells: int = 500) -> int:
    """
    Export a representative sample of silver_production to CSV.
    Picks the top N wells by cumulative oil and exports all their months.
    """
    # Prefer real production
    real_path = silver_path("silver_production_real")
    legacy_path = silver_path("silver_production")

    if os.path.exists(real_path):
        prod = spark.read.format("delta").load(real_path)
        if "shale_play" not in prod.columns:
            prod = prod.withColumn("shale_play",
                                   F.coalesce(F.col("basin"), F.lit("Williston")))
        if "well_name" not in prod.columns:
            prod = prod.withColumn("well_name", F.col("api_number"))
        if "well_length_ft" not in prod.columns:
            prod = prod.withColumn("well_length_ft", F.lit(None).cast("double"))
    elif os.path.exists(legacy_path):
        prod = spark.read.format("delta").load(legacy_path)
    else:
        print(f"  silver_production sample      : SKIPPED (no source found)")
        return 0

    top_wells = (
        prod.filter(F.col("oil_and_gas_group") == "O")
        .groupBy("api_number")
        .agg(F.sum("production").alias("cum_oil"))
        .orderBy(F.col("cum_oil").desc())
        .limit(max_wells)
        .select("api_number")
    )

    sample = prod.join(top_wells, on="api_number", how="inner")
    dest = os.path.join(output_dir, "silver_production.csv")
    return _write_csv(sample, dest, "silver_production (sample)")


def export_flaring_timeseries_sample(spark: SparkSession, output_dir: str,
                                     top_n_operators: int = 50) -> int:
    """Export top-N operators' flaring time-series to keep file size bounded."""
    delta_p = gold_path("gold_flaring_timeseries")
    if not os.path.exists(delta_p):
        print(f"  gold_flaring_timeseries       : SKIPPED (not found)")
        return 0

    df = spark.read.format("delta").load(delta_p)

    top_ops = (
        df.groupBy("operator")
        .agg(F.sum("flared_gas_mcf").alias("total_flared"))
        .orderBy(F.col("total_flared").desc())
        .limit(top_n_operators)
        .select("operator")
    )
    sample = df.join(top_ops, on="operator", how="inner")
    dest = os.path.join(output_dir, "gold_flaring_timeseries.csv")
    return _write_csv(sample, dest, "gold_flaring_timeseries (top-50 ops)")


def export_three_stream_sample(spark: SparkSession, output_dir: str,
                               max_wells: int = 200) -> int:
    """Export three-stream data for top-N wells to keep file size bounded."""
    delta_p = gold_path("gold_three_stream_production")
    if not os.path.exists(delta_p):
        print(f"  gold_three_stream_production  : SKIPPED (not found)")
        return 0

    df = spark.read.format("delta").load(delta_p)

    top_wells = (
        df.groupBy("api_number")
        .agg(F.count("*").alias("months"))
        .orderBy(F.col("months").desc())
        .limit(max_wells)
        .select("api_number")
    )
    sample = df.join(top_wells, on="api_number", how="inner")
    dest = os.path.join(output_dir, "gold_three_stream_production.csv")
    return _write_csv(sample, dest, "gold_three_stream_production (200 wells)")


def run_export(spark: SparkSession, output_dir: str) -> dict:
    """
    Export all Gold/Silver tables to data/sample/ for Streamlit Cloud.
    Returns {filename: row_count}.
    """
    os.makedirs(output_dir, exist_ok=True)
    counts = {}
    print(f"\n[EXPORT] Writing CSV samples to {output_dir} ...")

    # Existing Gold tables (now with real data)
    for table in [
        "gold_operator_performance",
        "gold_basin_production_trends",
        "gold_flaring_intensity",
        "gold_ethane_dry_gas",
        "gold_well_summary",
        "gold_production_forecast",
    ]:
        n = export_gold_table(spark, table, output_dir)
        counts[table] = n

    # Silver production sample (capped by well count)
    n = export_silver_production_sample(spark, output_dir, max_wells=500)
    counts["silver_production"] = n

    # New Gold tables
    n = export_gold_table(spark, "gold_well_economics",  output_dir)
    counts["gold_well_economics"] = n

    n = export_gold_table(spark, "gold_ip_benchmarks", output_dir)
    counts["gold_ip_benchmarks"] = n

    n = export_flaring_timeseries_sample(spark, output_dir, top_n_operators=50)
    counts["gold_flaring_timeseries"] = n

    n = export_three_stream_sample(spark, output_dir, max_wells=200)
    counts["gold_three_stream_production"] = n

    total = sum(counts.values())
    print(f"\n[EXPORT] Done — {total:,} total rows across {len(counts)} files")
    return counts
