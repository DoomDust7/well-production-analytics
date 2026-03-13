"""
Well Production Forecasting & Performance Analytics — Pipeline Orchestrator

Runs the full Medallion pipeline: Bronze → Silver → Gold → Forecasting.

Usage:
    python run_pipeline.py                  # run all stages
    python run_pipeline.py --stage bronze   # run only Bronze ingestion
    python run_pipeline.py --stage silver
    python run_pipeline.py --stage gold
    python run_pipeline.py --stage forecast
"""
import argparse
import sys
import time

from src.spark_session import get_spark, RAW_DIR
from src.data_downloader import download_all
from src.synthetic_data import save_synthetic_data
from src.bronze_ingestion import run_bronze
from src.silver_transform import run_silver
from src.gold_analytics import run_gold
from src.forecasting import run_forecasting


def _hms(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def main(stage: str = "all"):
    print("=" * 65)
    print("  Well Production Forecasting & Performance Analytics System")
    print("=" * 65)

    t_total = time.time()
    spark = get_spark()
    all_counts = {}

    # ── Data acquisition ──────────────────────────────────────────────────────
    if stage in ("all", "bronze"):
        print("\n[DATA] Downloading source CSVs from GitHub...")
        download_all(verbose=True)

        print("\n[DATA] Generating synthetic well production time-series...")
        import os
        wells_path = os.path.join(RAW_DIR, "well_metadata.csv")
        save_synthetic_data(wells_csv_path=wells_path, n_wells=300)

    # ── Bronze ────────────────────────────────────────────────────────────────
    if stage in ("all", "bronze"):
        t0 = time.time()
        counts = run_bronze(spark)
        all_counts.update(counts)
        print(f"  [TIME] Bronze completed in {_hms(time.time() - t0)}")

    # ── Silver ────────────────────────────────────────────────────────────────
    if stage in ("all", "silver"):
        t0 = time.time()
        counts = run_silver(spark)
        all_counts.update(counts)
        print(f"  [TIME] Silver completed in {_hms(time.time() - t0)}")

    # ── Gold ──────────────────────────────────────────────────────────────────
    if stage in ("all", "gold"):
        t0 = time.time()
        counts = run_gold(spark)
        all_counts.update(counts)
        print(f"  [TIME] Gold completed in {_hms(time.time() - t0)}")

    # ── Forecasting ───────────────────────────────────────────────────────────
    if stage in ("all", "forecast"):
        t0 = time.time()
        n = run_forecasting(spark)
        all_counts["gold_production_forecast"] = n
        print(f"  [TIME] Forecasting completed in {_hms(time.time() - t0)}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("  PIPELINE COMPLETE — Table Row Counts")
    print("=" * 65)
    for table, count in all_counts.items():
        layer = table.split("_")[0].upper()
        print(f"  [{layer:6s}] {table:<40s} {count:>8,}")
    print(f"\n  Total pipeline time: {_hms(time.time() - t_total)}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Well Production Analytics Pipeline")
    parser.add_argument(
        "--stage",
        choices=["all", "bronze", "silver", "gold", "forecast"],
        default="all",
        help="Pipeline stage to run (default: all)"
    )
    args = parser.parse_args()
    main(stage=args.stage)
