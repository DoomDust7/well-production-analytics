"""
Well Production Forecasting & Performance Analytics — Pipeline Orchestrator

Runs the full Medallion pipeline: Download → Bronze → Silver → Gold → Forecast → Export

Usage:
    python run_pipeline.py                    # run all stages
    python run_pipeline.py --stage download   # download CSVs from Wells-Dataset
    python run_pipeline.py --stage bronze     # ingest into Delta bronze tables
    python run_pipeline.py --stage silver     # transform to silver tables
    python run_pipeline.py --stage gold       # compute gold KPI tables
    python run_pipeline.py --stage forecast   # Arps decline curve forecasting
    python run_pipeline.py --stage export     # write data/sample/*.csv for Streamlit Cloud
    python run_pipeline.py --stage synthetic  # (legacy) generate synthetic data only
"""
import argparse
import os
import sys
import time

from src.spark_session import get_spark, RAW_DIR
from src.data_downloader import download_all
from src.bronze_ingestion import run_bronze
from src.silver_transform import run_silver
from src.gold_analytics import run_gold
from src.forecasting import run_forecasting
from src.sample_exporter import run_export


SAMPLE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sample")


def _hms(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def main(stage: str = "all"):
    print("=" * 65)
    print("  Well Production Forecasting & Performance Analytics System")
    print("=" * 65)

    t_total = time.time()
    all_counts = {}

    # ── Download ──────────────────────────────────────────────────────────────
    if stage in ("all", "download", "bronze"):
        print("\n[DATA] Downloading source CSVs from Wells-Dataset (GitHub)...")
        download_all(verbose=True)

    # ── Synthetic (legacy, only when explicitly requested) ────────────────────
    if stage == "synthetic":
        from src.synthetic_data import save_synthetic_data
        import os as _os
        wells_path = _os.path.join(RAW_DIR, "well_metadata.csv")
        print("\n[DATA] Generating synthetic well production time-series...")
        save_synthetic_data(wells_csv_path=wells_path, n_wells=300)
        return

    # ── Spark required from here ──────────────────────────────────────────────
    if stage not in ("download",):
        spark = get_spark()
    else:
        print(f"\n  [TIME] Total: {_hms(time.time() - t_total)}")
        return

    # ── Bronze ────────────────────────────────────────────────────────────────
    if stage in ("all", "bronze"):
        t0 = time.time()
        counts = run_bronze(spark, use_synthetic=False)
        all_counts.update(counts)
        print(f"  [TIME] Bronze completed in {_hms(time.time() - t0)}")

    # ── Silver ────────────────────────────────────────────────────────────────
    if stage in ("all", "silver"):
        t0 = time.time()
        counts = run_silver(spark, use_synthetic=False)
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

    # ── Export (always run at end of "all", or on demand) ─────────────────────
    if stage in ("all", "export"):
        t0 = time.time()
        counts = run_export(spark, SAMPLE_DIR)
        all_counts.update({f"sample/{k}": v for k, v in counts.items()})
        print(f"  [TIME] Export completed in {_hms(time.time() - t0)}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("  PIPELINE COMPLETE — Table Row Counts")
    print("=" * 65)
    for table, count in all_counts.items():
        prefix = table.split("_")[0].upper() if "/" not in table else "EXPORT"
        print(f"  [{prefix:6s}] {table:<45s} {count:>8,}")
    print(f"\n  Total pipeline time: {_hms(time.time() - t_total)}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Well Production Analytics Pipeline")
    parser.add_argument(
        "--stage",
        choices=["all", "download", "bronze", "silver", "gold", "forecast", "export", "synthetic"],
        default="all",
        help="Pipeline stage to run (default: all)",
    )
    args = parser.parse_args()
    main(stage=args.stage)
