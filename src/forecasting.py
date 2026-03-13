"""
Arps Decline Curve Analysis — Production Forecasting Module.

Three decline model types:
  - Exponential (b=0):  q(t) = Qi * exp(-Di * t)
  - Hyperbolic (0<b<1): q(t) = Qi / (1 + b*Di*t)^(1/b)
  - Harmonic (b=1):     q(t) = Qi / (1 + Di*t)

Best model selected by R² score.
Forecasts at well, operator, and basin levels.
"""
from __future__ import annotations
import warnings
from datetime import date
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from scipy.stats import pearsonr

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType,
)

from src.spark_session import silver_path, gold_path


# ── Arps model functions ──────────────────────────────────────────────────────

def _exp(t, qi, di):
    return qi * np.exp(-di * t)


def _hyp(t, qi, di, b):
    return qi / np.power(np.maximum(1.0 + b * di * t, 1e-9), 1.0 / b)


def _har(t, qi, di):
    return qi / (1.0 + di * t)


def _r2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    return float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0


def fit_arps(t: np.ndarray, q: np.ndarray) -> dict:
    """
    Fit Exponential, Hyperbolic, and Harmonic decline curves.
    Returns the best-fitting model's parameters + R² score.
    """
    t = t.astype(float)
    q = np.maximum(q.astype(float), 1e-3)
    qi0 = float(q[0])
    di0 = 0.05
    b0 = 0.5

    results = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # Exponential
        try:
            popt, _ = curve_fit(_exp, t, q,
                                p0=[qi0, di0],
                                bounds=([0, 1e-6], [qi0 * 10, 5.0]),
                                maxfev=5000)
            r2 = _r2(q, _exp(t, *popt))
            results["exponential"] = {"qi": popt[0], "di": popt[1], "b": 0.0, "r2": r2}
        except Exception:
            pass

        # Hyperbolic
        try:
            popt, _ = curve_fit(_hyp, t, q,
                                p0=[qi0, di0, b0],
                                bounds=([0, 1e-6, 0.001], [qi0 * 10, 5.0, 0.999]),
                                maxfev=5000)
            r2 = _r2(q, _hyp(t, *popt))
            results["hyperbolic"] = {"qi": popt[0], "di": popt[1], "b": popt[2], "r2": r2}
        except Exception:
            pass

        # Harmonic
        try:
            popt, _ = curve_fit(_har, t, q,
                                p0=[qi0, di0],
                                bounds=([0, 1e-6], [qi0 * 10, 5.0]),
                                maxfev=5000)
            r2 = _r2(q, _har(t, *popt))
            results["harmonic"] = {"qi": popt[0], "di": popt[1], "b": 1.0, "r2": r2}
        except Exception:
            pass

    if not results:
        # Fallback: flat decline
        return {"model": "flat", "qi": qi0, "di": 0.0, "b": 0.0, "r2": 0.0}

    best_model = max(results, key=lambda k: results[k]["r2"])
    best = results[best_model]
    best["model"] = best_model
    return best


def project_forward(params: dict, last_t: int, n_months: int = 24) -> np.ndarray:
    """Project production from last_t+1 for n_months using fitted params."""
    t_future = np.arange(last_t + 1, last_t + 1 + n_months, dtype=float)
    model = params.get("model", "exponential")
    qi, di, b = params["qi"], params["di"], params["b"]

    if model == "exponential":
        return _exp(t_future, qi, di)
    elif model == "harmonic":
        return _har(t_future, qi, di)
    else:  # hyperbolic or fallback
        if b == 0:
            return _exp(t_future, qi, di)
        return _hyp(t_future, qi, di, b)


# ── Forecast runners ─────────────────────────────────────────────────────────

def _forecast_entity(
    entity_type: str,
    entity_id: str,
    monthly_df: pd.DataFrame,
    n_months: int = 24,
) -> list[dict]:
    """
    Fit Arps curves to oil + gas separately, generate n_months forecast.
    Returns a list of row dicts for the forecast table.
    """
    df = monthly_df.sort_values("production_month").reset_index(drop=True)
    if len(df) < 6:
        return []

    last_month = pd.to_datetime(df["production_month"].max())
    t = np.arange(len(df), dtype=float)
    last_t = float(len(df) - 1)

    oil_q = df["oil_production"].values if "oil_production" in df else np.zeros(len(df))
    gas_q = df["gas_production"].values if "gas_production" in df else np.zeros(len(df))

    oil_params = fit_arps(t, oil_q) if oil_q.max() > 0 else {"model": "flat", "qi": 0, "di": 0, "b": 0, "r2": 0}
    gas_params = fit_arps(t, gas_q) if gas_q.max() > 0 else {"model": "flat", "qi": 0, "di": 0, "b": 0, "r2": 0}

    oil_forecast = project_forward(oil_params, int(last_t), n_months)
    gas_forecast = project_forward(gas_params, int(last_t), n_months)

    # Use the dominant model (higher R²) for metadata columns
    dominant = oil_params if oil_params["r2"] >= gas_params["r2"] else gas_params

    rows = []
    for i in range(n_months):
        forecast_month = last_month + relativedelta(months=i + 1)
        rows.append({
            "entity_type": entity_type,
            "entity_id": str(entity_id),
            "forecast_month": forecast_month.date(),
            "forecast_oil_bbl": float(max(oil_forecast[i], 0)),
            "forecast_gas_mcf": float(max(gas_forecast[i], 0)),
            "model_type": dominant["model"],
            "r2_score": float(dominant["r2"]),
            "qi": float(dominant["qi"]),
            "di": float(dominant["di"]),
            "b_factor": float(dominant["b"]),
        })
    return rows


def run_forecasting(spark: SparkSession, n_months: int = 24) -> int:
    """
    Fit decline curves and generate forecasts at well, operator, and basin levels.
    Writes to gold_production_forecast Delta table.
    Returns total forecast rows written.
    """
    print("\n[FORECAST] Running Arps decline curve analysis...")

    prod = spark.read.format("delta").load(silver_path("silver_production"))

    # Pivot oil vs gas into columns for convenience
    monthly = (
        prod.groupBy("api_number", "well_name", "operator", "shale_play", "production_month")
        .pivot("oil_and_gas_group", ["O", "G"])
        .agg(F.sum("production"))
        .withColumnRenamed("O", "oil_production")
        .withColumnRenamed("G", "gas_production")
        .fillna(0, subset=["oil_production", "gas_production"])
    )

    # ── Well-level forecasts (top 100 wells by cumulative oil) ────────────────
    top_wells = (
        monthly.groupBy("api_number")
        .agg(F.sum("oil_production").alias("cum_oil"))
        .orderBy(F.col("cum_oil").desc())
        .limit(100)
        .select("api_number")
        .toPandas()["api_number"]
        .tolist()
    )

    well_pdf = (
        monthly.filter(F.col("api_number").isin(top_wells))
        .toPandas()
    )

    all_rows = []
    for api in top_wells:
        w = well_pdf[well_pdf["api_number"] == api]
        rows = _forecast_entity("well", api, w, n_months)
        all_rows.extend(rows)
    print(f"  Well-level forecasts  : {len(all_rows):>6,} rows ({len(top_wells)} wells)")

    # ── Operator-level forecasts ──────────────────────────────────────────────
    op_monthly = (
        monthly.groupBy("operator", "production_month")
        .agg(
            F.sum("oil_production").alias("oil_production"),
            F.sum("gas_production").alias("gas_production"),
        )
        .toPandas()
    )
    op_rows_start = len(all_rows)
    for op in op_monthly["operator"].unique():
        op_df = op_monthly[op_monthly["operator"] == op]
        rows = _forecast_entity("operator", op, op_df, n_months)
        all_rows.extend(rows)
    print(f"  Operator-level        : {len(all_rows) - op_rows_start:>6,} rows ({op_monthly['operator'].nunique()} operators)")

    # ── Basin-level forecasts ─────────────────────────────────────────────────
    basin_monthly = (
        monthly.groupBy("shale_play", "production_month")
        .agg(
            F.sum("oil_production").alias("oil_production"),
            F.sum("gas_production").alias("gas_production"),
        )
        .toPandas()
    )
    basin_rows_start = len(all_rows)
    for basin in basin_monthly["shale_play"].unique():
        b_df = basin_monthly[basin_monthly["shale_play"] == basin]
        rows = _forecast_entity("basin", basin, b_df, n_months)
        all_rows.extend(rows)
    print(f"  Basin-level           : {len(all_rows) - basin_rows_start:>6,} rows ({basin_monthly['shale_play'].nunique()} basins)")

    # ── Write to Delta ────────────────────────────────────────────────────────
    schema = StructType([
        StructField("entity_type",     StringType(), False),
        StructField("entity_id",       StringType(), False),
        StructField("forecast_month",  DateType(),   False),
        StructField("forecast_oil_bbl", DoubleType(), True),
        StructField("forecast_gas_mcf", DoubleType(), True),
        StructField("model_type",      StringType(), True),
        StructField("r2_score",        DoubleType(), True),
        StructField("qi",              DoubleType(), True),
        StructField("di",              DoubleType(), True),
        StructField("b_factor",        DoubleType(), True),
    ])

    forecast_pdf = pd.DataFrame(all_rows)
    forecast_pdf["forecast_month"] = pd.to_datetime(forecast_pdf["forecast_month"])

    forecast_sdf = spark.createDataFrame(forecast_pdf, schema=schema)
    (forecast_sdf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_production_forecast")))

    total = len(all_rows)
    print(f"  gold_production_forecast: {total:>6,} rows total")
    return total
