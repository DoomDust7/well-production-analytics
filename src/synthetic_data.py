"""
Generates synthetic well-level monthly production time-series data.
Uses Arps Hyperbolic decline model seeded from real well/operator names.
Produces a pandas DataFrame saved to data/raw/synthetic_well_production.csv.
"""
from __future__ import annotations
import os
from typing import Optional, List
import numpy as np
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

from src.spark_session import RAW_DIR

# Reproducible seed
RNG = np.random.default_rng(42)

SHALE_PLAYS = ["Bakken Shale, US", "Permian Basin, US", "Eagle Ford, US"]
BASINS = ["Williston", "Permian", "Gulf Coast"]

# Operators mirroring the real flaring.csv dataset (extended for variety)
DEFAULT_OPERATORS = [
    "XTO ENERGY INC", "HESS BAKKEN INVESTMENTS II", "BURLINGTON RESOURCES",
    "QEP ENERGY COMPANY", "CONTINENTAL RESOURCES", "WHITING PETROLEUM",
    "CONOCO PHILLIPS", "EXXONMOBIL", "OASIS PETROLEUM", "MARATHON OIL",
    "PETRO HUNT", "KODIAK OIL & GAS", "STATOIL OIL & GAS", "EMERALD BAY ENERGY",
    "PRIMA EXPLORATION", "PETRO SHALE", "DENBURY RESOURCES", "ENERPLUS RESOURCES",
]

SHALE_BY_OPERATOR = {op: SHALE_PLAYS[i % len(SHALE_PLAYS)] for i, op in enumerate(DEFAULT_OPERATORS)}
BASIN_BY_OPERATOR = {op: BASINS[i % len(BASINS)] for i, op in enumerate(DEFAULT_OPERATORS)}


def arps_hyperbolic(t: np.ndarray, qi: float, di: float, b: float) -> np.ndarray:
    """Arps hyperbolic decline: q(t) = Qi / (1 + b*Di*t)^(1/b)"""
    return qi / np.power(1.0 + b * di * t, 1.0 / b)


def generate_well_production(
    wells_df: Optional[pd.DataFrame] = None,
    operators: Optional[List[str]] = None,
    n_wells: int = 300,
    start_date: date = date(2018, 1, 1),
    n_months: int = 72,
) -> pd.DataFrame:
    """
    Generate monthly oil + gas production for n_wells from start_date over n_months.

    If wells_df is provided, real well names and API numbers are used;
    otherwise synthetic names are generated.
    """
    if operators is None:
        operators = DEFAULT_OPERATORS

    months = [start_date + relativedelta(months=i) for i in range(n_months)]
    t = np.arange(n_months, dtype=float)

    # Build well roster
    if wells_df is not None and len(wells_df) > 0:
        sample_n = min(n_wells, len(wells_df))
        well_sample = wells_df.sample(n=sample_n, random_state=42).reset_index(drop=True)
        api_numbers = well_sample["API Number"].tolist()
        well_names = well_sample["Well Name"].tolist()
    else:
        api_numbers = [f"33-053-{10000 + i:05d}-0000" for i in range(n_wells)]
        well_names = [f"SYNTHETIC WELL {i+1:04d}H" for i in range(n_wells)]
        sample_n = n_wells

    records = []
    for idx in range(sample_n):
        api = api_numbers[idx]
        name = well_names[idx]
        operator = operators[idx % len(operators)]
        shale = SHALE_BY_OPERATOR.get(operator, SHALE_PLAYS[0])
        basin = BASIN_BY_OPERATOR.get(operator, BASINS[0])

        # Arps parameters — oil
        qi_oil = float(np.exp(RNG.normal(6.2, 0.5)))  # ~490 bbl/month initial
        di_oil = float(RNG.uniform(0.03, 0.15))
        b_oil = float(RNG.uniform(0.3, 0.8))

        # Arps parameters — gas (higher Qi, similar decline)
        qi_gas = qi_oil * float(RNG.uniform(3.0, 6.0))
        di_gas = di_oil * float(RNG.uniform(0.8, 1.2))
        b_gas = float(RNG.uniform(0.3, 0.9))

        oil_profile = arps_hyperbolic(t, qi_oil, di_oil, b_oil)
        gas_profile = arps_hyperbolic(t, qi_gas, di_gas, b_gas)

        # Add noise
        noise_o = RNG.normal(1.0, 0.05, size=n_months)
        noise_g = RNG.normal(1.0, 0.05, size=n_months)
        oil_profile = np.maximum(oil_profile * noise_o, 0)
        gas_profile = np.maximum(gas_profile * noise_g, 0)

        # Random workover for 10% of wells (production bump at a random month)
        if RNG.random() < 0.10:
            bump_month = int(RNG.integers(12, n_months - 12))
            oil_profile[bump_month:bump_month + 6] *= float(RNG.uniform(1.3, 1.8))
            gas_profile[bump_month:bump_month + 6] *= float(RNG.uniform(1.2, 1.5))

        for m_idx, month in enumerate(months):
            # Oil record
            records.append({
                "api_number": api,
                "well_name": name,
                "operator": operator,
                "production_month": month.strftime("%Y-%m-01"),
                "oil_and_gas_group": "O",
                "production": round(float(oil_profile[m_idx]), 2),
                "shale_play": shale,
                "basin": basin,
            })
            # Gas record
            records.append({
                "api_number": api,
                "well_name": name,
                "operator": operator,
                "production_month": month.strftime("%Y-%m-01"),
                "oil_and_gas_group": "G",
                "production": round(float(gas_profile[m_idx]), 2),
                "shale_play": shale,
                "basin": basin,
            })

    df = pd.DataFrame(records)
    return df


def save_synthetic_data(
    wells_csv_path: Optional[str] = None,
    n_wells: int = 300,
) -> str:
    """
    Generate and save synthetic production to data/raw/synthetic_well_production.csv.
    Returns the saved file path.
    """
    out_path = os.path.join(RAW_DIR, "synthetic_well_production.csv")

    wells_df = None
    if wells_csv_path and os.path.exists(wells_csv_path):
        try:
            wells_df = pd.read_csv(wells_csv_path)
        except Exception:
            pass

    df = generate_well_production(wells_df=wells_df, n_wells=n_wells)
    df.to_csv(out_path, index=False)
    print(f"  [GEN]  synthetic_well_production.csv — {len(df):,} rows, {df['api_number'].nunique()} wells")
    return out_path
