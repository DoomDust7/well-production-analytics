# Well Production Forecasting & Performance Analytics

> **Shell UnextGen Hackathon — Sept / Oct 2023**
> A production-grade oil & gas analytics platform built on PySpark, Delta Lake, and Arps decline curve theory — mirroring enterprise Databricks pipelines used in the energy industry.

---

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Medallion Layers in Detail](#medallion-layers-in-detail)
  - [Bronze — Raw Ingestion](#bronze--raw-ingestion)
  - [Silver — Cleansed & Conformed](#silver--cleansed--conformed)
  - [Gold — Analytics KPIs](#gold--analytics-kpis)
- [Forecasting Methodology](#forecasting-methodology)
- [Jupyter Notebooks](#jupyter-notebooks)
- [Gold Table Schemas](#gold-table-schemas)
- [Sample Outputs](#sample-outputs)
- [Key Design Decisions](#key-design-decisions)

---

## Overview

This project implements a **full-stack data engineering and analytics pipeline** for upstream oil & gas production data. It was built during the Shell UnextGen Hackathon to demonstrate how a modern data lakehouse architecture can power both operational KPI dashboards and strategic production forecasting.

**Three core capabilities:**

| Capability | What it does |
|-----------|-------------|
| **Data Pipeline** | Ingests raw well, production, and flaring data into partitioned Delta Lake tables using a Bronze → Silver → Gold Medallion architecture with schema enforcement and data validation |
| **Analytics KPIs** | Computes business-critical metrics: operator performance rankings, basin production trends (MoM/YoY), flaring intensity ESG scores, ethane vs dry gas ratios |
| **Production Forecasting** | Fits Arps decline curves (Exponential, Hyperbolic, Harmonic) to historical well data and projects output 24 months forward at well, operator, and basin levels |

The entire pipeline is **Databricks-compatible** — every notebook and module runs on a local Spark session or can be imported directly into a Databricks workspace.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                          │
│                                                              │
│  GitHub CSVs (Shell Hackathon)   Synthetic Time-Series       │
│  ├─ flaring.csv                  (Arps Hyperbolic Model)     │
│  ├─ operator_well_counts.csv      300 wells × 72 months      │
│  ├─ shale_play_production.csv     Jan 2018 – Dec 2023        │
│  ├─ well_metadata.csv                                        │
│  └─ chemical_production.csv                                  │
└─────────────────────────┬────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Delta)                      │
│                                                              │
│  • Raw ingestion — no business transformations               │
│  • All values stored as StringType (preserves raw fidelity)  │
│  • Metadata appended: load_timestamp, source_file,           │
│    data_source                                               │
│  • bronze_well_production partitioned by ingest_year         │
│  • 5 tables | ~45,400 total rows                             │
└─────────────────────────┬────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Delta)                      │
│                                                              │
│  • Type casting: String → Date, Double, Integer              │
│  • Column standardization: snake_case naming                 │
│  • Null filtering on key fields (api_number, operator,       │
│    production_month)                                         │
│  • Deduplication on business keys                            │
│  • Derived columns: production_year, production_quarter      │
│  • silver_production enriched via LEFT JOIN on api_number    │
│    (adds well_length_ft from well metadata)                  │
│  • Partitioned by production_year                            │
│  • 3 tables | ~45,400 total rows                             │
└─────────────────────────┬────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Delta)                       │
│                                                              │
│  gold_operator_performance     ← operator rankings           │
│  gold_basin_production_trends  ← MoM/YoY growth rates       │
│  gold_flaring_intensity        ← ESG flaring KPIs           │
│  gold_ethane_dry_gas           ← liquid-rich vs dry gas      │
│  gold_well_summary             ← per-well lifetime metrics   │
│                                                              │
│  5 tables | ~880 rows — optimized for BI & ad-hoc SQL        │
└─────────────────────────┬────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                  FORECASTING (Arps DCA)                      │
│                                                              │
│  Fit: scipy.optimize.curve_fit on historical monthly data    │
│  Models: Exponential | Hyperbolic | Harmonic                 │
│  Selection: best R² score per entity                         │
│                                                              │
│  gold_production_forecast                                    │
│  ├─ Well level  (top 100 wells, 24-month forward)            │
│  ├─ Operator level  (18 operators, 24-month forward)         │
│  └─ Basin level  (3 basins, 24-month forward)                │
│                                                              │
│  1 table | 2,904 rows | avg R² = 0.99                        │
└──────────────────────────────────────────────────────────────┘
```

---

## Dataset

### Real Data (Shell Hackathon GitHub)
All real data is sourced from the [Shell Final Case Study repository](https://github.com/DoomDust7/Shell-Final-CaseStudy) and downloaded automatically at pipeline runtime.

| File | Rows | Description |
|------|------|-------------|
| `FLARING.csv` | 26 | Operator names + total flared gas volumes (MCf) — Bakken Basin |
| `Operator with highest well counts.csv` | 28 | Unique well counts per reported operator |
| `shale play 3.csv` | 57 | Monthly total production by shale play + oil/gas type with rankings |
| `average well length (2).csv` | 2,133 | Well names, API numbers, and lateral length proxy (SquareRootSum) |
| `Question 6th.csv` | ~9,000 | Chemical ingredient usage per well per month, includes production + operator |

### Synthetic Time-Series Data
Generated using `src/synthetic_data.py` to provide a realistic 72-month production history needed for decline curve analysis.

- **300 wells** seeded from real API numbers and well names in `well_metadata.csv`
- **72 months** of production: January 2018 – December 2023
- **Arps Hyperbolic model** generates the base decline profile per well:
  - Initial production `Qi ~ LogNormal(μ=6.2, σ=0.5)` bbl/month
  - Decline rate `Di ~ Uniform(0.03, 0.15)` per month
  - Hyperbolic exponent `b ~ Uniform(0.3, 0.8)`
- **5% Gaussian noise** added to simulate real-world variance
- **10% of wells** receive a random "workover" production bump (1.3×–1.8× for 6 months)
- Assigned to real operators from `flaring.csv` and distributed across 3 shale plays: Bakken, Permian, Eagle Ford

---

## Project Structure

```
well-production-analytics/
│
├── data/
│   ├── raw/                            # Downloaded + generated CSVs
│   │   ├── flaring.csv
│   │   ├── operator_well_counts.csv
│   │   ├── shale_play_production.csv
│   │   ├── well_metadata.csv
│   │   ├── chemical_production.csv
│   │   └── synthetic_well_production.csv
│   ├── delta/
│   │   ├── bronze/                     # 5 Bronze Delta tables
│   │   ├── silver/                     # 3 Silver Delta tables
│   │   └── gold/                       # 6 Gold Delta tables
│   └── plots/                          # Generated chart PNGs
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb       # Data download + Bronze layer
│   ├── 02_silver_transformation.ipynb  # Cleaning, typing, quality checks
│   ├── 03_gold_analytics.ipynb         # KPI tables + 5 visualizations
│   └── 04_forecasting.ipynb            # Decline curves + 4 forecast charts
│
├── src/
│   ├── __init__.py
│   ├── spark_session.py                # SparkSession factory + path helpers
│   ├── schema_definitions.py           # All StructType schemas (Bronze/Silver/Gold)
│   ├── data_downloader.py              # GitHub CSV download utility
│   ├── synthetic_data.py               # Arps-based time-series generator
│   ├── bronze_ingestion.py             # Bronze Delta write functions
│   ├── silver_transform.py             # Silver cleaning + join logic
│   ├── gold_analytics.py               # Gold KPI computation (5 tables)
│   └── forecasting.py                  # Arps DCA fitting + forecast generation
│
├── requirements.txt
├── README.md
└── run_pipeline.py                     # CLI orchestrator: Bronze→Silver→Gold→Forecast
```

---

## Setup & Installation

### Prerequisites
- **Python 3.9+**
- **Java 8, 11, or 17** (required by Apache Spark)

```bash
# Verify Java is available
java -version
```

### Install Dependencies

```bash
# Clone the repo
git clone https://github.com/DoomDust7/well-production-analytics.git
cd well-production-analytics

# Optional: create a virtual environment
python -m venv venv && source venv/bin/activate   # macOS/Linux
# python -m venv venv && venv\Scripts\activate    # Windows

# Install all dependencies
pip install -r requirements.txt
```

**`requirements.txt`:**
```
pyspark==3.5.0
delta-spark==3.1.0
numpy>=2.0.0
pandas>=2.3.0
scipy>=1.13.0
matplotlib>=3.8.0
seaborn>=0.13.0
jupyter>=1.0.0
notebook>=7.0.0
requests>=2.31.0
```

> **Note:** `delta-spark==3.1.0` is specifically paired with `pyspark==3.5.0`. Using a different version combination may cause classpath errors.

---

## Running the Pipeline

### Full Pipeline (recommended)
```bash
python run_pipeline.py
```
Runs all four stages in sequence: data download → Bronze → Silver → Gold → Forecast.

### Individual Stages
```bash
python run_pipeline.py --stage bronze    # Download CSVs + generate synthetic data + Bronze ingest
python run_pipeline.py --stage silver    # Bronze → Silver transformation
python run_pipeline.py --stage gold      # Silver → Gold KPI tables
python run_pipeline.py --stage forecast  # Fit Arps curves + write forecast table
```

### Expected Output
```
=================================================================
  Well Production Forecasting & Performance Analytics System
=================================================================

[DATA] Downloading source CSVs from GitHub...
  [DOWN] flaring.csv <- https://raw.githubusercontent.com/...
  [DOWN] well_metadata.csv <- ...
  ...

[DATA] Generating synthetic well production time-series...
  [GEN]  synthetic_well_production.csv — 43,200 rows, 300 wells

[BRONZE] Ingesting raw data into Delta tables...
  bronze_flaring                :     26 rows
  bronze_operator_well_counts   :     28 rows
  bronze_shale_production       :     57 rows
  bronze_well_metadata          :  2,133 rows
  bronze_well_production        : 43,200 rows
  [TIME] Bronze completed in 00:00:08

[SILVER] Transforming Bronze -> Silver Delta tables...
  silver_wells       :  2,133 rows
  silver_flaring     :     25 rows
  silver_production  : 43,200 rows
  [TIME] Silver completed in 00:00:04

[GOLD] Computing analytics KPI tables...
  gold_operator_performance      :     18 rows
  gold_basin_production_trends   :    432 rows
  gold_flaring_intensity         :     25 rows
  gold_ethane_dry_gas            :    108 rows
  gold_well_summary              :    300 rows
  [TIME] Gold completed in 00:00:05

[FORECAST] Running Arps decline curve analysis...
  Well-level forecasts  :  2,400 rows (100 wells)
  Operator-level        :    432 rows (18 operators)
  Basin-level           :     72 rows (3 basins)
  gold_production_forecast:  2,904 rows total
  [TIME] Forecasting completed in 00:00:02
```

---

## Medallion Layers in Detail

### Bronze — Raw Ingestion

**Goal:** Land raw data exactly as received, with zero business transformations.

Every table gets three metadata columns appended:

| Column | Type | Value |
|--------|------|-------|
| `load_timestamp` | TimestampType | UTC time of ingestion |
| `source_file` | StringType | Original filename |
| `data_source` | StringType | `shell_hackathon_github` or `synthetic_arps_model` |

All business columns are stored as **StringType** — this is intentional. Bronze is a faithful record of what arrived; type casting happens in Silver.

`bronze_well_production` is partitioned by `ingest_year` to support efficient time-based pruning as the dataset grows.

Column names with spaces (e.g., `"Well Name"`, `"API Number"`) are sanitized to `snake_case` before writing, as Delta Lake prohibits spaces in column names.

---

### Silver — Cleansed & Conformed

**Goal:** Produce a single source of truth, strongly typed and ready for analytics.

**Data quality operations applied:**

| Operation | Detail |
|-----------|--------|
| Null filtering | Drop rows where `api_number`, `operator`, or `production_month` is null |
| Type casting | `production_month` → `DateType`, `production` → `DoubleType`, `UniqueOperatorCount` → `IntegerType` |
| Deduplication | `dropDuplicates(["api_number", "production_month", "oil_and_gas_group"])` |
| Operator normalization | `TRIM(UPPER(operator))` for consistent join keys |
| Derived columns | `production_year` (IntegerType), `production_quarter` (Q1–Q4) |

**Enrichment join:**

`silver_production` is LEFT JOINed with `silver_wells` on `api_number` to attach `well_name` and `well_length_ft` to every production record. This denormalization enables well-level analytics without a runtime join in the Gold layer.

`silver_production` is partitioned by `production_year` — this enables partition pruning in Gold queries that filter by year (e.g., YoY growth calculations).

---

### Gold — Analytics KPIs

**Goal:** Pre-aggregated, BI-optimized tables directly queryable via SQL or Power BI.

#### `gold_operator_performance`
Aggregates all production activity and flaring data at the operator level.

```sql
SELECT operator, total_oil_bbl, total_gas_mcf, well_count,
       total_flaring_mcf, flaring_intensity_ratio, production_rank
FROM gold_operator_performance
ORDER BY production_rank
```

| Column | Definition |
|--------|-----------|
| `total_oil_bbl` | Sum of all oil production across all wells and months |
| `total_gas_mcf` | Sum of all gas production |
| `well_count` | `COUNT(DISTINCT api_number)` |
| `flaring_intensity_ratio` | `total_flaring_mcf / total_gas_mcf` — key ESG metric |
| `production_rank` | `RANK() OVER (ORDER BY total_oil_bbl DESC)` |

#### `gold_basin_production_trends`
Monthly production aggregated by shale play and commodity type with growth metrics.

| Column | Definition |
|--------|-----------|
| `mom_growth_pct` | Month-over-month production growth % using `LAG(1)` window |
| `yoy_growth_pct` | Year-over-year growth % using `LAG(12)` window |
| `production_rank` | Rank of each shale play for that month and commodity |

#### `gold_flaring_intensity`
ESG-focused table ranking operators by flaring behavior.

| `flaring_category` | Threshold |
|-------------------|-----------|
| Low | < 5% of gas production |
| Medium | 5% – 15% |
| High | > 15% |

#### `gold_ethane_dry_gas`
Pivots oil vs gas production per operator per year to compute liquid-rich / dry gas ratios — key for understanding the hydrocarbon mix and commodity price exposure.

| `basin_category` | Ethane/Dry Gas Ratio |
|-----------------|---------------------|
| Liquid-Rich | > 0.30 |
| Dry Gas | ≤ 0.30 |

#### `gold_well_summary`
Per-well lifetime production metrics for asset performance analysis.

Captures: `first_production_month`, `last_production_month`, `cumulative_oil_bbl`, `cumulative_gas_mcf`, `peak_oil_month`, `peak_oil_production`, `active_months`.

---

## Forecasting Methodology

### Arps Decline Curve Analysis (DCA)

Production from oil and gas wells follows predictable decline patterns described by the **Arps equations** (Arps, J.J., 1945). Three models are evaluated for each entity:

#### Exponential Decline (b = 0)
```
q(t) = Qi × exp(−Di × t)
```
Assumes constant percentage decline. Conservative — common in tight-rock reservoirs with constant-pressure depletion.

#### Hyperbolic Decline (0 < b < 1)
```
q(t) = Qi / (1 + b × Di × t)^(1/b)
```
The industry-standard model for shale wells. The `b` exponent captures the rate at which the decline itself is decelerating — transient flow in tight formations creates a concave-up production curve that exponential models underfit.

#### Harmonic Decline (b = 1)
```
q(t) = Qi / (1 + Di × t)
```
The most optimistic case. Appropriate for gravity-drainage mechanisms.

### Parameter Fitting

For each entity (well, operator, basin), `scipy.optimize.curve_fit` is used to minimize least-squares error against historical monthly production data. Bounds are applied to keep parameters physically meaningful:
- `Qi`: `[0, 10 × initial_production]`
- `Di`: `[1e-6, 5.0]` per month
- `b`: `[0.001, 0.999]`

The model with the highest **R² score** is selected and written to `gold_production_forecast`.

### Forecast Generation

From the last historical data point (December 2023), production is projected **24 months forward** (through December 2025) using the fitted parameters.

**Coverage:**
- **Well level:** Top 100 wells by cumulative oil production → 2,400 forecast rows
- **Operator level:** All 18 operators → 432 forecast rows
- **Basin level:** 3 shale plays → 72 forecast rows

**Results (on synthetic data):**

| Level | Entities | Avg R² | Winning Model |
|-------|---------|--------|--------------|
| Well | 100 | 0.9918 | Hyperbolic (100%) |
| Operator | 18 | 0.9995 | Hyperbolic (100%) |
| Basin | 3 | 0.9999 | Hyperbolic (100%) |

---

## Jupyter Notebooks

Run locally with:
```bash
cd notebooks
jupyter notebook
```

Or import as Databricks notebooks — each notebook uses `configure_spark_with_delta_pip` which works identically in both environments.

| Notebook | Cells | Key Content |
|----------|-------|-------------|
| `01_bronze_ingestion.ipynb` | 5 | CSV download, synthetic data generation, Bronze Delta writes, schema validation |
| `02_silver_transformation.ipynb` | 5 | Data quality report (null counts before/after), type casting, ad-hoc SQL exploration |
| `03_gold_analytics.ipynb` | 7 | All 5 Gold tables + 5 visualizations (bar charts, line charts, pie) |
| `04_forecasting.ipynb` | 5 | Arps curve fitting on sample wells, basin historical vs forecast with confidence bands, R² distribution, model selection breakdown |

### Generated Visualizations

| Plot | Description |
|------|-------------|
| `01_top10_operators_oil.png` | Horizontal bar — top operators ranked by cumulative oil production |
| `02_basin_production_trends.png` | Multi-line chart — monthly oil production by shale play (2018–2023) |
| `03_flaring_intensity.png` | Color-coded horizontal bar — flaring intensity with Low/Medium/High thresholds |
| `04_ethane_vs_dry_gas.png` | Stacked bar — liquid-rich vs dry gas production split per operator |
| `05_well_count_by_basin.png` | Pie chart — well distribution across Bakken, Permian, Eagle Ford |
| `06_decline_curve_wells.png` | Scatter + line — Arps fit and 24-month forecast for top 3 wells |
| `07_basin_forecast.png` | Historical vs forecast line with ±15% confidence band per basin |
| `08_model_quality.png` | R² histogram + model selection bar chart |
| `09_operator_forecast.png` | Horizontal bar — 24-month projected oil production by operator |

---

## Gold Table Schemas

### `gold_operator_performance`
| Column | Type | Nullable |
|--------|------|----------|
| operator | String | No |
| total_oil_bbl | Double | Yes |
| total_gas_mcf | Double | Yes |
| well_count | Integer | Yes |
| total_flaring_mcf | Double | Yes |
| flaring_intensity_ratio | Double | Yes |
| avg_monthly_production | Double | Yes |
| production_rank | Integer | Yes |

### `gold_basin_production_trends`
| Column | Type | Nullable |
|--------|------|----------|
| shale_play | String | No |
| production_month | Date | No |
| oil_and_gas_group | String | No |
| total_production | Double | Yes |
| prev_month_production | Double | Yes |
| mom_growth_pct | Double | Yes |
| yoy_growth_pct | Double | Yes |
| production_rank | Integer | Yes |

### `gold_production_forecast`
| Column | Type | Nullable |
|--------|------|----------|
| entity_type | String | No — `well`, `operator`, or `basin` |
| entity_id | String | No — API number / name |
| forecast_month | Date | No |
| forecast_oil_bbl | Double | Yes |
| forecast_gas_mcf | Double | Yes |
| model_type | String | Yes — `exponential`, `hyperbolic`, `harmonic` |
| r2_score | Double | Yes |
| qi | Double | Yes |
| di | Double | Yes |
| b_factor | Double | Yes |

---

## Key Design Decisions

**Why Delta Lake instead of raw Parquet?**
Delta Lake provides ACID transactions, schema enforcement, `MERGE` support, and time-travel. This makes it possible to do incremental loads (append new monthly data without re-ingesting everything) and audit historical states — both critical in production oil & gas pipelines.

**Why separate Bronze/Silver/Gold instead of one big transformation?**
Each layer has a distinct contract. Bronze is immutable and auditable — you can always re-derive Silver from it if transformation logic changes. Silver is the single source of truth for business logic. Gold is compute-once, query-many. This separation also matches how Databricks-based enterprise pipelines are structured, making the code directly portable.

**Why synthetic data for the time-series?**
The real hackathon data did not include a well-level monthly production history in a format suitable for statistical curve-fitting. Synthetic data generated with realistic Arps parameters gives a 72-month panel dataset that enables meaningful decline curve analysis while preserving the real operator names, API numbers, and shale play assignments from the hackathon dataset.

**Why Arps DCA instead of ML-based forecasting?**
Arps decline curves are the industry standard in reservoir engineering. For strategic planning and reserve estimation, regulators and finance teams trust physically-grounded models. The Hyperbolic model in particular captures transient flow behavior in shale wells — something black-box ML models often overfit. R² > 0.99 on aggregated data confirms the model fits well.

---

## References

- Arps, J.J. (1945). *Analysis of Decline Curves.* Trans. AIME, 160, 228–247.
- North Dakota Industrial Commission — Bakken Shale production data
- Shell UnextGen Hackathon Case Study (Sept–Oct 2023)
- Apache Spark Delta Lake documentation: https://docs.delta.io
