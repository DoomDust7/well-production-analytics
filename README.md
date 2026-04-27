# Well Production Forecasting & Performance Analytics

> **Shell UnextGen Hackathon — Sept / Oct 2023**
> A production-grade oil & gas analytics platform built on PySpark, Delta Lake, and Arps decline curve theory — powered by **real Bakken / Williston Basin production data** and mirroring enterprise Databricks pipelines used in the energy industry.

## 🚀 Live Dashboard

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://oilgas-well-analytics.streamlit.app)

**👉 [https://oilgas-well-analytics.streamlit.app](https://oilgas-well-analytics.streamlit.app)**

5 interactive pages — Overview · Production Trends · Flaring & ESG · Forecasting (Arps DCA) · Well Economics

---

## Table of Contents
- [Live Dashboard](#-live-dashboard)
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
- [Dashboard Pages](#dashboard-pages)
- [Gold Table Schemas](#gold-table-schemas)
- [Key Design Decisions](#key-design-decisions)
- [References](#references)

---

## Overview

This project implements a **full-stack data engineering and analytics pipeline** for upstream oil & gas production data sourced from the real **Bakken / Williston Basin, North Dakota**. It was built during the Shell UnextGen Hackathon to demonstrate how a modern data lakehouse architecture powers both operational KPI dashboards and strategic production forecasting.

**Four core capabilities:**

| Capability | What it does |
|-----------|-------------|
| **Real Data Ingestion** | Downloads 10 production CSVs from the [Wells-Dataset](https://github.com/DoomDust7/Wells-Dataset) — 800K+ rows of real per-well production, flaring, water, EUR, IP rates, economics, and well header attributes |
| **Data Pipeline** | Ingests into partitioned Delta Lake tables using a Bronze → Silver → Gold Medallion architecture with schema enforcement, null filtering, deduplication, and operator normalization |
| **Analytics KPIs** | Computes business-critical metrics across 9 Gold tables: operator rankings, basin trends (MoM/YoY), ESG flaring intensity, EUR categories, IP benchmarks, breakeven/IRR/NPV economics, three-stream production, and water cut |
| **Production Forecasting** | Fits Arps decline curves (Exponential, Hyperbolic, Harmonic) to real Bakken well histories and projects output 24 months forward at well, operator, and basin levels |

The entire pipeline is **Databricks-compatible** — every module runs on a local SparkSession or can be imported directly into a Databricks workspace.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│                                                                  │
│   Wells-Dataset (GitHub / DoomDust7)  — 10 CSVs, ~69 MB         │
│   ├─ WELLHEADER.csv        (well geometry, economics, formation) │
│   ├─ PRODUCTION.csv        (monthly oil + gas per well) ~45 MB   │
│   ├─ PRODUCTIONFLARING.csv (monthly flared gas per well) ~12 MB  │
│   ├─ WATERPRODUCTION.csv   (monthly water per well)    ~9 MB     │
│   ├─ WELLEUR.csv / EUR.csv (EUR estimates per well)              │
│   ├─ INITIALPRODUCTION.csv (IP30/IP90/IP180/IP365)               │
│   ├─ ECONOMICSCOST.csv     (well cost breakdown by category)     │
│   ├─ PRICES.csv            (monthly WTI / Brent / WCS)           │
│   └─ OPERATOR.csv          (operator ticker + public/private)    │
└───────────────────────────┬──────────────────────────────────────┘
                             │  chunked streaming for large files
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Delta)                         │
│                                                                  │
│  • Raw ingestion — no business transformations                   │
│  • All values stored as StringType (preserves raw fidelity)      │
│  • Metadata: load_timestamp, source_file, data_source            │
│  • North Dakota API filter: prefix "33" (Bakken scope)           │
│  • bronze_production_real partitioned by year                    │
│  • 14 tables | ~800K+ total rows                                 │
└───────────────────────────┬──────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (Delta)                         │
│                                                                  │
│  • Type casting: String → Date, Double, Integer                  │
│  • production_month constructed from integer Year + Month        │
│  • Operator resolved via first_reported_operator (WELLHEADER)    │
│    + join to silver_wellheader for all production records        │
│  • EUR categorisation: Low / Medium / High (BOE thresholds)      │
│  • Flaring intensity: flared_gas_mcf / gross_gas_production_mcf  │
│  • Deduplication on business keys per table                      │
│  • silver_production_real partitioned by production_year         │
│  • 11 tables | ~489K real production records                     │
└───────────────────────────┬──────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER (Delta)                          │
│                                                                  │
│  gold_operator_performance     ← operator rankings + ESG         │
│  gold_basin_production_trends  ← MoM/YoY growth rates           │
│  gold_flaring_intensity        ← ESG flaring KPIs               │
│  gold_ethane_dry_gas           ← liquid-rich vs dry gas          │
│  gold_well_summary             ← per-well lifetime metrics       │
│  gold_well_economics           ← breakeven / IRR / NPV / EUR     │
│  gold_ip_benchmarks            ← IP30 P10/P50/P90 by formation   │
│  gold_flaring_timeseries       ← monthly flaring per operator    │
│  gold_three_stream_production  ← oil + gas + water + GOR         │
│                                                                  │
│  9 tables | optimized for BI & ad-hoc SQL                        │
└───────────────────────────┬──────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                   FORECASTING (Arps DCA)                         │
│                                                                  │
│  Fit: scipy.optimize.curve_fit on real well decline profiles     │
│  Models: Exponential | Hyperbolic | Harmonic                     │
│  Selection: best R² score per entity                             │
│                                                                  │
│  gold_production_forecast                                        │
│  ├─ Well level    (top wells, 24-month forward)                   │
│  ├─ Operator level (40 operators, 24-month forward)              │
│  └─ Basin level   (Bakken/Williston, 24-month forward)           │
│                                                                  │
│  3,432 rows | fitted on real Bakken decline curves               │
└──────────────────────────────────────────────────────────────────┘
```

---

## Dataset

All data is sourced from the [DoomDust7/Wells-Dataset](https://github.com/DoomDust7/Wells-Dataset) repository and downloaded automatically at pipeline runtime. The dataset covers **Bakken / Williston Basin, North Dakota** wells.

| File | Size | Description |
|------|------|-------------|
| `PRODUCTION.csv` | ~45 MB | Monthly oil & gas production per well (API number, year, month, commodity type) |
| `PRODUCTIONFLARING.csv` | ~12 MB | Monthly flared gas and gross gas production per well |
| `WATERPRODUCTION.csv` | ~9 MB | Monthly water production and days on production per well |
| `WELLHEADER.csv` | ~3 MB | 228-column well header: lateral length, TVD, spud/completion dates, formation, proppant, breakeven price, IRR, NPV |
| `WELLEUR.csv` | ~0.5 MB | Estimated Ultimate Recovery per well by commodity group |
| `EUR.csv` | ~0.5 MB | Secondary EUR source for coalescing |
| `INITIALPRODUCTION.csv` | ~0.5 MB | IP30, IP90, IP180, IP365 rates per well |
| `ECONOMICSCOST.csv` | ~0.5 MB | Well costs by category (MUSD) |
| `PRICES.csv` | ~0.1 MB | Monthly WTI, Brent, and WCS commodity prices |
| `OPERATOR.csv` | ~0.1 MB | Operator ticker symbols and public/private classification |

**Large files** (`PRODUCTION.csv`, `WATERPRODUCTION.csv`, `PRODUCTIONFLARING.csv`) are downloaded using chunked HTTP streaming (8 MB chunks) to avoid memory exhaustion.

**North Dakota filter:** Only API numbers beginning with `"33"` (the ND state prefix) are retained — this scopes the pipeline to the Bakken/Williston Basin and eliminates out-of-state records.

---

## Project Structure

```
well-production-analytics/
│
├── data/
│   ├── raw/                             # Downloaded CSVs (10 files, ~69 MB)
│   ├── delta/
│   │   ├── bronze/                      # 14 Bronze Delta tables
│   │   ├── silver/                      # 11 Silver Delta tables
│   │   └── gold/                        # 10 Gold Delta tables (incl. forecast)
│   └── sample/                          # 11 pre-exported CSVs for Streamlit Cloud
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformation.ipynb
│   ├── 03_gold_analytics.ipynb
│   └── 04_forecasting.ipynb
│
├── src/
│   ├── __init__.py
│   ├── spark_session.py                 # SparkSession factory + path helpers
│   ├── schema_definitions.py            # All StructType schemas (Bronze/Silver/Gold)
│   ├── data_downloader.py               # Chunked CSV downloader (10 files)
│   ├── synthetic_data.py                # Legacy Arps-based generator (--stage synthetic)
│   ├── bronze_ingestion.py              # 14 Bronze Delta write functions
│   ├── silver_transform.py              # 11 Silver cleaning + enrichment functions
│   ├── gold_analytics.py                # 9 Gold KPI builders
│   ├── forecasting.py                   # Arps DCA fitting + forecast generation
│   └── sample_exporter.py               # Exports data/sample/*.csv for Streamlit Cloud
│
├── Technical_Report.docx                # Full technical design document
├── requirements.txt
├── README.md
└── run_pipeline.py                      # CLI orchestrator with stage flags
```

---

## Setup & Installation

### Prerequisites
- **Python 3.9+**
- **Java 8, 11, or 17** (required by Apache Spark)

```bash
java -version
```

### Install Dependencies

```bash
git clone https://github.com/DoomDust7/well-production-analytics.git
cd well-production-analytics

python -m venv venv && source venv/bin/activate   # macOS/Linux
# python -m venv venv && venv\Scripts\activate    # Windows

pip install -r requirements.txt
```

> **Note:** `delta-spark==3.1.0` is specifically paired with `pyspark==3.5.0`. Using a different version combination will cause classpath errors.

---

## Running the Pipeline

### Full Pipeline
```bash
python run_pipeline.py
```
Downloads all 10 CSVs → Bronze → Silver → Gold → Forecast → Export sample CSVs.

### Individual Stages
```bash
python run_pipeline.py --stage download   # Download 10 CSVs from Wells-Dataset
python run_pipeline.py --stage bronze     # Ingest into Bronze Delta tables
python run_pipeline.py --stage silver     # Bronze → Silver transformation
python run_pipeline.py --stage gold       # Silver → Gold KPI tables
python run_pipeline.py --stage forecast   # Fit Arps curves + write forecast table
python run_pipeline.py --stage export     # Write data/sample/*.csv for Streamlit Cloud
python run_pipeline.py --stage synthetic  # (legacy) generate synthetic data only
```

### Expected Output (real data)
```
=================================================================
  Well Production Forecasting & Performance Analytics System
=================================================================

[DATA] Downloading source CSVs from Wells-Dataset (GitHub)...
  [SKIP] WELLHEADER.csv already present
  [DOWN] PRODUCTION.csv (large file, streaming)...  45.2 MB
  ...

[BRONZE] Ingesting raw data into Delta tables...
  bronze_production_real      : 812,443 rows
  bronze_wellheader           :   2,133 rows
  bronze_production_flaring   : 153,700 rows
  ...

[SILVER] Transforming Bronze -> Silver Delta tables...
  silver_production_real      : 489,124 rows
  silver_wellheader           :   2,132 rows
  silver_production_flaring   : 153,700 rows
  ...

[GOLD] Computing analytics KPI tables...
  gold_operator_performance   :      40 rows
  gold_well_summary           :   2,132 rows
  gold_well_economics         :   2,132 rows
  gold_ip_benchmarks          :   2,129 rows
  gold_flaring_timeseries     : 153,700 rows
  gold_three_stream_production: 246,551 rows
  ...

[FORECAST] Running Arps decline curve analysis (real wells)...
  gold_production_forecast    :   3,432 rows

[EXPORT] Writing data/sample/*.csv ...
  11 files written to data/sample/
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
| `data_source` | StringType | `doomdust7_wells_dataset` |

All business columns are stored as **StringType** — type casting happens in Silver.

Column names with spaces and special characters are sanitized to `snake_case` via:
```python
re.sub(r"[ ,;{}()\n\t=]+", "_", col_name).strip("_").lower()
```

`bronze_production_real` is partitioned by `year` for efficient time-based pruning. An early API number filter (`startswith("33")`) limits ingestion to North Dakota wells before writing to Delta.

---

### Silver — Cleansed & Conformed

**Goal:** Produce a single source of truth, strongly typed and ready for analytics.

**Data quality operations per table:**

| Operation | Detail |
|-----------|--------|
| Date construction | `production_month = to_date(concat(year, "-", lpad(month, 2, "0"), "-01"))` |
| Numeric casting | Strip commas via `regexp_replace`, then cast to `DoubleType` |
| Operator resolution | `first_reported_operator` from WELLHEADER → normalized via `TRIM(UPPER())` → joined to all production records |
| EUR categorization | Low (<100K BOE) / Medium (100K–500K) / High (>500K) assigned in `silver_well_eur` |
| Flaring intensity | `flared_gas_mcf / gross_gas_production_mcf` — null-safe division |
| Deduplication | Business key per table (e.g., `(api_number, production_month, oil_and_gas_group)` for production) |
| Derived columns | `production_year` (Int), `production_quarter` (Q1–Q4) |

`silver_production_real` is partitioned by `production_year`. All downstream Gold builders check for this table first and fall back to the legacy `silver_production` (synthetic) if not present — ensuring backward compatibility.

---

### Gold — Analytics KPIs

**Goal:** Pre-aggregated, BI-optimized tables directly queryable via SQL or Streamlit.

#### `gold_operator_performance`
Operator-level aggregation of production, well counts, and flaring. Includes `ticker` and `public_private` status from `silver_operator_enriched`.

| Column | Definition |
|--------|-----------|
| `total_oil_bbl` | Sum of all oil production |
| `total_gas_mcf` | Sum of all gas production |
| `well_count` | `COUNT(DISTINCT api_number)` |
| `flaring_intensity_ratio` | `total_flaring_mcf / total_gas_mcf` |
| `production_rank` | `RANK() OVER (ORDER BY total_oil_bbl DESC)` |

#### `gold_basin_production_trends`
Monthly production by basin with MoM and YoY growth metrics using `LAG(1)` and `LAG(12)` window functions.

#### `gold_well_summary`
Per-well lifetime metrics including `cumulative_oil_bbl`, `cumulative_gas_mcf`, `peak_oil_month`, `active_months`, `formation`, `lateral_length_ft`, `eur`, `completion_date`.

#### `gold_well_economics`
Joins well summary + wellheader economics (breakeven, IRR, NPV) + EUR + WTI price to classify each well:
- **Economic** — WTI > breakeven
- **Marginal** — within 20% of breakeven
- **Uneconomic** — WTI < breakeven

#### `gold_ip_benchmarks`
IP30/IP90/IP180/IP365 normalized per 1,000 ft of lateral length. P10/P50/P90 percentiles computed per formation using `percentile_approx` window functions. Performance tier (Top / Mid / Bottom) assigned vs formation peers.

#### `gold_flaring_timeseries`
Monthly flaring per well joined with operator + basin. Enables ESG trend tracking over time.

#### `gold_three_stream_production`
Oil + gas + water joined on `(api_number, production_month)`. Computes:
- `water_cut_pct = water_production_bbl / (oil_bbl + water_production_bbl)`
- `gor = gas_mcf / oil_bbl` (gas-oil ratio)

---

## Forecasting Methodology

### Arps Decline Curve Analysis (DCA)

Production from oil and gas wells follows predictable decline patterns described by the **Arps equations** (Arps, J.J., 1945). Three models are evaluated for each entity:

#### Exponential Decline (b = 0)
```
q(t) = Qi × exp(−Di × t)
```
Assumes constant percentage decline. Conservative — common for constant-pressure boundary-dominated flow.

#### Hyperbolic Decline (0 < b < 1)
```
q(t) = Qi / (1 + b × Di × t)^(1/b)
```
Industry-standard for shale wells. The `b` exponent captures how the decline rate itself decelerates — transient flow in tight formations creates a concave-up production curve that exponential models underfit.

#### Harmonic Decline (b = 1)
```
q(t) = Qi / (1 + Di × t)
```
The most optimistic case — appropriate for gravity-drainage mechanisms.

### Parameter Fitting

For each entity (well, operator, basin), `scipy.optimize.curve_fit` minimizes least-squares error against historical monthly production. Bounds keep parameters physically meaningful:
- `Qi`: `[0, 10 × initial_production]`
- `Di`: `[1e-6, 5.0]` per month
- `b`: `[0.001, 0.999]`

The model with the highest **R²** is selected and written to `gold_production_forecast`.

### Forecast Generation

From the last historical data point, production is projected **24 months forward** using the fitted parameters. The forecasting module prefers `silver_production_real` (real Bakken wells) when present, falling back to synthetic data otherwise.

---

## Dashboard Pages

The Streamlit dashboard at [oilgas-well-analytics.streamlit.app](https://oilgas-well-analytics.streamlit.app) has 5 pages:

| Page | Key Content |
|------|------------|
| **🏠 Overview** | Portfolio KPIs: 2,132 wells · 40 operators · 698.1M BBL cumulative oil · 327K median EUR. Operator bubble chart, production mix bar, well summary table with formation + lateral length |
| **📈 Production Trends** | Monthly and YoY basin production trends, operator comparison bar charts, MoM growth heatmap |
| **🔥 Flaring & ESG** | Flaring intensity by operator (Low/Medium/High), monthly flaring trend line from real PRODUCTIONFLARING data, per-well intensity ratios |
| **🔮 Forecasting** | Arps DCA curves fitted to real well histories, 24-month forward projections at well/operator/basin levels, R² distribution, model selection breakdown |
| **💰 Well Economics** | Avg breakeven $68.2/BBL · 50% economic wells · Breakeven histogram by formation · Risk-return scatter (breakeven vs IRR) · IP30 P50 benchmarks by formation · Water cut ranking |

The dashboard loads from `data/sample/*.csv` on Streamlit Cloud (no Spark required) and falls back to live Delta Lake reads when running locally.

---

## Gold Table Schemas

### `gold_operator_performance`
| Column | Type |
|--------|------|
| operator | String |
| ticker | String |
| public_private | String |
| total_oil_bbl | Double |
| total_gas_mcf | Double |
| well_count | Integer |
| total_flaring_mcf | Double |
| flaring_intensity_ratio | Double |
| avg_monthly_production | Double |
| production_rank | Integer |

### `gold_well_economics`
| Column | Type |
|--------|------|
| api_number | String |
| operator | String |
| formation | String |
| cumulative_oil_bbl | Double |
| avg_wti_price | Double |
| breakeven_oil_price | Double |
| irr | Double |
| npv | Double |
| eur | Double |
| total_well_cost_musd | Double |
| cumulative_revenue_usd | Double |
| economics_category | String |

### `gold_ip_benchmarks`
| Column | Type |
|--------|------|
| api_number | String |
| formation | String |
| ip30 / ip90 / ip180 / ip365 | Double |
| ip30_per_1000ft | Double |
| lateral_length_ft | Double |
| p50_ip30_formation | Double |
| performance_tier | String |

### `gold_production_forecast`
| Column | Type |
|--------|------|
| entity_type | String (`well` / `operator` / `basin`) |
| entity_id | String |
| forecast_month | Date |
| forecast_oil_bbl | Double |
| forecast_gas_mcf | Double |
| model_type | String (`exponential` / `hyperbolic` / `harmonic`) |
| r2_score | Double |
| qi | Double |
| di | Double |
| b_factor | Double |

---

## Key Design Decisions

**Why Delta Lake instead of raw Parquet?**
ACID transactions, schema enforcement, `MERGE` support, and time-travel. This enables incremental monthly data loads and historical audits — both critical in production oil & gas pipelines.

**Why Bronze/Silver/Gold instead of one big transformation?**
Each layer has a distinct contract. Bronze is immutable and auditable — Silver can always be re-derived if transformation logic changes. Gold is compute-once, query-many. This also matches Databricks enterprise pipeline patterns, making the code directly portable.

**Why filter to API prefix "33" (North Dakota) at Bronze ingestion?**
The Wells-Dataset includes wells from multiple states. Scoping to ND keeps the Bakken / Williston Basin analysis coherent and avoids mixing geology, regulations, and price environments across basins.

**Why resolve operator names from WELLHEADER rather than the production table?**
The PRODUCTION.csv operator field is inconsistent. `first_reported_operator` in WELLHEADER.csv is the authoritative, normalized source — joining silver_wellheader to silver_production_real gives clean operator attribution for all 489K production records.

**Why Arps DCA instead of ML-based forecasting?**
Arps decline curves are the industry standard in reservoir engineering. For reserve estimation and strategic planning, regulators and finance teams trust physically-grounded models. The Hyperbolic model captures transient flow behavior in shale wells — something black-box ML often overfits. Running on real Bakken data confirms meaningful R² scores.

---

## References

- Arps, J.J. (1945). *Analysis of Decline Curves.* Trans. AIME, 160, 228–247.
- North Dakota Industrial Commission — Bakken Shale production data
- DoomDust7/Wells-Dataset — [https://github.com/DoomDust7/Wells-Dataset](https://github.com/DoomDust7/Wells-Dataset)
- Shell UnextGen Hackathon Case Study (Sept–Oct 2023)
- Apache Spark Delta Lake documentation — [https://docs.delta.io](https://docs.delta.io)
