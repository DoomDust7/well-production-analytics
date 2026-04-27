"""
All Bronze, Silver, and Gold StructType schemas in one place.
Prevents schema drift across pipeline modules.
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    DateType, TimestampType,
)

# ── BRONZE SCHEMAS (legacy hackathon sources) ─────────────────────────────────

BRONZE_FLARING_SCHEMA = StructType([
    StructField("Operator",           StringType(), True),
    StructField("TotalFlaredGas_MCf", StringType(), True),
    StructField("load_timestamp",     TimestampType(), False),
    StructField("source_file",        StringType(), False),
    StructField("data_source",        StringType(), False),
])

BRONZE_OPERATOR_WELL_COUNTS_SCHEMA = StructType([
    StructField("Reported Operator",   StringType(), True),
    StructField("UniqueOperatorCount", StringType(), True),
    StructField("load_timestamp",      TimestampType(), False),
    StructField("source_file",         StringType(), False),
    StructField("data_source",         StringType(), False),
])

BRONZE_SHALE_PRODUCTION_SCHEMA = StructType([
    StructField("Shale Plays",       StringType(), True),
    StructField("Month",             StringType(), True),
    StructField("OilAndGasGroup",    StringType(), True),
    StructField("Total Production",  StringType(), True),
    StructField("Rank",              StringType(), True),
    StructField("load_timestamp",    TimestampType(), False),
    StructField("source_file",       StringType(), False),
    StructField("data_source",       StringType(), False),
])

BRONZE_WELL_METADATA_SCHEMA = StructType([
    StructField("SquareRootSum",  StringType(), True),
    StructField("Well Name",      StringType(), True),
    StructField("API Number",     StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
    StructField("source_file",    StringType(), False),
    StructField("data_source",    StringType(), False),
])

BRONZE_WELL_PRODUCTION_SCHEMA = StructType([
    StructField("api_number",        StringType(), True),
    StructField("well_name",         StringType(), True),
    StructField("operator",          StringType(), True),
    StructField("production_month",  StringType(), True),
    StructField("oil_and_gas_group", StringType(), True),
    StructField("production",        StringType(), True),
    StructField("shale_play",        StringType(), True),
    StructField("basin",             StringType(), True),
    StructField("load_timestamp",    TimestampType(), False),
    StructField("source_file",       StringType(), False),
    StructField("data_source",       StringType(), False),
    StructField("ingest_year",       StringType(), False),
])

# ── BRONZE SCHEMAS (Wells-Dataset real data) ──────────────────────────────────

BRONZE_WELLHEADER_SCHEMA = StructType([
    StructField("Id",                   StringType(), True),
    StructField("API Number",           StringType(), True),
    StructField("Well Name",            StringType(), True),
    StructField("Latitude",             StringType(), True),
    StructField("Longitude",            StringType(), True),
    StructField("Spud Date",            StringType(), True),
    StructField("Completion Date",      StringType(), True),
    StructField("Lateral Length",       StringType(), True),
    StructField("True Vertical Depth",  StringType(), True),
    StructField("Formation",            StringType(), True),
    StructField("Basin",                StringType(), True),
    StructField("Proppant Type",        StringType(), True),
    StructField("Proppant Intensity",   StringType(), True),
    StructField("Breakeven Oil Price",  StringType(), True),
    StructField("IRR",                  StringType(), True),
    StructField("NPV",                  StringType(), True),
    StructField("Operator",             StringType(), True),
    StructField("load_timestamp",       TimestampType(), False),
    StructField("source_file",          StringType(), False),
    StructField("data_source",          StringType(), False),
])

BRONZE_PRODUCTION_SCHEMA = StructType([
    StructField("Id",               StringType(), True),
    StructField("API Number",       StringType(), True),
    StructField("Year",             StringType(), True),
    StructField("Month",            StringType(), True),
    StructField("Production",       StringType(), True),
    StructField("OilAndGasGroup",   StringType(), True),
    StructField("load_timestamp",   TimestampType(), False),
    StructField("source_file",      StringType(), False),
    StructField("data_source",      StringType(), False),
])

BRONZE_PRODUCTION_FLARING_SCHEMA = StructType([
    StructField("Id",                       StringType(), True),
    StructField("API Number",               StringType(), True),
    StructField("Year",                     StringType(), True),
    StructField("Month",                    StringType(), True),
    StructField("FlaredGas_MCf",            StringType(), True),
    StructField("GrossGasProduction_MCf",   StringType(), True),
    StructField("load_timestamp",           TimestampType(), False),
    StructField("source_file",              StringType(), False),
    StructField("data_source",              StringType(), False),
])

BRONZE_WATER_PRODUCTION_SCHEMA = StructType([
    StructField("Id",               StringType(), True),
    StructField("API Number",       StringType(), True),
    StructField("Year",             StringType(), True),
    StructField("Month",            StringType(), True),
    StructField("WaterProduction",  StringType(), True),
    StructField("DaysOnProduction", StringType(), True),
    StructField("load_timestamp",   TimestampType(), False),
    StructField("source_file",      StringType(), False),
    StructField("data_source",      StringType(), False),
])

BRONZE_WELLEUR_SCHEMA = StructType([
    StructField("Id",                             StringType(), True),
    StructField("API Number",                     StringType(), True),
    StructField("Oil And Gas Group",              StringType(), True),
    StructField("Oil And Gas Category",           StringType(), True),
    StructField("EstimatedWellUltimateRecovery",  StringType(), True),
    StructField("load_timestamp",                 TimestampType(), False),
    StructField("source_file",                    StringType(), False),
    StructField("data_source",                    StringType(), False),
])

BRONZE_INITIALPRODUCTION_SCHEMA = StructType([
    StructField("API Number",             StringType(), True),
    StructField("Id",                     StringType(), True),
    StructField("Initial Production - 30 Days",       StringType(), True),
    StructField("Initial Production - 90 Days",       StringType(), True),
    StructField("Initial Production - Half-Year",     StringType(), True),
    StructField("Initial Production - Year",          StringType(), True),
    StructField("OilAndGasGroup",                     StringType(), True),
    StructField("load_timestamp",         TimestampType(), False),
    StructField("source_file",            StringType(), False),
    StructField("data_source",            StringType(), False),
])

BRONZE_ECONOMICSCOST_SCHEMA = StructType([
    StructField("Id",               StringType(), True),
    StructField("API Number",       StringType(), True),
    StructField("Well Cost Group",  StringType(), True),
    StructField("Well Cost",        StringType(), True),
    StructField("Well Cost Category", StringType(), True),
    StructField("Well Cost Detail", StringType(), True),
    StructField("load_timestamp",   TimestampType(), False),
    StructField("source_file",      StringType(), False),
    StructField("data_source",      StringType(), False),
])

BRONZE_EUR_SCHEMA = StructType([
    StructField("Id",           StringType(), True),
    StructField("API Number",   StringType(), True),
    StructField("EUR",          StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
    StructField("source_file",  StringType(), False),
    StructField("data_source",  StringType(), False),
])

BRONZE_PRICES_SCHEMA = StructType([
    StructField("Id",           StringType(), True),
    StructField("Year",         StringType(), True),
    StructField("Month",        StringType(), True),
    StructField("Brent",        StringType(), True),
    StructField("WCS",          StringType(), True),
    StructField("WTI Cushing",  StringType(), True),
    StructField("WTI Midland",  StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
    StructField("source_file",  StringType(), False),
    StructField("data_source",  StringType(), False),
])

BRONZE_OPERATOR_SCHEMA = StructType([
    StructField("Id",                   StringType(), True),
    StructField("Reported Operator",    StringType(), True),
    StructField("Operator",             StringType(), True),
    StructField("Bloomberg Ticker",     StringType(), True),
    StructField("Short Ticker",         StringType(), True),
    StructField("Public Private Company", StringType(), True),
    StructField("Private Equity Backer", StringType(), True),
    StructField("load_timestamp",       TimestampType(), False),
    StructField("source_file",          StringType(), False),
    StructField("data_source",          StringType(), False),
])

# ── SILVER SCHEMAS (legacy) ───────────────────────────────────────────────────

SILVER_WELLS_SCHEMA = StructType([
    StructField("api_number",      StringType(),  False),
    StructField("well_name",       StringType(),  True),
    StructField("well_length_ft",  DoubleType(),  True),
    StructField("load_timestamp",  TimestampType(), False),
])

SILVER_PRODUCTION_SCHEMA = StructType([
    StructField("api_number",         StringType(),  False),
    StructField("well_name",          StringType(),  True),
    StructField("operator",           StringType(),  False),
    StructField("production_month",   DateType(),    False),
    StructField("production_year",    IntegerType(), False),
    StructField("production_quarter", StringType(),  False),
    StructField("oil_and_gas_group",  StringType(),  False),
    StructField("production",         DoubleType(),  True),
    StructField("shale_play",         StringType(),  True),
    StructField("basin",              StringType(),  True),
    StructField("well_length_ft",     DoubleType(),  True),
])

SILVER_FLARING_SCHEMA = StructType([
    StructField("operator",             StringType(), False),
    StructField("total_flared_gas_mcf", DoubleType(), True),
    StructField("load_timestamp",       TimestampType(), False),
])

# ── SILVER SCHEMAS (Wells-Dataset real data) ──────────────────────────────────

SILVER_WELLHEADER_SCHEMA = StructType([
    StructField("api_number",           StringType(), False),
    StructField("well_name",            StringType(), True),
    StructField("operator",             StringType(), True),
    StructField("latitude",             DoubleType(), True),
    StructField("longitude",            DoubleType(), True),
    StructField("spud_date",            DateType(),   True),
    StructField("completion_date",      DateType(),   True),
    StructField("lateral_length_ft",    DoubleType(), True),
    StructField("tvd_ft",               DoubleType(), True),
    StructField("formation",            StringType(), True),
    StructField("basin",                StringType(), True),
    StructField("proppant_type",        StringType(), True),
    StructField("proppant_intensity",   DoubleType(), True),
    StructField("breakeven_oil_price",  DoubleType(), True),
    StructField("irr",                  DoubleType(), True),
    StructField("npv",                  DoubleType(), True),
])

SILVER_PRODUCTION_REAL_SCHEMA = StructType([
    StructField("api_number",         StringType(),  False),
    StructField("production_month",   DateType(),    False),
    StructField("production_year",    IntegerType(), False),
    StructField("production_quarter", StringType(),  False),
    StructField("oil_and_gas_group",  StringType(),  False),
    StructField("production",         DoubleType(),  True),
    StructField("operator",           StringType(),  True),
    StructField("basin",              StringType(),  True),
])

SILVER_PRODUCTION_FLARING_SCHEMA = StructType([
    StructField("api_number",               StringType(), False),
    StructField("production_month",         DateType(),   False),
    StructField("flared_gas_mcf",           DoubleType(), True),
    StructField("gross_gas_production_mcf", DoubleType(), True),
    StructField("flaring_intensity",        DoubleType(), True),
])

SILVER_WATER_PRODUCTION_SCHEMA = StructType([
    StructField("api_number",         StringType(), False),
    StructField("production_month",   DateType(),   False),
    StructField("water_production_bbl", DoubleType(), True),
    StructField("days_on_production", DoubleType(), True),
])

SILVER_WELL_EUR_SCHEMA = StructType([
    StructField("api_number",       StringType(), False),
    StructField("oil_and_gas_group", StringType(), True),
    StructField("eur",              DoubleType(), True),
    StructField("eur_category",     StringType(), True),
])

SILVER_INITIAL_PRODUCTION_SCHEMA = StructType([
    StructField("api_number",       StringType(), False),
    StructField("ip30",             DoubleType(), True),
    StructField("ip90",             DoubleType(), True),
    StructField("ip180",            DoubleType(), True),
    StructField("ip365",            DoubleType(), True),
    StructField("oil_and_gas_group", StringType(), True),
])

SILVER_OPERATOR_ENRICHED_SCHEMA = StructType([
    StructField("operator",         StringType(), False),
    StructField("ticker",           StringType(), True),
    StructField("public_private",   StringType(), True),
    StructField("pe_backer",        StringType(), True),
])

SILVER_PRICES_SCHEMA = StructType([
    StructField("price_month",  DateType(),   False),
    StructField("wti",          DoubleType(), True),
    StructField("brent",        DoubleType(), True),
    StructField("wcs",          DoubleType(), True),
])

SILVER_WELL_COSTS_SCHEMA = StructType([
    StructField("api_number",       StringType(), False),
    StructField("cost_category",    StringType(), True),
    StructField("well_cost_musd",   DoubleType(), True),
])

# ── GOLD SCHEMAS ──────────────────────────────────────────────────────────────

GOLD_OPERATOR_PERFORMANCE_SCHEMA = StructType([
    StructField("operator",                StringType(), False),
    StructField("total_oil_bbl",           DoubleType(),  True),
    StructField("total_gas_mcf",           DoubleType(),  True),
    StructField("well_count",              IntegerType(), True),
    StructField("total_flaring_mcf",       DoubleType(),  True),
    StructField("flaring_intensity_ratio", DoubleType(),  True),
    StructField("avg_monthly_production",  DoubleType(),  True),
    StructField("production_rank",         IntegerType(), True),
    StructField("ticker",                  StringType(),  True),
    StructField("public_private",          StringType(),  True),
])

GOLD_PRODUCTION_FORECAST_SCHEMA = StructType([
    StructField("entity_type",      StringType(), False),
    StructField("entity_id",        StringType(), False),
    StructField("forecast_month",   DateType(),   False),
    StructField("forecast_oil_bbl", DoubleType(), True),
    StructField("forecast_gas_mcf", DoubleType(), True),
    StructField("model_type",       StringType(), True),
    StructField("r2_score",         DoubleType(), True),
    StructField("qi",               DoubleType(), True),
    StructField("di",               DoubleType(), True),
    StructField("b_factor",         DoubleType(), True),
])

GOLD_WELL_ECONOMICS_SCHEMA = StructType([
    StructField("api_number",             StringType(), False),
    StructField("well_name",              StringType(), True),
    StructField("operator",               StringType(), True),
    StructField("formation",              StringType(), True),
    StructField("breakeven_oil_price",    DoubleType(), True),
    StructField("irr",                    DoubleType(), True),
    StructField("npv",                    DoubleType(), True),
    StructField("eur",                    DoubleType(), True),
    StructField("total_well_cost_musd",   DoubleType(), True),
    StructField("cumulative_oil_bbl",     DoubleType(), True),
    StructField("cumulative_revenue_usd", DoubleType(), True),
    StructField("avg_wti_price",          DoubleType(), True),
    StructField("water_cut_pct",          DoubleType(), True),
    StructField("economics_category",     StringType(), True),
])

GOLD_IP_BENCHMARKS_SCHEMA = StructType([
    StructField("api_number",            StringType(), False),
    StructField("well_name",             StringType(), True),
    StructField("operator",              StringType(), True),
    StructField("formation",             StringType(), True),
    StructField("lateral_length_ft",     DoubleType(), True),
    StructField("completion_date",       DateType(),   True),
    StructField("ip30",                  DoubleType(), True),
    StructField("ip90",                  DoubleType(), True),
    StructField("ip180",                 DoubleType(), True),
    StructField("ip365",                 DoubleType(), True),
    StructField("ip30_per_1000ft",       DoubleType(), True),
    StructField("formation_p50_ip30",    DoubleType(), True),
    StructField("performance_tier",      StringType(), True),
])

GOLD_FLARING_TIMESERIES_SCHEMA = StructType([
    StructField("api_number",              StringType(), False),
    StructField("operator",               StringType(), True),
    StructField("basin",                  StringType(), True),
    StructField("production_month",       DateType(),   False),
    StructField("flared_gas_mcf",         DoubleType(), True),
    StructField("gross_gas_mcf",          DoubleType(), True),
    StructField("flaring_intensity",      DoubleType(), True),
    StructField("cumulative_flared_mcf",  DoubleType(), True),
])

GOLD_THREE_STREAM_SCHEMA = StructType([
    StructField("api_number",         StringType(), False),
    StructField("production_month",   DateType(),   False),
    StructField("oil_production_bbl", DoubleType(), True),
    StructField("gas_production_mcf", DoubleType(), True),
    StructField("water_production_bbl", DoubleType(), True),
    StructField("water_cut_pct",      DoubleType(), True),
    StructField("gor_mcf_per_bbl",    DoubleType(), True),
    StructField("days_on_production", DoubleType(), True),
])
