"""
All Bronze, Silver, and Gold StructType schemas in one place.
Prevents schema drift across pipeline modules.
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    DateType, TimestampType,
)

# ── BRONZE SCHEMAS ────────────────────────────────────────────────────────────
# Raw ingestion: business columns as StringType (preserve raw), + metadata cols.

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
    StructField("api_number",       StringType(), True),
    StructField("well_name",        StringType(), True),
    StructField("operator",         StringType(), True),
    StructField("production_month", StringType(), True),
    StructField("oil_and_gas_group", StringType(), True),
    StructField("production",       StringType(), True),
    StructField("shale_play",       StringType(), True),
    StructField("basin",            StringType(), True),
    StructField("load_timestamp",   TimestampType(), False),
    StructField("source_file",      StringType(), False),
    StructField("data_source",      StringType(), False),
    StructField("ingest_year",      StringType(), False),
])

# ── SILVER SCHEMAS ────────────────────────────────────────────────────────────

SILVER_WELLS_SCHEMA = StructType([
    StructField("api_number",      StringType(),  False),
    StructField("well_name",       StringType(),  True),
    StructField("well_length_ft",  DoubleType(),  True),
    StructField("load_timestamp",  TimestampType(), False),
])

SILVER_PRODUCTION_SCHEMA = StructType([
    StructField("api_number",        StringType(),  False),
    StructField("well_name",         StringType(),  True),
    StructField("operator",          StringType(),  False),
    StructField("production_month",  DateType(),    False),
    StructField("production_year",   IntegerType(), False),
    StructField("production_quarter",StringType(),  False),
    StructField("oil_and_gas_group", StringType(),  False),
    StructField("production",        DoubleType(),  True),
    StructField("shale_play",        StringType(),  True),
    StructField("basin",             StringType(),  True),
    StructField("well_length_ft",    DoubleType(),  True),
])

SILVER_FLARING_SCHEMA = StructType([
    StructField("operator",            StringType(), False),
    StructField("total_flared_gas_mcf", DoubleType(), True),
    StructField("load_timestamp",      TimestampType(), False),
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
])

GOLD_PRODUCTION_FORECAST_SCHEMA = StructType([
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
