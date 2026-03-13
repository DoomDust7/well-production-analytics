"""
Silver Layer: Cleaned, validated, and joined Delta tables.
- Type casting & column standardization (snake_case)
- Null handling and deduplication
- Derived columns: production_year, production_quarter
- Partitioned by production_year
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import bronze_path, silver_path


def _quarter(col_name: str) -> F.Column:
    """Return Q1/Q2/Q3/Q4 string from a month column."""
    return (
        F.when(F.month(col_name).between(1, 3), "Q1")
         .when(F.month(col_name).between(4, 6), "Q2")
         .when(F.month(col_name).between(7, 9), "Q3")
         .otherwise("Q4")
    )


def build_silver_wells(spark: SparkSession) -> int:
    """
    silver_wells: clean well metadata — api_number, well_name, well_length_ft.
    Bronze columns are already snake_case from _sanitize_columns:
      api_number, well_name, squarerootsum
    """
    df = spark.read.format("delta").load(bronze_path("bronze_well_metadata"))
    df = (
        df
        .withColumn("well_length_ft", F.col("squarerootsum").cast("double"))
        .select("api_number", "well_name", "well_length_ft", "load_timestamp")
        .filter(F.col("api_number").isNotNull())
        .dropDuplicates(["api_number"])
    )
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_wells")))
    return df.count()


def build_silver_flaring(spark: SparkSession) -> int:
    """
    silver_flaring: standardized operator + flaring volumes.
    """
    df = spark.read.format("delta").load(bronze_path("bronze_flaring"))
    df = (
        df
        # Bronze columns: operator, totalflaredgas_mcf (after sanitization)
        .withColumn("operator", F.trim(F.upper(F.col("operator"))))
        .withColumn("total_flared_gas_mcf",
                    F.regexp_replace(F.col("totalflaredgas_mcf"), ",", "").cast("double"))
        .select("operator", "total_flared_gas_mcf", "load_timestamp")
        .filter(F.col("operator").isNotNull())
        .filter(F.col("total_flared_gas_mcf").isNotNull())
        .dropDuplicates(["operator"])
    )
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_flaring")))
    return df.count()


def build_silver_production(spark: SparkSession) -> int:
    """
    silver_production: clean + typed production records, joined with well metadata.
    Partitioned by production_year.
    """
    prod = spark.read.format("delta").load(bronze_path("bronze_well_production"))
    wells = spark.read.format("delta").load(silver_path("silver_wells"))

    # Cast and clean
    prod = (
        prod
        .withColumn("production",
                    F.col("production").cast("double"))
        .withColumn("production_month",
                    F.to_date(F.col("production_month"), "yyyy-MM-dd"))
        .withColumn("operator", F.trim(F.upper(F.col("operator"))))
        .filter(F.col("api_number").isNotNull())
        .filter(F.col("production_month").isNotNull())
        .filter(F.col("operator").isNotNull())
        .filter(F.col("production") >= 0)
        .dropDuplicates(["api_number", "production_month", "oil_and_gas_group"])
    )

    # Add derived time columns
    prod = (
        prod
        .withColumn("production_year", F.year(F.col("production_month")))
        .withColumn("production_quarter", _quarter("production_month"))
    )

    # Left join with well metadata to enrich with well_length_ft
    prod = (
        prod
        .join(
            wells.select("api_number",
                         F.col("well_name").alias("well_name_meta"),
                         "well_length_ft"),
            on="api_number",
            how="left",
        )
        .withColumn("well_name",
                    F.coalesce(F.col("well_name"), F.col("well_name_meta")))
        .drop("well_name_meta")
    )

    cols = [
        "api_number", "well_name", "operator",
        "production_month", "production_year", "production_quarter",
        "oil_and_gas_group", "production", "shale_play", "basin", "well_length_ft",
    ]
    prod = prod.select(cols)

    (prod.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("production_year")
        .save(silver_path("silver_production")))
    return prod.count()


def run_silver(spark: SparkSession) -> dict:
    """Run all Silver transformations. Returns {table: row_count}."""
    counts = {}
    print("\n[SILVER] Transforming Bronze -> Silver Delta tables...")

    n = build_silver_wells(spark)
    counts["silver_wells"] = n
    print(f"  silver_wells       : {n:>6,} rows")

    n = build_silver_flaring(spark)
    counts["silver_flaring"] = n
    print(f"  silver_flaring     : {n:>6,} rows")

    n = build_silver_production(spark)
    counts["silver_production"] = n
    print(f"  silver_production  : {n:>6,} rows")

    return counts
