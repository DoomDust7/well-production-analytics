"""
Gold Layer: Analytics-ready KPI tables.
All tables are optimized for BI queries and ad-hoc SQL exploration.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import silver_path, gold_path, bronze_path


def build_gold_operator_performance(spark: SparkSession) -> int:
    """
    gold_operator_performance:
    Total oil/gas production, well count, flaring volume, flaring intensity ratio,
    and production rank per operator.
    """
    prod = spark.read.format("delta").load(silver_path("silver_production"))
    flaring = spark.read.format("delta").load(silver_path("silver_flaring"))

    oil = (
        prod.filter(F.col("oil_and_gas_group") == "O")
        .groupBy("operator")
        .agg(
            F.sum("production").alias("total_oil_bbl"),
            F.countDistinct("api_number").alias("well_count"),
            F.avg("production").alias("avg_monthly_production"),
        )
    )
    gas = (
        prod.filter(F.col("oil_and_gas_group") == "G")
        .groupBy("operator")
        .agg(F.sum("production").alias("total_gas_mcf"))
    )

    # Step 1: join oil + gas (string "operator" key avoids ambiguity)
    oil_gas = oil.join(gas, on="operator", how="outer")

    # Step 2: join with flaring using SQL-style column comparison to avoid ambiguity
    flaring_sub = flaring.select(
        F.col("operator").alias("f_operator"),
        F.col("total_flared_gas_mcf").alias("total_flaring_mcf")
    )
    perf = (
        oil_gas.join(flaring_sub, oil_gas["operator"] == flaring_sub["f_operator"], how="left")
        .drop("f_operator")
        .withColumn(
            "flaring_intensity_ratio",
            F.when(F.col("total_gas_mcf") > 0,
                   F.col("total_flaring_mcf") / F.col("total_gas_mcf"))
             .otherwise(None)
        )
        .withColumn(
            "production_rank",
            F.rank().over(Window.orderBy(F.col("total_oil_bbl").desc()))
        )
    )

    (perf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_operator_performance")))
    return perf.count()


def build_gold_basin_production_trends(spark: SparkSession) -> int:
    """
    gold_basin_production_trends:
    Monthly total production per shale_play + oil_and_gas_group
    with MoM and YoY growth rates.
    """
    prod = spark.read.format("delta").load(silver_path("silver_production"))

    monthly = (
        prod.groupBy("shale_play", "production_month", "oil_and_gas_group")
        .agg(F.sum("production").alias("total_production"))
    )

    w_month = (
        Window.partitionBy("shale_play", "oil_and_gas_group")
              .orderBy("production_month")
    )
    w_year = (
        Window.partitionBy("shale_play", "oil_and_gas_group",
                           F.month("production_month"))
              .orderBy("production_month")
    )

    monthly = (
        monthly
        .withColumn("prev_month_production", F.lag("total_production", 1).over(w_month))
        .withColumn(
            "mom_growth_pct",
            F.when(F.col("prev_month_production") > 0,
                   (F.col("total_production") - F.col("prev_month_production"))
                   / F.col("prev_month_production") * 100)
             .otherwise(None)
        )
        .withColumn("prev_year_production", F.lag("total_production", 12).over(w_month))
        .withColumn(
            "yoy_growth_pct",
            F.when(F.col("prev_year_production") > 0,
                   (F.col("total_production") - F.col("prev_year_production"))
                   / F.col("prev_year_production") * 100)
             .otherwise(None)
        )
        .withColumn(
            "production_rank",
            F.rank().over(
                Window.partitionBy("production_month", "oil_and_gas_group")
                      .orderBy(F.col("total_production").desc())
            )
        )
        .drop("prev_year_production")
    )

    (monthly.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_basin_production_trends")))
    return monthly.count()


def build_gold_flaring_intensity(spark: SparkSession) -> int:
    """
    gold_flaring_intensity:
    Operator-level flaring intensity KPIs with categorical ranking.
    Low (<5%), Medium (5-15%), High (>15%)
    """
    flaring = spark.read.format("delta").load(silver_path("silver_flaring"))
    gas = (
        spark.read.format("delta").load(silver_path("silver_production"))
        .filter(F.col("oil_and_gas_group") == "G")
        .groupBy("operator")
        .agg(F.sum("production").alias("total_gas_production_mcf"))
    )

    intensity = (
        flaring.join(gas, on="operator", how="left")
        .withColumn(
            "flaring_intensity_ratio",
            F.when(F.col("total_gas_production_mcf") > 0,
                   F.col("total_flared_gas_mcf") / F.col("total_gas_production_mcf") * 100)
             .otherwise(None)
        )
        .withColumn(
            "flaring_category",
            F.when(F.col("flaring_intensity_ratio") < 5, "Low")
             .when(F.col("flaring_intensity_ratio").between(5, 15), "Medium")
             .otherwise("High")
        )
        .withColumn(
            "flaring_rank",
            F.rank().over(Window.orderBy(F.col("flaring_intensity_ratio").desc()))
        )
        .select(
            "operator", "total_flared_gas_mcf", "total_gas_production_mcf",
            "flaring_intensity_ratio", "flaring_rank", "flaring_category"
        )
    )

    (intensity.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_flaring_intensity")))
    return intensity.count()


def build_gold_ethane_dry_gas(spark: SparkSession) -> int:
    """
    gold_ethane_dry_gas:
    Ethane (liquid-rich, OilAndGasGroup=O) vs dry gas (OilAndGasGroup=G) ratios per operator/year.
    """
    prod = spark.read.format("delta").load(silver_path("silver_production"))

    pivot = (
        prod.groupBy("operator", "production_year", "shale_play")
        .pivot("oil_and_gas_group", ["O", "G"])
        .agg(F.sum("production"))
        .withColumnRenamed("O", "liquid_rich_production")
        .withColumnRenamed("G", "dry_gas_production")
    )

    pivot = (
        pivot
        .withColumn(
            "ethane_dry_gas_ratio",
            F.when(F.col("dry_gas_production") > 0,
                   F.col("liquid_rich_production") / F.col("dry_gas_production"))
             .otherwise(None)
        )
        .withColumn(
            "basin_category",
            F.when(F.col("ethane_dry_gas_ratio") > 0.3, "Liquid-Rich")
             .otherwise("Dry Gas")
        )
    )

    (pivot.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_ethane_dry_gas")))
    return pivot.count()


def build_gold_well_summary(spark: SparkSession) -> int:
    """
    gold_well_summary:
    Per-well cumulative production, peak month, active months.
    """
    prod = spark.read.format("delta").load(silver_path("silver_production"))

    oil = prod.filter(F.col("oil_and_gas_group") == "O")
    gas = prod.filter(F.col("oil_and_gas_group") == "G")

    oil_agg = (
        oil.groupBy("api_number", "well_name", "operator", "well_length_ft", "shale_play", "basin")
        .agg(
            F.min("production_month").alias("first_production_month"),
            F.max("production_month").alias("last_production_month"),
            F.sum("production").alias("cumulative_oil_bbl"),
            F.max("production").alias("peak_oil_production"),
            F.count("*").alias("active_months"),
        )
    )

    gas_agg = (
        gas.groupBy("api_number")
        .agg(F.sum("production").alias("cumulative_gas_mcf"))
    )

    # Find peak oil month via window
    w = Window.partitionBy("api_number").orderBy(F.col("production").desc())
    peak_month = (
        oil.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(F.col("api_number").alias("api_peak"),
                F.col("production_month").alias("peak_oil_month"))
    )

    summary = (
        oil_agg
        .join(gas_agg, on="api_number", how="left")
        .join(peak_month, oil_agg["api_number"] == peak_month["api_peak"], how="left")
        .drop("api_peak")
        .select(
            "api_number", "well_name", "operator",
            "first_production_month", "last_production_month",
            "cumulative_oil_bbl", "cumulative_gas_mcf",
            "peak_oil_month", "peak_oil_production",
            "well_length_ft", "active_months", "shale_play", "basin"
        )
    )

    (summary.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_well_summary")))
    return summary.count()


def run_gold(spark: SparkSession) -> dict:
    """Run all Gold KPI computations. Returns {table: row_count}."""
    counts = {}
    print("\n[GOLD] Computing analytics KPI tables...")

    n = build_gold_operator_performance(spark)
    counts["gold_operator_performance"] = n
    print(f"  gold_operator_performance      : {n:>6,} rows")

    n = build_gold_basin_production_trends(spark)
    counts["gold_basin_production_trends"] = n
    print(f"  gold_basin_production_trends   : {n:>6,} rows")

    n = build_gold_flaring_intensity(spark)
    counts["gold_flaring_intensity"] = n
    print(f"  gold_flaring_intensity         : {n:>6,} rows")

    n = build_gold_ethane_dry_gas(spark)
    counts["gold_ethane_dry_gas"] = n
    print(f"  gold_ethane_dry_gas            : {n:>6,} rows")

    n = build_gold_well_summary(spark)
    counts["gold_well_summary"] = n
    print(f"  gold_well_summary              : {n:>6,} rows")

    return counts
