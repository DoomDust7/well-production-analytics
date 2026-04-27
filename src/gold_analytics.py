"""
Gold Layer: Analytics-ready KPI tables.
All tables are optimized for BI queries and dashboard consumption.
Prefers real data (silver_production_real) over synthetic fallback.
"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import silver_path, gold_path, bronze_path


def _prod_source(spark: SparkSession) -> DataFrame:
    """Return the best available production Silver table."""
    real = silver_path("silver_production_real")
    if os.path.exists(real):
        df = spark.read.format("delta").load(real)
        # Ensure shale_play column exists (may not be in real data)
        if "shale_play" not in df.columns:
            df = df.withColumn("shale_play", F.coalesce(F.col("basin"), F.lit("Williston")))
        return df
    return spark.read.format("delta").load(silver_path("silver_production"))


# ── Existing Gold builders (updated to use real data) ────────────────────────

def build_gold_operator_performance(spark: SparkSession) -> int:
    """
    gold_operator_performance:
    Total oil/gas production, well count, flaring intensity, production rank.
    Enriched with operator company metadata when available.
    """
    prod = _prod_source(spark)

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

    oil_gas = oil.join(gas, on="operator", how="outer")

    # Flaring: prefer per-well real data, fall back to operator totals
    flaring_real_path = silver_path("silver_production_flaring_real")
    if os.path.exists(flaring_real_path):
        flr_df = spark.read.format("delta").load(flaring_real_path)
        # Join with wellheader to get operator per API
        hdr_path = silver_path("silver_wellheader")
        if os.path.exists(hdr_path):
            hdr = (spark.read.format("delta").load(hdr_path)
                   .select("api_number", F.col("operator").alias("flr_operator")))
            flr_df = flr_df.join(hdr, on="api_number", how="left")
            flr_agg = (flr_df
                       .groupBy("flr_operator")
                       .agg(F.sum("flared_gas_mcf").alias("total_flaring_mcf"))
                       .withColumnRenamed("flr_operator", "f_operator"))
        else:
            flr_agg = (flr_df
                       .groupBy("api_number")
                       .agg(F.sum("flared_gas_mcf").alias("total_flaring_mcf"))
                       .withColumnRenamed("api_number", "f_operator"))
    else:
        legacy_path = silver_path("silver_flaring")
        if os.path.exists(legacy_path):
            flr_agg = (spark.read.format("delta").load(legacy_path)
                       .select(
                           F.col("operator").alias("f_operator"),
                           F.col("total_flared_gas_mcf").alias("total_flaring_mcf"),
                       ))
        else:
            flr_agg = spark.createDataFrame([], "f_operator string, total_flaring_mcf double")

    perf = (
        oil_gas
        .join(flr_agg, oil_gas["operator"] == flr_agg["f_operator"], how="left")
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

    # Enrich with company metadata if available
    op_path = silver_path("silver_operator_enriched")
    if os.path.exists(op_path):
        op_meta = (spark.read.format("delta").load(op_path)
                   .select(
                       F.col("operator").alias("op_key"),
                       "ticker", "public_private",
                   ))
        perf = (
            perf.join(op_meta, perf["operator"] == op_meta["op_key"], how="left")
            .drop("op_key")
        )
    else:
        perf = (perf
                .withColumn("ticker", F.lit(None).cast("string"))
                .withColumn("public_private", F.lit(None).cast("string")))

    (perf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_operator_performance")))
    return perf.count()


def build_gold_basin_production_trends(spark: SparkSession) -> int:
    """
    gold_basin_production_trends:
    Monthly total production per shale_play + oil_and_gas_group with MoM/YoY growth.
    """
    prod = _prod_source(spark)

    monthly = (
        prod.groupBy("shale_play", "production_month", "oil_and_gas_group")
        .agg(F.sum("production").alias("total_production"))
    )

    w_month = (
        Window.partitionBy("shale_play", "oil_and_gas_group")
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
    Operator-level flaring intensity KPIs. Low (<5%), Medium (5-15%), High (>15%).
    Uses per-well flaring data when available.
    """
    prod = _prod_source(spark)
    gas = (
        prod.filter(F.col("oil_and_gas_group") == "G")
        .groupBy("operator")
        .agg(F.sum("production").alias("total_gas_production_mcf"))
    )

    flaring_real_path = silver_path("silver_production_flaring_real")
    if os.path.exists(flaring_real_path):
        flr = spark.read.format("delta").load(flaring_real_path)
        hdr_path = silver_path("silver_wellheader")
        if os.path.exists(hdr_path):
            hdr = (spark.read.format("delta").load(hdr_path)
                   .select("api_number", F.col("operator").alias("flr_operator")))
            flr = flr.join(hdr, on="api_number", how="left")
            flaring = (flr
                       .groupBy("flr_operator")
                       .agg(F.sum("flared_gas_mcf").alias("total_flared_gas_mcf"))
                       .withColumnRenamed("flr_operator", "operator"))
        else:
            flaring = (flr
                       .groupBy("api_number")
                       .agg(F.sum("flared_gas_mcf").alias("total_flared_gas_mcf"))
                       .withColumnRenamed("api_number", "operator"))
    else:
        legacy_path = silver_path("silver_flaring")
        if os.path.exists(legacy_path):
            flaring = spark.read.format("delta").load(legacy_path)
        else:
            return 0

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
    Liquid-rich (Oil) vs dry gas ratios per operator/year.
    """
    prod = _prod_source(spark)

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
    Enriched with wellheader attributes and EUR when available.
    """
    prod = _prod_source(spark)

    oil = prod.filter(F.col("oil_and_gas_group") == "O")
    gas = prod.filter(F.col("oil_and_gas_group") == "G")

    oil_agg = (
        oil.groupBy("api_number", "operator")
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

    w = Window.partitionBy("api_number").orderBy(F.col("production").desc())
    peak_month = (
        oil.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("api_number").alias("api_peak"),
            F.col("production_month").alias("peak_oil_month"),
        )
    )

    summary = (
        oil_agg
        .join(gas_agg, on="api_number", how="left")
        .join(peak_month, oil_agg["api_number"] == peak_month["api_peak"], how="left")
        .drop("api_peak")
    )

    # Enrich with wellheader attributes
    hdr_path = silver_path("silver_wellheader")
    if os.path.exists(hdr_path):
        hdr = (spark.read.format("delta").load(hdr_path)
               .select("api_number",
                       F.col("well_name").alias("hdr_well_name"),
                       "formation", "lateral_length_ft", "completion_date",
                       "proppant_type", "basin",
                       F.col("basin").alias("shale_play")))
        summary = (summary
                   .join(hdr, on="api_number", how="left")
                   .withColumn("well_name", F.col("hdr_well_name"))
                   .drop("hdr_well_name"))
    else:
        # Add missing columns when wellheader not available
        for c in ["well_name", "formation", "lateral_length_ft",
                  "completion_date", "proppant_type", "shale_play", "basin"]:
            if c not in summary.columns:
                summary = summary.withColumn(c, F.lit(None).cast("string"))

    # Enrich with EUR
    eur_path = silver_path("silver_well_eur")
    if os.path.exists(eur_path):
        eur = (spark.read.format("delta").load(eur_path)
               .select("api_number", "eur", "eur_category"))
        summary = summary.join(eur, on="api_number", how="left")
    else:
        summary = (summary
                   .withColumn("eur", F.lit(None).cast("double"))
                   .withColumn("eur_category", F.lit(None).cast("string")))

    # Enrich with cumulative water
    wat_path = silver_path("silver_water_production")
    if os.path.exists(wat_path):
        wat = (spark.read.format("delta").load(wat_path)
               .groupBy("api_number")
               .agg(F.sum("water_production_bbl").alias("cumulative_water_bbl")))
        summary = summary.join(wat, on="api_number", how="left")
    else:
        summary = summary.withColumn("cumulative_water_bbl", F.lit(None).cast("double"))

    (summary.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_well_summary")))
    return summary.count()


# ── New Gold builders ─────────────────────────────────────────────────────────

def build_gold_well_economics(spark: SparkSession) -> int:
    """
    gold_well_economics:
    Well-level economic analysis: breakeven, IRR, NPV, EUR, revenue vs cost.
    """
    hdr_path = silver_path("silver_wellheader")
    ws_path  = gold_path("gold_well_summary")
    if not os.path.exists(hdr_path) or not os.path.exists(ws_path):
        return 0

    summary = spark.read.format("delta").load(ws_path)
    hdr = (spark.read.format("delta").load(hdr_path)
           .select(
               "api_number",
               F.col("breakeven_oil_price").alias("hdr_breakeven"),
               F.col("irr").alias("hdr_irr"),
               F.col("npv").alias("hdr_npv"),
               F.col("formation").alias("hdr_formation"),
               F.col("lateral_length_ft").alias("hdr_lateral"),
           ))

    # Average WTI price across the dataset period
    prices_path = silver_path("silver_prices")
    if os.path.exists(prices_path):
        avg_wti = (spark.read.format("delta").load(prices_path)
                   .agg(F.avg("wti").alias("avg_wti_price"))
                   .collect()[0]["avg_wti_price"] or 65.0)
    else:
        avg_wti = 65.0

    # Total well cost per API
    cost_path = silver_path("silver_well_costs")
    if os.path.exists(cost_path):
        costs = (spark.read.format("delta").load(cost_path)
                 .groupBy("api_number")
                 .agg(F.sum("well_cost_musd").alias("total_well_cost_musd")))
    else:
        costs = spark.createDataFrame([], "api_number string, total_well_cost_musd double")

    # Water cut from water production
    water_cut = F.lit(None).cast("double")
    wat_path = silver_path("silver_water_production")
    if os.path.exists(wat_path):
        wat = (spark.read.format("delta").load(wat_path)
               .groupBy("api_number")
               .agg(F.sum("water_production_bbl").alias("total_water")))

    econ = (
        summary
        .join(hdr, on="api_number", how="left")
        .withColumn("breakeven_oil_price", F.col("hdr_breakeven"))
        .withColumn("irr", F.col("hdr_irr"))
        .withColumn("npv", F.col("hdr_npv"))
        .withColumn("formation", F.coalesce(F.col("hdr_formation"), F.lit(None).cast("string")))
        .drop("hdr_breakeven", "hdr_irr", "hdr_npv", "hdr_formation", "hdr_lateral")
        .join(costs, on="api_number", how="left")
    )

    if os.path.exists(wat_path):
        econ = (econ.join(wat, on="api_number", how="left")
                .withColumn(
                    "water_cut_pct",
                    F.when(
                        (F.col("cumulative_oil_bbl") + F.col("total_water")) > 0,
                        F.col("total_water") / (F.col("cumulative_oil_bbl") + F.col("total_water")) * 100,
                    ).otherwise(None)
                )
                .drop("total_water"))
    else:
        econ = econ.withColumn("water_cut_pct", F.lit(None).cast("double"))

    econ = (
        econ
        .withColumn("avg_wti_price", F.lit(avg_wti))
        .withColumn("cumulative_revenue_usd",
                    F.col("cumulative_oil_bbl") * F.lit(avg_wti))
        .withColumn(
            "economics_category",
            F.when(
                F.col("breakeven_oil_price").isNull(), "Unknown"
            ).when(
                F.col("avg_wti_price") >= F.col("breakeven_oil_price"), "Economic"
            ).when(
                F.col("avg_wti_price") >= F.col("breakeven_oil_price") * 0.8, "Marginal"
            ).otherwise("Uneconomic")
        )
        .select(
            "api_number", "well_name", "operator", "formation",
            "breakeven_oil_price", "irr", "npv", "eur",
            "total_well_cost_musd", "cumulative_oil_bbl",
            "cumulative_revenue_usd", "avg_wti_price",
            "water_cut_pct", "economics_category",
        )
    )

    (econ.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_well_economics")))
    return econ.count()


def build_gold_ip_benchmarks(spark: SparkSession) -> int:
    """
    gold_ip_benchmarks:
    IP30/90/180/365 per well with formation-normalized benchmarks and performance tiers.
    """
    ip_path  = silver_path("silver_initial_production")
    hdr_path = silver_path("silver_wellheader")
    if not os.path.exists(ip_path):
        return 0

    ip = spark.read.format("delta").load(ip_path)

    if os.path.exists(hdr_path):
        hdr = (spark.read.format("delta").load(hdr_path)
               .select("api_number", "well_name", "operator", "formation",
                       "lateral_length_ft", "completion_date"))
        df = ip.join(hdr, on="api_number", how="left")
    else:
        df = ip
        for c in ["well_name", "operator", "formation", "lateral_length_ft", "completion_date"]:
            df = df.withColumn(c, F.lit(None).cast("string"))

    df = df.withColumn(
        "ip30_per_1000ft",
        F.when(
            F.col("lateral_length_ft") > 0,
            F.col("ip30") / (F.col("lateral_length_ft") / 1000.0)
        ).otherwise(None)
    )

    # Formation-level P50 IP30
    w_form = Window.partitionBy("formation")
    df = df.withColumn(
        "formation_p50_ip30",
        F.percentile_approx("ip30", 0.5).over(w_form)
    )

    df = df.withColumn(
        "performance_tier",
        F.when(F.col("ip30") >= F.percentile_approx("ip30", 0.75).over(w_form), "Top")
         .when(F.col("ip30") >= F.percentile_approx("ip30", 0.25).over(w_form), "Mid")
         .otherwise("Bottom")
    )

    df = df.select(
        "api_number", "well_name", "operator", "formation",
        "lateral_length_ft", "completion_date",
        "ip30", "ip90", "ip180", "ip365",
        "ip30_per_1000ft", "formation_p50_ip30", "performance_tier",
    )

    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_ip_benchmarks")))
    return df.count()


def build_gold_flaring_timeseries(spark: SparkSession) -> int:
    """
    gold_flaring_timeseries:
    Per-well monthly flaring with operator/basin context and cumulative running total.
    """
    flr_path = silver_path("silver_production_flaring_real")
    if not os.path.exists(flr_path):
        return 0

    flr = spark.read.format("delta").load(flr_path)

    hdr_path = silver_path("silver_wellheader")
    if os.path.exists(hdr_path):
        hdr = (spark.read.format("delta").load(hdr_path)
               .select("api_number", "operator", "basin"))
        flr = flr.join(hdr, on="api_number", how="left")
    else:
        flr = (flr
               .withColumn("operator", F.lit(None).cast("string"))
               .withColumn("basin", F.lit("Williston").cast("string")))

    w_cum = Window.partitionBy("api_number").orderBy("production_month").rowsBetween(
        Window.unboundedPreceding, Window.currentRow)

    flr = (
        flr
        .withColumn("cumulative_flared_mcf", F.sum("flared_gas_mcf").over(w_cum))
        .select(
            "api_number", "operator", "basin", "production_month",
            "flared_gas_mcf", "gross_gas_production_mcf",
            "flaring_intensity", "cumulative_flared_mcf",
        )
    )

    (flr.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_flaring_timeseries")))
    return flr.count()


def build_gold_three_stream_production(spark: SparkSession) -> int:
    """
    gold_three_stream_production:
    Per-well monthly oil + gas + water with water cut and GOR metrics.
    """
    prod_path = silver_path("silver_production_real")
    wat_path  = silver_path("silver_water_production")
    if not os.path.exists(prod_path):
        return 0

    prod = spark.read.format("delta").load(prod_path)

    # Pivot oil and gas into separate columns
    oil = (prod.filter(F.col("oil_and_gas_group") == "O")
           .select("api_number", "production_month",
                   F.col("production").alias("oil_production_bbl")))
    gas = (prod.filter(F.col("oil_and_gas_group") == "G")
           .select("api_number", "production_month",
                   F.col("production").alias("gas_production_mcf")))

    three = oil.join(gas, on=["api_number", "production_month"], how="outer")

    if os.path.exists(wat_path):
        wat = spark.read.format("delta").load(wat_path)
        three = three.join(wat, on=["api_number", "production_month"], how="left")
    else:
        three = (three
                 .withColumn("water_production_bbl", F.lit(None).cast("double"))
                 .withColumn("days_on_production", F.lit(None).cast("double")))

    three = (
        three
        .withColumn(
            "water_cut_pct",
            F.when(
                (F.col("oil_production_bbl") + F.col("water_production_bbl")) > 0,
                F.col("water_production_bbl")
                / (F.col("oil_production_bbl") + F.col("water_production_bbl")) * 100,
            ).otherwise(None)
        )
        .withColumn(
            "gor_mcf_per_bbl",
            F.when(
                F.col("oil_production_bbl") > 0,
                F.col("gas_production_mcf") / F.col("oil_production_bbl"),
            ).otherwise(None)
        )
        .select(
            "api_number", "production_month",
            "oil_production_bbl", "gas_production_mcf",
            "water_production_bbl", "water_cut_pct",
            "gor_mcf_per_bbl", "days_on_production",
        )
    )

    (three.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path("gold_three_stream_production")))
    return three.count()


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_gold(spark: SparkSession) -> dict:
    """Run all Gold KPI computations. Returns {table: row_count}."""
    counts = {}
    print("\n[GOLD] Computing analytics KPI tables...")

    def _try(name, fn):
        try:
            n = fn(spark)
            counts[name] = n
            print(f"  {name:<40}: {n:>8,} rows")
        except Exception as e:
            print(f"  {name:<40}: SKIPPED ({e})")

    _try("gold_operator_performance",    build_gold_operator_performance)
    _try("gold_basin_production_trends", build_gold_basin_production_trends)
    _try("gold_flaring_intensity",       build_gold_flaring_intensity)
    _try("gold_ethane_dry_gas",          build_gold_ethane_dry_gas)
    _try("gold_well_summary",            build_gold_well_summary)

    # New Gold tables
    _try("gold_flaring_timeseries",       build_gold_flaring_timeseries)
    _try("gold_three_stream_production",  build_gold_three_stream_production)
    _try("gold_ip_benchmarks",            build_gold_ip_benchmarks)
    _try("gold_well_economics",           build_gold_well_economics)  # depends on gold_well_summary

    return counts
