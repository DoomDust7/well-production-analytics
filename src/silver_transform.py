"""
Silver Layer: Cleaned, validated, and joined Delta tables.
- Type casting & column standardization (snake_case)
- Null handling and deduplication
- Derived columns: production_year, production_quarter
"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import bronze_path, silver_path


def _quarter(col_name: str) -> F.Column:
    """Return Q1/Q2/Q3/Q4 string from a date column."""
    return (
        F.when(F.month(col_name).between(1, 3), "Q1")
         .when(F.month(col_name).between(4, 6), "Q2")
         .when(F.month(col_name).between(7, 9), "Q3")
         .otherwise("Q4")
    )


def _make_month_date(year_col: str, month_col: str) -> F.Column:
    """Construct a Date from integer Year and Month columns."""
    return F.to_date(
        F.concat(
            F.col(year_col).cast("string"),
            F.lit("-"),
            F.lpad(F.col(month_col).cast("string"), 2, "0"),
            F.lit("-01"),
        )
    )


def _strip_num(col: F.Column) -> F.Column:
    """Remove commas and cast to Double."""
    return F.regexp_replace(col.cast("string"), ",", "").cast("double")


# ── Legacy hackathon sources ──────────────────────────────────────────────────

def build_silver_wells(spark: SparkSession) -> int:
    """silver_wells: clean well metadata — api_number, well_name, well_length_ft."""
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
    """silver_flaring: standardized operator + flaring volumes (operator totals)."""
    df = spark.read.format("delta").load(bronze_path("bronze_flaring"))
    df = (
        df
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
    """silver_production: synthetic production records (legacy fallback)."""
    prod = spark.read.format("delta").load(bronze_path("bronze_well_production"))
    wells = spark.read.format("delta").load(silver_path("silver_wells"))

    prod = (
        prod
        .withColumn("production", F.col("production").cast("double"))
        .withColumn("production_month", F.to_date(F.col("production_month"), "yyyy-MM-dd"))
        .withColumn("operator", F.trim(F.upper(F.col("operator"))))
        .filter(F.col("api_number").isNotNull())
        .filter(F.col("production_month").isNotNull())
        .filter(F.col("operator").isNotNull())
        .filter(F.col("production") >= 0)
        .dropDuplicates(["api_number", "production_month", "oil_and_gas_group"])
    )

    prod = (
        prod
        .withColumn("production_year", F.year(F.col("production_month")))
        .withColumn("production_quarter", _quarter("production_month"))
    )

    prod = (
        prod
        .join(
            wells.select("api_number",
                         F.col("well_name").alias("well_name_meta"),
                         "well_length_ft"),
            on="api_number",
            how="left",
        )
        .withColumn("well_name", F.coalesce(F.col("well_name"), F.col("well_name_meta")))
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


# ── Wells-Dataset real data sources ──────────────────────────────────────────

def build_silver_wellheader(spark: SparkSession) -> int:
    """silver_wellheader: typed well attributes from WELLHEADER.csv."""
    df = spark.read.format("delta").load(bronze_path("bronze_wellheader"))
    cols = {c.lower(): c for c in df.columns}

    def _col(candidates):
        for c in candidates:
            if c in cols:
                return F.col(cols[c])
        return F.lit(None)

    df = (
        df
        .withColumn("api_number",        _col(["api_number"]).cast("string"))
        .withColumn("well_name",         _col(["well_name"]).cast("string"))
        # first_reported_operator is the human-readable name in this dataset
        .withColumn("operator",          F.trim(F.upper(_col([
            "first_reported_operator", "operator", "reported_operator"]).cast("string"))))
        .withColumn("latitude",          _strip_num(_col(["latitude"])))
        .withColumn("longitude",         _strip_num(_col(["longitude"])))
        # lateral_length in the dataset (not "Lateral Length")
        .withColumn("lateral_length_ft", _strip_num(_col(["lateral_length"])))
        .withColumn("tvd_ft",            _strip_num(_col(["true_vertical_depth", "tvd"])))
        .withColumn("formation",         _col(["formation"]).cast("string"))
        # No explicit basin column — use formation as proxy
        .withColumn("basin",             _col(["formation"]).cast("string"))
        .withColumn("proppant_type",     _col([
            "proppant_type_category", "proppant_type_group", "proppant_type"]).cast("string"))
        .withColumn("proppant_intensity", _strip_num(_col([
            "proppant_thousand_pounds", "proppant_intensity"])))
        # wti_breakeven_full_cycle is the per-well WTI breakeven
        .withColumn("breakeven_oil_price", _strip_num(_col([
            "wti_breakeven_full_cycle", "wellhead_breakeven_oil_price", "breakeven_oil_price"])))
        .withColumn("irr", _strip_num(_col(["irr"])))
        .withColumn("npv", _strip_num(_col(["npv"])))
        .withColumn("spud_date",
                    F.coalesce(
                        F.to_date(_col(["spud_date"]).cast("string"), "yyyy-MM-dd"),
                        F.to_date(_col(["spud_date"]).cast("string"), "M/d/yyyy"),
                    ))
        .withColumn("completion_date",
                    F.coalesce(
                        F.to_date(_col(["completion_date"]).cast("string"), "yyyy-MM-dd"),
                        F.to_date(_col(["completion_date"]).cast("string"), "M/d/yyyy"),
                    ))
        .select(
            "api_number", "well_name", "operator", "latitude", "longitude",
            "spud_date", "completion_date", "lateral_length_ft", "tvd_ft",
            "formation", "basin", "proppant_type", "proppant_intensity",
            "breakeven_oil_price", "irr", "npv",
        )
        .filter(F.col("api_number").isNotNull())
        .dropDuplicates(["api_number"])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_wellheader")))
    return df.count()


def build_silver_operator_enriched(spark: SparkSession) -> int:
    """silver_operator_enriched: operator lookup with company metadata."""
    df = spark.read.format("delta").load(bronze_path("bronze_operator"))
    cols = {c.lower(): c for c in df.columns}

    def _col(candidates):
        for c in candidates:
            if c in cols:
                return F.col(cols[c])
        return F.lit(None)

    df = (
        df
        .withColumn("operator",       F.trim(F.upper(_col(["operator"]).cast("string"))))
        .withColumn("ticker",         _col(["bloomberg_ticker", "short_ticker"]).cast("string"))
        .withColumn("public_private", _col(["public_private_company", "publicprivatecompany"]).cast("string"))
        .withColumn("pe_backer",      _col(["private_equity_backer", "privateequitybacker"]).cast("string"))
        .select("operator", "ticker", "public_private", "pe_backer")
        .filter(F.col("operator").isNotNull())
        .dropDuplicates(["operator"])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_operator_enriched")))
    return df.count()


def build_silver_prices(spark: SparkSession) -> int:
    """silver_prices: monthly commodity prices (WTI, Brent, WCS)."""
    df = spark.read.format("delta").load(bronze_path("bronze_prices"))
    cols = {c.lower(): c for c in df.columns}

    def _col(candidates):
        for c in candidates:
            if c in cols:
                return F.col(cols[c])
        return F.lit(None)

    df = (
        df
        .withColumn("price_month", _make_month_date(
            cols.get("year", "year"), cols.get("month", "month")))
        .withColumn("wti",   _strip_num(_col(["wti_cushing_oil_price", "wti_cushing", "wti"])))
        .withColumn("brent", _strip_num(_col(["brent_oil_price", "brent"])))
        .withColumn("wcs",   _strip_num(_col(["wcs_oil_price", "wcs"])))
        .select("price_month", "wti", "brent", "wcs")
        .filter(F.col("price_month").isNotNull())
        .dropDuplicates(["price_month"])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_prices")))
    return df.count()


def build_silver_well_eur(spark: SparkSession) -> int:
    """silver_well_eur: EUR per well from WELLEUR and EUR files."""
    df1 = spark.read.format("delta").load(bronze_path("bronze_welleur"))
    cols1 = {c.lower(): c for c in df1.columns}
    api1 = cols1.get("api_number", cols1.get("api_number_", "api_number"))
    eur_col1 = cols1.get("estimatedwellultimaterecovery", cols1.get("estimated_well_ultimate_recovery", None))
    oag_col1 = cols1.get("oil_and_gas_group", cols1.get("oilandgasgroup", None))

    df1 = df1.withColumn("api_number", F.col(api1).cast("string"))
    if eur_col1:
        df1 = df1.withColumn("eur_primary", _strip_num(F.col(eur_col1)))
    else:
        df1 = df1.withColumn("eur_primary", F.lit(None).cast("double"))
    if oag_col1:
        df1 = df1.withColumn("oil_and_gas_group", F.col(oag_col1).cast("string"))
    else:
        df1 = df1.withColumn("oil_and_gas_group", F.lit(None).cast("string"))
    df1 = df1.select("api_number", "oil_and_gas_group", "eur_primary")

    df2 = spark.read.format("delta").load(bronze_path("bronze_eur"))
    cols2 = {c.lower(): c for c in df2.columns}
    api2 = cols2.get("api_number", cols2.get("api_number_", "api_number"))
    eur_col2 = cols2.get("eur", None)
    df2 = df2.withColumn("api_number", F.col(api2).cast("string"))
    if eur_col2:
        df2 = df2.withColumn("eur_fallback", _strip_num(F.col(eur_col2)))
    else:
        df2 = df2.withColumn("eur_fallback", F.lit(None).cast("double"))
    df2 = df2.select("api_number", "eur_fallback")

    df = (
        df1.join(df2, on="api_number", how="left")
        .withColumn("eur", F.coalesce(F.col("eur_primary"), F.col("eur_fallback")))
        .withColumn("eur_category",
            F.when(F.col("eur") > 500000, "High")
             .when(F.col("eur") > 100000, "Medium")
             .otherwise("Low"))
        .select("api_number", "oil_and_gas_group", "eur", "eur_category")
        .filter(F.col("api_number").isNotNull())
        .dropDuplicates(["api_number"])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_well_eur")))
    return df.count()


def build_silver_initial_production(spark: SparkSession) -> int:
    """silver_initial_production: IP30/90/180/365 per well."""
    df = spark.read.format("delta").load(bronze_path("bronze_initialproduction"))
    cols = {c.lower(): c for c in df.columns}

    def _col(candidates):
        for c in candidates:
            if c in cols:
                return F.col(cols[c])
        return F.lit(None)

    api_col = next((cols[c] for c in ["api_number", "api_number_"] if c in cols), "api_number")

    df = (
        df
        .withColumn("api_number", F.col(api_col).cast("string"))
        .withColumn("ip30",  _strip_num(_col(["initial_production_-_30_days",  "initial_production___30_days"])))
        .withColumn("ip90",  _strip_num(_col(["initial_production_-_90_days",  "initial_production___90_days"])))
        .withColumn("ip180", _strip_num(_col(["initial_production_-_half-year","initial_production___half-year",
                                              "initial_production_-_half_year"])))
        .withColumn("ip365", _strip_num(_col(["initial_production_-_year",     "initial_production___year"])))
        .withColumn("oil_and_gas_group", _col(["oilandgasgroup", "oil_and_gas_group"]).cast("string"))
        .select("api_number", "ip30", "ip90", "ip180", "ip365", "oil_and_gas_group")
        .filter(F.col("api_number").isNotNull())
        .filter(F.col("ip30").isNotNull())
        .dropDuplicates(["api_number"])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_initial_production")))
    return df.count()


def build_silver_well_costs(spark: SparkSession) -> int:
    """silver_well_costs: total well cost (MUSD) by API and cost category."""
    df = spark.read.format("delta").load(bronze_path("bronze_economicscost"))
    cols = {c.lower(): c for c in df.columns}

    api_col = next((cols[c] for c in ["api_number", "api_number_"] if c in cols), "api_number")
    cat_col = next((cols[c] for c in ["well_cost_category", "wellcostcategory"] if c in cols), None)
    # "Well Cost (MUSD)" sanitizes to "well_cost__musd_" then stripped to "well_cost__musd"
    cost_col = next(
        (cols[c] for c in cols
         if c.startswith("well_cost") and ("musd" in c or c == "well_cost" or c == "wellcost")),
        None,
    )

    df = df.withColumn("api_number", F.col(api_col).cast("string"))
    if cat_col:
        df = df.withColumn("cost_category", F.col(cat_col).cast("string"))
    else:
        df = df.withColumn("cost_category", F.lit("Total"))
    if cost_col:
        df = df.withColumn("well_cost_musd", _strip_num(F.col(cost_col)))
    else:
        df = df.withColumn("well_cost_musd", F.lit(None).cast("double"))

    df = (
        df
        .filter(F.col("api_number").isNotNull())
        .filter(F.col("well_cost_musd").isNotNull())
        .groupBy("api_number", "cost_category")
        .agg(F.sum("well_cost_musd").alias("well_cost_musd"))
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_well_costs")))
    return df.count()


def build_silver_production_real(spark: SparkSession) -> int:
    """silver_production_real: typed real per-well monthly production."""
    prod = spark.read.format("delta").load(bronze_path("bronze_production_real"))
    cols = {c.lower(): c for c in prod.columns}

    api_col  = next((cols[c] for c in ["api_number", "api_number_"] if c in cols), "api_number")
    year_col = next((cols[c] for c in ["year"] if c in cols), "year")
    mon_col  = next((cols[c] for c in ["month"] if c in cols), "month")
    prod_col = next((cols[c] for c in ["production"] if c in cols), "production")
    oag_col  = next((cols[c] for c in ["oilandgasgroup", "oil_and_gas_group"] if c in cols), None)

    prod = (
        prod
        .withColumn("api_number",        F.col(api_col).cast("string"))
        .withColumn("production_month",  _make_month_date(year_col, mon_col))
        .withColumn("production",        _strip_num(F.col(prod_col)))
        .withColumn("oil_and_gas_group", F.col(oag_col).cast("string") if oag_col else F.lit("O"))
    )

    # Join with silver_wellheader to get operator (already normalized in build_silver_wellheader)
    hdr_silver_path = silver_path("silver_wellheader")
    if os.path.exists(hdr_silver_path):
        hdr = (spark.read.format("delta").load(hdr_silver_path)
               .select("api_number", F.col("operator").alias("hdr_operator"))
               .dropDuplicates(["api_number"]))
        prod = (prod.join(hdr, on="api_number", how="left")
                .withColumn("operator", F.col("hdr_operator"))
                .drop("hdr_operator"))
    else:
        prod = prod.withColumn("operator", F.lit(None).cast("string"))

    # Basin from silver_wellheader (uses formation as basin proxy)
    if os.path.exists(hdr_silver_path):
        basin_df = (spark.read.format("delta").load(hdr_silver_path)
                    .select("api_number", F.col("basin").alias("hdr_basin"))
                    .dropDuplicates(["api_number"]))
        if "basin" not in prod.columns:
            prod = (prod.join(basin_df, on="api_number", how="left")
                    .withColumn("basin", F.col("hdr_basin"))
                    .drop("hdr_basin"))
    if "basin" not in prod.columns:
        prod = prod.withColumn("basin", F.lit("Williston").cast("string"))

    prod = (
        prod
        .withColumn("production_year",    F.year(F.col("production_month")))
        .withColumn("production_quarter", _quarter("production_month"))
        .filter(F.col("api_number").isNotNull())
        .filter(F.col("production_month").isNotNull())
        .filter(F.col("production") >= 0)
        .dropDuplicates(["api_number", "production_month", "oil_and_gas_group"])
        .select(
            "api_number", "production_month", "production_year", "production_quarter",
            "oil_and_gas_group", "production", "operator", "basin",
        )
    )

    (prod.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("production_year")
        .save(silver_path("silver_production_real")))
    return prod.count()


def build_silver_production_flaring_real(spark: SparkSession) -> int:
    """silver_production_flaring_real: per-well monthly flaring with intensity."""
    df = spark.read.format("delta").load(bronze_path("bronze_production_flaring_real"))
    cols = {c.lower(): c for c in df.columns}

    api_col   = next((cols[c] for c in ["api_number", "api_number_"] if c in cols), "api_number")
    year_col  = next((cols[c] for c in ["year"] if c in cols), "year")
    mon_col   = next((cols[c] for c in ["month"] if c in cols), "month")
    flrd_col  = next((cols[c] for c in ["flaredgas_mcf"] if c in cols), None)
    gross_col = next((cols[c] for c in ["grossgasproduction_mcf"] if c in cols), None)

    df = (
        df
        .withColumn("api_number",               F.col(api_col).cast("string"))
        .withColumn("production_month",         _make_month_date(year_col, mon_col))
        .withColumn("flared_gas_mcf",           _strip_num(F.col(flrd_col))  if flrd_col  else F.lit(None).cast("double"))
        .withColumn("gross_gas_production_mcf", _strip_num(F.col(gross_col)) if gross_col else F.lit(None).cast("double"))
        .withColumn("flaring_intensity",
            F.when(
                F.col("gross_gas_production_mcf") > 0,
                F.col("flared_gas_mcf") / F.col("gross_gas_production_mcf")
            ).otherwise(F.lit(None)))
        .select("api_number", "production_month",
                "flared_gas_mcf", "gross_gas_production_mcf", "flaring_intensity")
        .filter(F.col("api_number").isNotNull())
        .filter(F.col("production_month").isNotNull())
        .dropDuplicates(["api_number", "production_month"])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_production_flaring_real")))
    return df.count()


def build_silver_water_production(spark: SparkSession) -> int:
    """silver_water_production: per-well monthly water production."""
    df = spark.read.format("delta").load(bronze_path("bronze_water_production"))
    cols = {c.lower(): c for c in df.columns}

    api_col  = next((cols[c] for c in ["api_number", "api_number_"] if c in cols), "api_number")
    year_col = next((cols[c] for c in ["year"] if c in cols), "year")
    mon_col  = next((cols[c] for c in ["month"] if c in cols), "month")
    wat_col  = next((cols[c] for c in ["waterproduction", "water_production"] if c in cols), None)
    dop_col  = next((cols[c] for c in ["daysonproduction", "days_on_production"] if c in cols), None)

    df = (
        df
        .withColumn("api_number",           F.col(api_col).cast("string"))
        .withColumn("production_month",     _make_month_date(year_col, mon_col))
        .withColumn("water_production_bbl", _strip_num(F.col(wat_col)) if wat_col else F.lit(None).cast("double"))
        .withColumn("days_on_production",   _strip_num(F.col(dop_col)) if dop_col else F.lit(None).cast("double"))
        .select("api_number", "production_month", "water_production_bbl", "days_on_production")
        .filter(F.col("api_number").isNotNull())
        .filter(F.col("production_month").isNotNull())
        .dropDuplicates(["api_number", "production_month"])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(silver_path("silver_water_production")))
    return df.count()


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_silver(spark: SparkSession, use_synthetic: bool = False) -> dict:
    """Run Silver transformations. Returns {table: row_count}."""
    counts = {}
    print("\n[SILVER] Transforming Bronze -> Silver Delta tables...")

    def _try(name, fn):
        try:
            n = fn(spark)
            counts[name] = n
            print(f"  {name:<40}: {n:>8,} rows")
        except Exception as e:
            print(f"  {name:<40}: SKIPPED ({e})")

    # Real data sources (run first — others depend on wellheader)
    if os.path.exists(bronze_path("bronze_wellheader")):
        _try("silver_wellheader", build_silver_wellheader)

    if os.path.exists(bronze_path("bronze_operator")):
        _try("silver_operator_enriched", build_silver_operator_enriched)

    if os.path.exists(bronze_path("bronze_prices")):
        _try("silver_prices", build_silver_prices)

    if os.path.exists(bronze_path("bronze_welleur")) and os.path.exists(bronze_path("bronze_eur")):
        _try("silver_well_eur", build_silver_well_eur)
    elif os.path.exists(bronze_path("bronze_welleur")):
        _try("silver_well_eur", build_silver_well_eur)

    if os.path.exists(bronze_path("bronze_initialproduction")):
        _try("silver_initial_production", build_silver_initial_production)

    if os.path.exists(bronze_path("bronze_economicscost")):
        _try("silver_well_costs", build_silver_well_costs)

    if os.path.exists(bronze_path("bronze_production_real")):
        _try("silver_production_real", build_silver_production_real)

    if os.path.exists(bronze_path("bronze_production_flaring_real")):
        _try("silver_production_flaring_real", build_silver_production_flaring_real)

    if os.path.exists(bronze_path("bronze_water_production")):
        _try("silver_water_production", build_silver_water_production)

    # Legacy sources
    if os.path.exists(bronze_path("bronze_well_metadata")):
        _try("silver_wells", build_silver_wells)

    if os.path.exists(bronze_path("bronze_flaring")):
        _try("silver_flaring", build_silver_flaring)

    if use_synthetic and os.path.exists(bronze_path("bronze_well_production")):
        if os.path.exists(silver_path("silver_wells")):
            _try("silver_production", build_silver_production)

    return counts
