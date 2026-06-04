# Databricks notebook source
# DBTITLE 1,DataNexus BD Pipeline Overview
# MAGIC %md
# MAGIC # DataNexus BD — Enterprise Big Data Analytics Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC **End-to-end Medallion Architecture (Bronze → Silver → Gold) on Databricks Delta Lake**
# MAGIC
# MAGIC ### Datasets
# MAGIC * **Semiconductor Stocks**: NVDA, AMD, INTC, TSM, QCOM (OHLCV data from yfinance)
# MAGIC * **ESG Risk Ratings**: 500 companies, 10 sectors, quarterly risk scores
# MAGIC * **Renewable Energy**: 50 countries, 5 energy types, 10 years of capacity/generation data
# MAGIC
# MAGIC ### Pipeline Layers
# MAGIC 1. **Bronze**: Raw data ingestion with schema-on-read
# MAGIC 2. **Silver**: Cleaned, validated, enriched data
# MAGIC 3. **Gold**: Business-ready aggregations and analytics
# MAGIC 4. **Validation**: Data quality checks and metrics
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC Raw CSVs → Bronze (Delta) → Silver (Delta) → Gold (Delta) → Dashboards
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Environment Setup & Configuration
# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================

import warnings
warnings.filterwarnings('ignore')

# Install required libraries
%pip install yfinance pandas numpy --quiet

# Import libraries (after pip install)
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, year, month, quarter,
    avg, sum as _sum, count, max as _max, min as _min, stddev,
    lag, lead, datediff, row_number, desc, asc,
    round as spark_round, when, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, DateType, TimestampType, LongType
)
from datetime import datetime, timedelta
import yfinance as yf

print("\n" + "="*80)
print("DataNexus BD - Lakehouse Pipeline")
print("="*80)
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Spark Version: {spark.version}")
print("="*80 + "\n")

# COMMAND ----------

# DBTITLE 1,Configuration & Parameters
# ============================================================================
# CONFIGURATION
# ============================================================================

# Database configuration (using Unity Catalog Volume)
DATABASE_NAME = "datanexus"
BASE_PATH = "/Volumes/workspace/default/datanexus_data"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

# Data paths
RAW_DATA_PATH = "/tmp/datanexus/raw/"
CHECKPOINT_PATH = "/tmp/datanexus/checkpoints/"

# Stock tickers
TICKERS = ['NVDA', 'AMD', 'INTC', 'TSM', 'QCOM']
START_DATE = '2020-01-01'
END_DATE = '2024-12-31'

print(f"✓ Base Path: {BASE_PATH}")
print(f"✓ Delta Layers: Bronze, Silver, Gold")
print(f"✓ Tickers: {', '.join(TICKERS)}")
print(f"✓ Date Range: {START_DATE} to {END_DATE}\n")

# COMMAND ----------

# DBTITLE 1,Bronze Layer — Raw Data Ingestion
# MAGIC %md
# MAGIC ## Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC **Purpose**: Load raw data exactly as received, preserving source fidelity
# MAGIC
# MAGIC **Operations**:
# MAGIC * Fetch live stock data from yfinance API
# MAGIC * Read mock ESG and Energy CSVs from DBFS (if available)
# MAGIC * Write to Delta tables with minimal transformation
# MAGIC * Add ingestion metadata (timestamp, source)

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Stocks Data (Live from yfinance)
# ============================================================================
# BRONZE LAYER - STOCKS DATA (from CSV)
# ============================================================================

print("[BRONZE] Ingesting Stocks data from CSV...")

try:
    # Read from CSV file
    csv_path = "/Workspace/Users/daymavrundavan@gmail.com/DataNexus_BD/data/stocks.csv"
    stocks_pd = pd.read_csv(csv_path)
    
    print(f"  ✓ Read {len(stocks_pd):,} rows from CSV")
    print(f"  Tickers: {stocks_pd['ticker'].nunique()}")
    print(f"  Date range: {stocks_pd['date'].min()} to {stocks_pd['date'].max()}")
    
    # Convert to Spark DataFrame
    stocks_spark = spark.createDataFrame(stocks_pd)
    
    # Add metadata
    stocks_spark = stocks_spark \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source", lit("yahoo_finance_csv"))
    
    # Write to Bronze Delta table
    stocks_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{BRONZE_PATH}/stocks_raw")
    
    row_count = stocks_spark.count()
    print(f"\n✓ Wrote {row_count:,} rows to {BRONZE_PATH}/stocks_raw\n")
    
except Exception as e:
    print(f"\n✗ Error ingesting stocks data: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest ESG Data (from CSV or Mock)
# ============================================================================
# BRONZE LAYER - ESG DATA (from CSV)
# ============================================================================

print("[BRONZE] Ingesting ESG data from CSV...")

try:
    # Read from CSV file
    csv_path = "/Workspace/Users/daymavrundavan@gmail.com/DataNexus_BD/data/esg.csv"
    esg_pd = pd.read_csv(csv_path)
    
    print(f"  ✓ Read {len(esg_pd):,} rows from CSV")
    print(f"  Companies: {esg_pd['ticker'].nunique()}")
    print(f"  Date range: {esg_pd['last_processing_date'].min()} to {esg_pd['last_processing_date'].max()}")
    
    # Convert to Spark DataFrame
    esg_df = spark.createDataFrame(esg_pd)
    
    # Add metadata
    esg_df = esg_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source", lit("yahoo_finance_esg_csv"))
    
    # Write to Bronze
    esg_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{BRONZE_PATH}/esg_raw")
    
    row_count = esg_df.count()
    print(f"\n✓ Wrote {row_count:,} rows to {BRONZE_PATH}/esg_raw\n")
    
except Exception as e:
    print(f"\n✗ Error ingesting ESG data: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Energy Data (from CSV or Mock)
# ============================================================================
# BRONZE LAYER - RENEWABLE ENERGY DATA (from CSV)
# ============================================================================

print("[BRONZE] Ingesting Renewable Energy data from CSV...")

try:
    # Read from CSV file
    csv_path = "/Workspace/Users/daymavrundavan@gmail.com/DataNexus_BD/data/renewable_energy.csv"
    energy_pd = pd.read_csv(csv_path)
    
    print(f"  ✓ Read {len(energy_pd):,} rows from CSV")
    print(f"  Countries: {energy_pd['country'].nunique()}")
    print(f"  Technologies: {energy_pd['technology_type'].nunique()}")
    print(f"  Year range: {energy_pd['year'].min()} to {energy_pd['year'].max()}")
    
    # Convert to Spark DataFrame
    energy_df = spark.createDataFrame(energy_pd)
    
    # Add metadata
    energy_df = energy_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source", lit("global_trends_csv"))
    
    # Write to Bronze
    energy_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{BRONZE_PATH}/energy_raw")
    
    row_count = energy_df.count()
    print(f"\n✓ Wrote {row_count:,} rows to {BRONZE_PATH}/energy_raw")
    
    print("\n" + "="*80)
    print("BRONZE LAYER COMPLETE")
    print("="*80)
    print(f"✓ Stocks: {spark.read.format('delta').load(f'{BRONZE_PATH}/stocks_raw').count():,} rows")
    print(f"✓ ESG: {spark.read.format('delta').load(f'{BRONZE_PATH}/esg_raw').count():,} rows")
    print(f"✓ Energy: {spark.read.format('delta').load(f'{BRONZE_PATH}/energy_raw').count():,} rows")
    print("="*80 + "\n")
    
except Exception as e:
    print(f"\n✗ Error ingesting energy data: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# DBTITLE 1,Silver Layer — Data Cleaning & Enrichment
# MAGIC %md
# MAGIC ## Silver Layer — Data Cleaning & Enrichment
# MAGIC
# MAGIC **Purpose**: Transform raw data into clean, validated, analysis-ready datasets
# MAGIC
# MAGIC **Operations**:
# MAGIC * Schema enforcement and type validation
# MAGIC * Null handling and deduplication
# MAGIC * Feature engineering (daily returns, risk categories)
# MAGIC * Data quality checks
# MAGIC * Standardized naming conventions

# COMMAND ----------

# DBTITLE 1,Silver — Clean Stocks Data
# ============================================================================
# SILVER LAYER - STOCKS CLEANED
# ============================================================================

print("\n[SILVER] Cleaning Stocks data...")

# Read from Bronze
stocks_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/stocks_raw")

# Clean and enrich
window_spec = Window.partitionBy("ticker").orderBy("date")

stocks_silver = stocks_bronze \
    .filter(col("close").isNotNull()) \
    .filter(col("volume") > 0) \
    .withColumn("date", to_date(col("date"))) \
    .withColumn("prev_close", lag("close", 1).over(window_spec)) \
    .withColumn("daily_return", 
                when(col("prev_close").isNotNull(), 
                     ((col("close") - col("prev_close")) / col("prev_close")) * 100)
                .otherwise(0)) \
    .withColumn("price_range", col("high") - col("low")) \
    .withColumn("year_col", year(col("date"))) \
    .withColumn("month_col", month(col("date"))) \
    .withColumn("quarter_col", quarter(col("date"))) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .select(
        "date", "ticker", "open", "high", "low", "close", "volume",
        "daily_return", "price_range", 
        col("year_col").alias("year"),
        col("month_col").alias("month"),
        col("quarter_col").alias("quarter"),
        "processed_timestamp"
    )

# Remove duplicates
stocks_silver = stocks_silver.dropDuplicates(["date", "ticker"])

# Write to Silver
stocks_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/stocks_clean")

print(f"✓ Wrote {stocks_silver.count():,} rows to {SILVER_PATH}/stocks_clean")

# COMMAND ----------

# DBTITLE 1,Silver — Clean ESG Data
# ============================================================================
# SILVER LAYER - ESG CLEANED
# ============================================================================

print("\n[SILVER] Cleaning ESG data...")

# Read from Bronze
esg_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/esg_raw")

# Clean and categorize
esg_silver = esg_bronze \
    .withColumn("last_processing_date", to_date(col("last_processing_date"))) \
    .filter(col("total_esg_risk_score").isNotNull()) \
    .withColumn("risk_category",
                when(col("total_esg_risk_score") < 20, "Low")
                .when(col("total_esg_risk_score") < 30, "Medium")
                .otherwise("High")) \
    .withColumn("has_controversy", 
                when(col("controversy_score") > 0, True).otherwise(False)) \
    .withColumn("esg_rating",
                when(col("total_esg_risk_score") < 20, "AAA")
                .when(col("total_esg_risk_score") < 25, "AA")
                .when(col("total_esg_risk_score") < 30, "A")
                .when(col("total_esg_risk_score") < 35, "BBB")
                .when(col("total_esg_risk_score") < 40, "BB")
                .when(col("total_esg_risk_score") < 45, "B")
                .otherwise("CCC")) \
    .withColumn("year_col", year(col("last_processing_date"))) \
    .withColumn("quarter_col", quarter(col("last_processing_date"))) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .select(
        "company_name", "ticker", "sector", "last_processing_date",
        "environmental_risk", "social_risk", "governance_risk",
        "total_esg_risk_score", "controversy_score",
        "risk_category", "esg_rating", "has_controversy",
        col("year_col").alias("year"),
        col("quarter_col").alias("quarter"),
        "processed_timestamp"
    )

# Remove duplicates
esg_silver = esg_silver.dropDuplicates(["ticker", "last_processing_date"])

# Write to Silver
esg_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/esg_clean")

print(f"✓ Wrote {esg_silver.count():,} rows to {SILVER_PATH}/esg_clean")

# COMMAND ----------

# DBTITLE 1,Silver — Clean Energy Data
# ============================================================================
# SILVER LAYER - ENERGY CLEANED
# ============================================================================

print("\n[SILVER] Cleaning Renewable Energy data...")

# Read from Bronze
energy_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/energy_raw")

# Clean and compute metrics
energy_silver = energy_bronze \
    .filter(col("capacity_mw").isNotNull()) \
    .filter(col("generation_gwh").isNotNull()) \
    .withColumn("capacity_utilization_pct", 
                spark_round(col("capacity_factor") * 100, 2)) \
    .withColumn("avg_generation_mwh_per_mw",
                spark_round((col("generation_gwh") * 1000) / col("capacity_mw"), 2)) \
    .withColumn("renewable_type_group",
                when(col("technology_type").isin(["Solar PV", "Onshore Wind", "Offshore Wind", "Concentrated Solar"]), "Variable")
                .otherwise("Dispatchable")) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .select(
        "country", "year", 
        col("technology_type").alias("energy_type"),
        col("capacity_mw").alias("installed_capacity_mw"), 
        "generation_gwh", "capacity_factor",
        "capacity_utilization_pct", "avg_generation_mwh_per_mw",
        "renewable_type_group", "processed_timestamp"
    )

# Remove duplicates
energy_silver = energy_silver.dropDuplicates(["country", "year", "energy_type"])

# Write to Silver
energy_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/energy_clean")

print(f"✓ Wrote {energy_silver.count():,} rows to {SILVER_PATH}/energy_clean")
print("\n" + "="*80)
print("SILVER LAYER COMPLETE")
print("="*80)
print(f"✓ Stocks: {spark.read.format('delta').load(f'{SILVER_PATH}/stocks_clean').count():,} rows")
print(f"✓ ESG: {spark.read.format('delta').load(f'{SILVER_PATH}/esg_clean').count():,} rows")
print(f"✓ Energy: {spark.read.format('delta').load(f'{SILVER_PATH}/energy_clean').count():,} rows")
print("="*80 + "\n")

# COMMAND ----------

# DBTITLE 1,Gold Layer — Business Analytics
# MAGIC %md
# MAGIC ## Gold Layer — Business Analytics
# MAGIC
# MAGIC **Purpose**: Create aggregated, business-ready datasets optimized for reporting and dashboards
# MAGIC
# MAGIC **Tables**:
# MAGIC * `stocks_moving_avg` — 7/30/90-day moving averages per ticker
# MAGIC * `esg_sector_summary` — Sector-level ESG performance metrics
# MAGIC * `energy_trend_summary` — Country/type renewable energy trends
# MAGIC * `portfolio_risk_profile` — Volatility and risk metrics per ticker

# COMMAND ----------

# DBTITLE 1,Gold — Stocks Moving Averages
# ============================================================================
# GOLD LAYER - STOCKS MOVING AVERAGES
# ============================================================================

print("[GOLD] Computing Stocks Moving Averages...")

# Read from Silver
stocks_silver = spark.read.format("delta").load(f"{SILVER_PATH}/stocks_clean")

# Calculate moving averages
window_7d = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
window_30d = Window.partitionBy("ticker").orderBy("date").rowsBetween(-29, 0)
window_90d = Window.partitionBy("ticker").orderBy("date").rowsBetween(-89, 0)

stocks_ma = stocks_silver \
    .withColumn("ma_7d", spark_round(avg("close").over(window_7d), 2)) \
    .withColumn("ma_30d", spark_round(avg("close").over(window_30d), 2)) \
    .withColumn("ma_90d", spark_round(avg("close").over(window_90d), 2)) \
    .withColumn("volume_7d_avg", spark_round(avg("volume").over(window_7d), 0).cast("long")) \
    .withColumn("golden_cross", 
                when((col("ma_7d") > col("ma_30d")) & 
                     (lag("ma_7d").over(Window.partitionBy("ticker").orderBy("date")) <= 
                      lag("ma_30d").over(Window.partitionBy("ticker").orderBy("date"))), True)
                .otherwise(False)) \
    .select(
        "date", "ticker", "close", "volume",
        "ma_7d", "ma_30d", "ma_90d", "volume_7d_avg", "golden_cross"
    )

# Write to Gold
stocks_ma.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{GOLD_PATH}/stocks_moving_avg")

print(f"✓ Wrote {stocks_ma.count():,} rows to {GOLD_PATH}/stocks_moving_avg")

# COMMAND ----------

# DBTITLE 1,Gold — ESG Sector Summary
# ============================================================================
# GOLD LAYER - ESG SECTOR SUMMARY
# ============================================================================

print("\n[GOLD] Computing ESG Sector Summary...")

# Read from Silver
esg_silver = spark.read.format("delta").load(f"{SILVER_PATH}/esg_clean")

# Aggregate by sector, year, and quarter
esg_sector = esg_silver \
    .groupBy("sector", "year", "quarter") \
    .agg(
        count("ticker").alias("company_count"),
        spark_round(avg("environmental_risk"), 2).alias("avg_environmental_risk"),
        spark_round(avg("social_risk"), 2).alias("avg_social_risk"),
        spark_round(avg("governance_risk"), 2).alias("avg_governance_risk"),
        spark_round(avg("total_esg_risk_score"), 2).alias("avg_total_esg_risk"),
        spark_round(avg("controversy_score"), 2).alias("avg_controversy_score"),
        _sum(when(col("has_controversy"), 1).otherwise(0)).alias("companies_with_controversy"),
        _sum(when(col("risk_category") == "High", 1).otherwise(0)).alias("high_risk_companies"),
        _sum(when(col("risk_category") == "Medium", 1).otherwise(0)).alias("medium_risk_companies"),
        _sum(when(col("risk_category") == "Low", 1).otherwise(0)).alias("low_risk_companies")
    ) \
    .withColumn("controversy_rate_pct", 
                spark_round((col("companies_with_controversy") / col("company_count")) * 100, 2)) \
    .orderBy("sector", "year", "quarter")

# Write to Gold
esg_sector.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{GOLD_PATH}/esg_sector_summary")

print(f"✓ Wrote {esg_sector.count():,} rows to {GOLD_PATH}/esg_sector_summary")

# COMMAND ----------

# DBTITLE 1,Gold — Energy Trend Summary
# ============================================================================
# GOLD LAYER - ENERGY TREND SUMMARY
# ============================================================================

print("\n[GOLD] Computing Energy Trend Summary...")

# Read from Silver
energy_silver = spark.read.format("delta").load(f"{SILVER_PATH}/energy_clean")

# Aggregate by country and year
energy_trends = energy_silver \
    .groupBy("country", "year") \
    .agg(
        spark_round(_sum("installed_capacity_mw"), 2).alias("total_capacity_mw"),
        spark_round(_sum("generation_gwh"), 2).alias("total_generation_gwh"),
        spark_round(avg("capacity_factor"), 3).alias("avg_capacity_factor"),
        count("energy_type").alias("energy_type_count")
    )

# Calculate year-over-year growth
window_yoy = Window.partitionBy("country").orderBy("year")

energy_trends = energy_trends \
    .withColumn("prev_year_capacity", lag("total_capacity_mw").over(window_yoy)) \
    .withColumn("capacity_growth_pct",
                when(col("prev_year_capacity").isNotNull(),
                     spark_round(((col("total_capacity_mw") - col("prev_year_capacity")) / 
                                   col("prev_year_capacity")) * 100, 2))
                .otherwise(None)) \
    .select(
        "country", "year", "total_capacity_mw", "total_generation_gwh",
        "avg_capacity_factor", "energy_type_count", "capacity_growth_pct"
    )

# Write to Gold
energy_trends.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{GOLD_PATH}/energy_trend_summary")

print(f"✓ Wrote {energy_trends.count():,} rows to {GOLD_PATH}/energy_trend_summary")

# COMMAND ----------

# DBTITLE 1,Gold — Portfolio Risk Profile
# ============================================================================
# GOLD LAYER - PORTFOLIO RISK PROFILE
# ============================================================================

print("\n[GOLD] Computing Portfolio Risk Profile...")

# Read from Silver
stocks_silver = spark.read.format("delta").load(f"{SILVER_PATH}/stocks_clean")

# Calculate risk metrics by ticker
risk_profile = stocks_silver \
    .groupBy("ticker") \
    .agg(
        count("date").alias("trading_days"),
        spark_round(avg("daily_return"), 4).alias("avg_daily_return"),
        spark_round(stddev("daily_return"), 4).alias("volatility"),
        spark_round(_max("daily_return"), 4).alias("max_daily_gain"),
        spark_round(_min("daily_return"), 4).alias("max_daily_loss"),
        spark_round(avg("volume"), 0).cast("long").alias("avg_daily_volume"),
        spark_round(_max("close"), 2).alias("max_price"),
        spark_round(_min("close"), 2).alias("min_price")
    ) \
    .withColumn("sharpe_ratio_proxy",
                when(col("volatility") > 0,
                     spark_round(col("avg_daily_return") / col("volatility"), 4))
                .otherwise(0)) \
    .withColumn("risk_category",
                when(col("volatility") < 2.0, "Low")
                .when(col("volatility") < 4.0, "Medium")
                .otherwise("High")) \
    .withColumn("annualized_return_pct",
                spark_round(col("avg_daily_return") * 252, 2)) \
    .withColumn("annualized_volatility_pct",
                spark_round(col("volatility") * (252 ** 0.5), 2))

# Write to Gold
risk_profile.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{GOLD_PATH}/portfolio_risk_profile")

print(f"✓ Wrote {risk_profile.count():,} rows to {GOLD_PATH}/portfolio_risk_profile")
print("\n" + "="*80)
print("GOLD LAYER COMPLETE")
print("="*80 + "\n")

# COMMAND ----------

# DBTITLE 1,Validation & Summary
# MAGIC %md
# MAGIC ## Validation & Data Quality Summary
# MAGIC
# MAGIC **Purpose**: Verify pipeline execution and data integrity
# MAGIC
# MAGIC **Checks**:
# MAGIC * Row counts per table
# MAGIC * Data freshness (latest dates)
# MAGIC * Null value percentages
# MAGIC * Schema validation

# COMMAND ----------

# DBTITLE 1,Validation — Pipeline Summary
# ============================================================================
# VALIDATION & SUMMARY
# ============================================================================

print("[VALIDATION] Pipeline Execution Summary")
print("="*80 + "\n")

# Bronze Layer
print("BRONZE LAYER:")
for table in ["stocks_raw", "esg_raw", "energy_raw"]:
    try:
        count = spark.read.format("delta").load(f"{BRONZE_PATH}/{table}").count()
        print(f"  ✓ {BRONZE_PATH}/{table}: {count:,} rows")
    except Exception as e:
        print(f"  ✗ {BRONZE_PATH}/{table}: Not found")

print("\nSILVER LAYER:")
for table in ["stocks_clean", "esg_clean", "energy_clean"]:
    try:
        count = spark.read.format("delta").load(f"{SILVER_PATH}/{table}").count()
        print(f"  ✓ {SILVER_PATH}/{table}: {count:,} rows")
    except Exception as e:
        print(f"  ✗ {SILVER_PATH}/{table}: Not found")

print("\nGOLD LAYER:")
for table in ["stocks_moving_avg", "esg_sector_summary", "energy_trend_summary", "portfolio_risk_profile"]:
    try:
        count = spark.read.format("delta").load(f"{GOLD_PATH}/{table}").count()
        print(f"  ✓ {GOLD_PATH}/{table}: {count:,} rows")
    except Exception as e:
        print(f"  ✗ {GOLD_PATH}/{table}: Not found")

print("\n" + "="*80)
print("DATANEXUS BD PIPELINE COMPLETE")
print("="*80)
print(f"\nExecution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\nNext steps:")
print("  1. Run OPTIMIZE and ZORDER on Gold tables for query performance")
print("  2. Create Databricks AI/BI dashboards using dashboard_spec.sql")
print("  3. Schedule this notebook as a Databricks Workflow\n")