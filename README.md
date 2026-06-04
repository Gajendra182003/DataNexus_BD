# DataNexus BD — Big Data Lakehouse Analytics Pipeline

> A production-grade Medallion Architecture ETL pipeline on Databricks for semiconductor stock analysis, ESG risk assessment, and renewable energy trends.

---

## Overview

DataNexus BD implements the **Bronze → Silver → Gold** Medallion Architecture to ingest, clean, and analyze financial and sustainability datasets at scale. Built on Databricks Lakehouse with Apache Spark and Delta Lake.

```
DATA SOURCES ──► BRONZE (Raw Ingestion) ──► SILVER (Cleaned & Enriched) ──► GOLD (Analytics Ready) ──► DASHBOARDS
```

---

## Datasets

| Dataset | Records | Description |
|---|---|---|
| `stocks.csv` | 17,584 | Daily OHLCV data — NVDA, AMD, INTC, TSM, QCOM, ASML, MU |
| `esg.csv` | 800 | Quarterly ESG assessments by sector |
| `renewable_energy.csv` | 1,925 | Country-level renewable energy capacity by technology |
| **Total** | **20,309** | |

---

## Pipeline Layers

### 🥉 Bronze — Raw Ingestion
Ingest CSV files into Delta Lake with no transformations. Preserves raw data fidelity.

```python
spark.read.csv("data/stocks.csv", header=True, inferSchema=True) \
    .write.format("delta") \
    .save("bronze/stocks_raw")
```

### 🥈 Silver — Cleaning & Enrichment
Apply schema validation, null handling, and feature engineering.

```python
stocks_clean = stocks_raw \
    .dropna(subset=["close", "volume"]) \
    .withColumn("daily_return", (col("close") - col("open")) / col("open"))
```

### 🥇 Gold — Analytics Layer
Compute aggregations, moving averages, and risk metrics for dashboards.

```python
window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
stocks_ma = stocks_clean.withColumn("ma_7d", avg("close").over(window))
```

---

## Dashboard

**DataNexus BD Analytics** — 3 pages, 12+ visualizations

| Page | Visualizations |
|---|---|
| 📈 Stock Market | Price trends, 7/30-day moving averages, volatility & risk |
| 🌱 ESG Performance | Sector scores, quarterly trends, company comparisons |
| ⚡ Renewable Energy | Capacity growth by country and technology type |

---

## Project Structure

```
DataNexus_BD/
├── README.md
├── SETUP.md                      # Detailed setup guide
├── DataNexus_Pipeline.ipynb      # Main ETL notebook
├── data/
│   ├── stocks.csv
│   ├── esg.csv
│   └── renewable_energy.csv
└── scripts/
    └── fetch_real_bigdata.py
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Platform | Databricks Lakehouse |
| Processing | Apache Spark (PySpark) |
| Storage | Delta Lake |
| Governance | Unity Catalog |
| Data Source | Yahoo Finance API |
| Languages | Python, SQL |

---

## Quick Start

See [SETUP.md](./SETUP.md) for the full setup guide.

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/DataNexus_BD.git

# 2. Upload to Databricks and run
# Open DataNexus_Pipeline.ipynb → Run All
# Verify: validation summary should show SUCCESS
```

---

## Team

| Role | Responsibility |
|---|---|
| Project Leader | Architecture, coordination |
| Data Engineer | Bronze & Silver pipeline |
| Analytics Engineer | Gold layer & SQL models |
| Data Scientist | Statistical analysis & metrics |
| DevOps Engineer | Infrastructure & deployment |

---
