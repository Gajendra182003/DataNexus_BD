# DataNexus BD — Setup Guide

This guide walks you through setting up and running the DataNexus BD pipeline on Databricks from scratch.

---

## Prerequisites

Before you begin, make sure you have the following:

- **Databricks Workspace** with Unity Catalog enabled
- **Serverless Compute** or a cluster running Databricks Runtime 13.x+
- **Git** installed locally
- Python 3.9+ (for running `fetch_real_bigdata.py` locally, if needed)

---

## Step 1 — Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/DataNexus_BD.git
cd DataNexus_BD
```

---

## Step 2 — Upload to Databricks

### Option A: Databricks UI (Recommended)

1. Log in to your Databricks workspace.
2. In the left sidebar, go to **Workspace** → **Your email folder**.
3. Click the **⋮ menu** → **Import**.
4. Select **Folder** and upload the cloned `DataNexus_BD/` directory.
5. Confirm the structure looks like:

```
/Users/YOUR_EMAIL/DataNexus_BD/
├── DataNexus_Pipeline.ipynb
├── data/
│   ├── stocks.csv
│   ├── esg.csv
│   └── renewable_energy.csv
└── scripts/
    └── fetch_real_bigdata.py
```

### Option B: Databricks CLI

```bash
# Install CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload folder
databricks workspace import_dir DataNexus_BD /Users/YOUR_EMAIL/DataNexus_BD
```

---

## Step 3 — Configure Compute

1. Open **DataNexus_Pipeline.ipynb** in Databricks.
2. In the top-right corner, click **Connect** and select your cluster or enable **Serverless Compute**.
3. Ensure the cluster has the following libraries (auto-installed on Databricks Runtime 13+):
   - `pyspark`
   - `delta-spark`
   - `yfinance` *(only needed if regenerating data via `fetch_real_bigdata.py`)*

> **Note**: If using Unity Catalog, ensure your user has `CREATE SCHEMA` and `CREATE TABLE` privileges on the target catalog.

---

## Step 4 — (Optional) Regenerate Data

The `data/` folder includes pre-fetched CSVs ready to use. To refresh the data from Yahoo Finance:

```bash
# Install dependencies
pip install yfinance pandas

# Run the fetch script
python scripts/fetch_real_bigdata.py
```

This will overwrite `stocks.csv`, `esg.csv`, and `renewable_energy.csv` with the latest data.

---

## Step 5 — Run the Pipeline

1. Open **DataNexus_Pipeline.ipynb**.
2. Click **Run All** (or `Shift + Enter` through each cell).
3. The pipeline will execute in order:
   - **Bronze**: Raw CSV ingestion → Delta tables
   - **Silver**: Cleaning, null handling, feature engineering
   - **Gold**: Aggregations, moving averages, risk metrics

---

## Step 6 — Verify the Output

At the end of the notebook, a **validation summary** will print. You should see:

```
✅ Bronze layer: SUCCESS — 20,309 records ingested
✅ Silver layer: SUCCESS — schema validated, nulls handled
✅ Gold layer:   SUCCESS — analytics tables created
```

If any layer shows `FAILED`, check the cell output above it for error details.

---

## Step 7 — View the Dashboard

1. In Databricks, navigate to **SQL** → **Dashboards**.
2. Open **DataNexus BD Analytics**.
3. The dashboard has 3 pages:
   - **Stock Market** — price trends, moving averages, volatility
   - **ESG Performance** — sector scores, quarterly trends
   - **Renewable Energy** — capacity growth by country and technology

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `AnalysisException: Table not found` | Re-run Bronze layer cells first |
| `Permission denied` on Delta write | Check Unity Catalog privileges |
| Cluster terminated mid-run | Re-attach cluster and re-run from failed cell |
| `ModuleNotFoundError: yfinance` | Install via `%pip install yfinance` in a notebook cell |
| Dashboard shows no data | Ensure Gold layer ran successfully and tables exist |

---

## Unity Catalog Setup (If Required)

If your workspace uses Unity Catalog, set the catalog and schema at the top of the notebook:

```python
spark.sql("USE CATALOG your_catalog_name")
spark.sql("CREATE SCHEMA IF NOT EXISTS datanexus")
spark.sql("USE SCHEMA datanexus")
```

Replace `your_catalog_name` with your workspace's catalog (e.g., `main` or `dev`).

---

*For questions, reach out to the Project Leader or open a GitHub Issue.*
