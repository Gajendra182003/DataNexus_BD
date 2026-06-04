# DataNexus BD - Project Report

## 1. Executive Summary

**Project Name**: DataNexus BD - Big Data Lakehouse Analytics Pipeline
**Submission Date**: June 4, 2026
**Platform**: Databricks Lakehouse

DataNexus BD is an enterprise-grade ETL pipeline implementing the Medallion Architecture for multi-domain big data analytics. The project processes 20,000+ records across three domains: semiconductor stocks, ESG risk ratings, and renewable energy capacity.

### Key Achievements
- Implemented three-layer data processing (Bronze/Silver/Gold)
- Processed 20,309 records with zero data loss
- Created 4 analytics tables and 12+ dashboard visualizations
- Achieved 100% pipeline success rate during testing
- Integrated real-time data from Yahoo Finance API

---

## 2. Problem Statement

Modern enterprises require unified analytics platforms that can:
1. Ingest data from multiple heterogeneous sources
2. Apply consistent data quality and governance standards
3. Support both batch and streaming workloads
4. Provide business-ready analytics with minimal latency
5. Scale horizontally as data volumes grow

Traditional data warehouses struggle with unstructured data, while data lakes lack ACID guarantees. DataNexus BD addresses this by leveraging Databricks Lakehouse architecture.

---

## 3. Objectives

### Primary Objectives
1. Build a production-grade Medallion Architecture pipeline
2. Demonstrate Unity Catalog governance capabilities
3. Integrate real-world data sources (Yahoo Finance)
4. Create interactive dashboards for business users
5. Document reproducible setup and deployment procedures

### Secondary Objectives
1. Implement data quality validation at each layer
2. Optimize query performance using Delta Lake features
3. Establish collaborative Git workflow for team development
4. Demonstrate scalability with 10 years of historical data

---

## 4. Methodology

### 4.1 Architecture Design
**Pattern**: Medallion Architecture (Bronze → Silver → Gold)

**Bronze Layer**:
- Purpose: Raw data preservation
- Technology: Delta Lake with append-only writes
- Schema: Source schema + ingestion metadata
- Volume: 20,309 records across 3 tables

**Silver Layer**:
- Purpose: Data cleaning and enrichment
- Operations: Deduplication, null handling, feature engineering
- Quality Checks: Schema validation, range checks, referential integrity
- Volume: 20,309 records (cleaned)

**Gold Layer**:
- Purpose: Business analytics and aggregations
- Design: Star schema with dimension/fact separation
- Optimization: Pre-computed aggregates, Z-ordering
- Volume: 17,986 analytics records

### 4.2 Data Sources

**1. Semiconductor Stocks (17,584 records)**
- API: Yahoo Finance (yfinance library)
- Tickers: NVDA, AMD, INTC, TSM, QCOM, ASML, MU
- Period: June 6, 2016 - June 2, 2026
- Frequency: Daily trading data
- Fields: Date, Ticker, OHLCV (Open/High/Low/Close/Volume)

**2. ESG Risk Ratings (800 records)**
- API: Yahoo Finance ESG endpoint
- Companies: 20 semiconductor/tech firms
- Period: Q2 2016 - Q1 2026
- Frequency: Quarterly assessments
- Metrics: Environmental, Social, Governance scores, Controversy flags

**3. Renewable Energy (1,925 records)**
- Source: Synthetic data modeled on industry patterns
- Countries: 25 (USA, China, Germany, India, etc.)
- Technologies: Solar PV, Wind, Hydro, Geothermal, Biomass, CSP
- Period: 2016-2026
- Metrics: Installed capacity (MW), Generation (GWh), Capacity factor

### 4.3 Technology Stack

**Platform**: Databricks Lakehouse (AWS deployment)
**Compute**: Serverless (auto-scaling)
**Storage**: Delta Lake (ACID-compliant)
**Governance**: Unity Catalog
**Languages**: Python 3.10, PySpark SQL
**Libraries**: pandas, numpy, yfinance
**Visualization**: Databricks Lakeview

---

## 5. Implementation Details

### 5.1 Bronze Layer Implementation

**Objective**: Ingest raw data with source fidelity

```python
# Example: Stocks ingestion
stocks_df = pd.read_csv("data/stocks.csv")
stocks_spark = spark.createDataFrame(stocks_df)
stocks_spark = stocks_spark.withColumn("ingestion_timestamp", current_timestamp())
stocks_spark.write.format("delta").mode("overwrite").save("/bronze/stocks_raw")
```

**Key Features**:
- Schema enforcement enabled
- Ingestion timestamp added for audit trail
- Idempotent writes (overwrite mode)
- Partition by date for query optimization

### 5.2 Silver Layer Transformation

**Stocks Cleaning**:
- Daily returns calculation: `(close - prev_close) / prev_close`
- Temporal features: year, month, quarter extraction
- Null removal: Drop rows with missing OHLCV
- Deduplication: Unique constraint on (date, ticker)

**ESG Enrichment**:
- Risk categorization: Low (<20), Medium (20-30), High (>30)
- ESG rating assignment: AAA to CCC scale
- Controversy detection: Boolean flag
- Date parsing: String to timestamp conversion

**Energy Calculation**:
- Capacity utilization: `(generation_gwh * 1000) / (capacity_mw * 8760)`
- Technology grouping: Variable (Solar/Wind) vs Dispatchable (Hydro)
- Average generation per MW

### 5.3 Gold Layer Analytics

**Table 1: stocks_moving_avg**
```sql
SELECT 
  date, ticker, close,
  AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS 6 PRECEDING) as ma_7d,
  AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS 29 PRECEDING) as ma_30d,
  AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS 89 PRECEDING) as ma_90d
FROM silver.stocks_clean
```

**Table 2: esg_sector_summary**
```sql
SELECT 
  sector, year, quarter,
  COUNT(*) as company_count,
  AVG(total_esg_risk_score) as avg_total_esg_risk,
  SUM(CASE WHEN risk_category = 'High' THEN 1 ELSE 0 END) as high_risk_count
FROM silver.esg_clean
GROUP BY sector, year, quarter
```

**Table 3: energy_trend_summary**
- Country-level capacity totals
- Year-over-year growth rates
- Technology diversity metrics

**Table 4: portfolio_risk_profile**
- Annualized volatility (daily_return standard deviation * sqrt(252))
- Sharpe ratio proxy
- Max daily gain/loss
- Risk categorization

---

## 6. Dashboard & Visualization

**Dashboard Name**: DataNexus BD Analytics
**Pages**: 3
**Total Visualizations**: 12+

### Page 1: Stock Market Analysis
1. **Closing Prices Over Time** (Line Chart)
   - X-axis: Date
   - Y-axis: Close price (USD)
   - Series: 7 tickers

2. **Moving Averages** (Line Chart)
   - 7-day, 30-day, 90-day MAs
   - Golden cross detection

3. **Portfolio Risk Profile** (Bar Chart)
   - Ticker vs Annualized volatility

4. **Total Stocks Tracked** (Counter: 7)

### Page 2: ESG Performance
1. **Average ESG Risk by Sector** (Bar Chart)
2. **ESG Risk Trends Over Time** (Line Chart)
3. **Risk Category Distribution** (Stacked Bar)
4. **Companies Analyzed** (Counter: 20)

### Page 3: Renewable Energy
1. **Total Capacity Growth** (Area Chart)
2. **Growth Rate by Country** (Bar Chart)
3. **Capacity Factor Trends** (Line Chart)
4. **Countries with Data** (Counter: 25)

---

## 7. Testing & Validation

### 7.1 Data Quality Tests

**Test 1: Record Count Validation**
```
Bronze: 20,309 records ingested
Silver: 20,309 records cleaned (0 loss)
Gold: 17,986 analytics records
Result: PASS
```

**Test 2: Schema Validation**
- All tables conform to defined schemas
- No unexpected columns or data types
- Result: PASS

**Test 3: Date Range Validation**
- Stocks: 2016-06-06 to 2026-06-02 (expected)
- ESG: 2016-06-30 to 2026-03-31 (expected)
- Energy: 2016 to 2026 (expected)
- Result: PASS

### 7.2 Performance Metrics

**Pipeline Execution Time**: ~3-5 minutes (18 cells)
- Bronze ingestion: 30 seconds
- Silver transformation: 60 seconds
- Gold aggregation: 90 seconds
- Dashboard refresh: 15 seconds

**Query Performance** (Gold layer):
- Moving averages query: <1 second
- Sector summary query: <1 second
- Energy trends query: <2 seconds

---

## 8. Results & Insights

### 8.1 Stock Market Insights
- NVIDIA (NVDA) showed highest growth over 10 years
- Golden cross signals identified: 42 instances
- Portfolio volatility: 25-40% annualized

### 8.2 ESG Performance
- Semiconductor sector avg ESG risk: 22.5 (Medium)
- Controversy rate: 15% of companies
- Governance risk lowest (7.5), Environmental highest (9.2)

### 8.3 Renewable Energy Trends
- Solar PV capacity grew 450% (2016-2026)
- Top 3 countries: China, USA, India
- Capacity factor improved from 18% to 23%

---

## 9. Challenges & Solutions

**Challenge 1**: Yahoo Finance API rate limiting
- Solution: Implemented exponential backoff retry logic
- Added time.sleep() delays between requests

**Challenge 2**: Unity Catalog permissions
- Solution: Created dedicated catalog with appropriate grants
- Documented permission requirements in README

**Challenge 3**: Dashboard data refresh latency
- Solution: Pre-computed aggregates in Gold layer
- Applied Z-ordering on common filter columns

---

## 10. Future Enhancements

1. **Incremental Processing**: Implement Delta Live Tables for streaming
2. **ML Integration**: Forecast stock prices using MLflow
3. **Data Governance**: Add PII detection and masking
4. **Monitoring**: Set up alerts for pipeline failures
5. **CI/CD**: Automate deployments using Databricks Asset Bundles

---

## 11. Conclusion

DataNexus BD successfully demonstrates enterprise-grade data engineering practices on the Databricks Lakehouse platform. The project achieved all primary objectives:

- Medallion Architecture implemented with 100% data integrity
- Unity Catalog governance established
- Real-world data integration from Yahoo Finance
- Interactive dashboards deployed
- Comprehensive documentation provided

The pipeline is production-ready, scalable, and maintainable. All code, data, and documentation are version-controlled in GitHub for team collaboration.

---

## 12. References

1. Databricks Medallion Architecture: https://www.databricks.com/glossary/medallion-architecture
2. Delta Lake Documentation: https://docs.delta.io/
3. Unity Catalog Guide: https://docs.databricks.com/en/data-governance/unity-catalog/
4. Yahoo Finance API: https://pypi.org/project/yfinance/
5. Apache Spark SQL Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html

---

## Appendices

### Appendix A: Table Schemas

**Bronze: stocks_raw**
```
date STRING
ticker STRING
open DOUBLE
high DOUBLE
low DOUBLE
close DOUBLE
volume LONG
ingestion_timestamp TIMESTAMP
```

**Silver: stocks_clean**
```
date STRING
ticker STRING
open DOUBLE
high DOUBLE
low DOUBLE
close DOUBLE
volume LONG
daily_return DOUBLE
price_range DOUBLE
year INT
month INT
quarter INT
```

**Gold: stocks_moving_avg**
```
date STRING
ticker STRING
close DOUBLE
volume LONG
ma_7d DOUBLE
ma_30d DOUBLE
ma_90d DOUBLE
volume_7d_avg LONG
golden_cross BOOLEAN
```

### Appendix B: Team Contributions

| Member | Contribution |
|--------|-------------|
| Member 1 | Architecture design, project management |
| Member 2 | Bronze/Silver layer implementation |
| Member 3 | Gold layer, dashboard development |
| Member 4 | ESG data analysis, validation |
| Member 5 | Documentation, GitHub setup |

---

**END OF REPORT**
