# Project Summary
A Databricks project "Exchange-Rate-Frankfurter-DB" implementing a Medallion Architecture (Bronze, Silver, Gold) to analyze the exchange rates, volatility of certain currencies, and some more metrics.

## Overview
This project transforms raw foreign exchange rate data from the Frankfurter API into clean, business-ready analytical datasets using Databricks. The automated ETL pipeline standardizes currency pairs, computes cross-currency conversions, tracks daily market volatility, and generates periodic financial accounting aggregates for executive reporting and BI integration.

## Data Architecture (Medallion Pipeline)
1. Bronze Layer (Raw Ingestion): Fetches daily exchange rate records directly from the Frankfurter API and stores them in Delta Lake format without modification.
2. Silver Layer (Cleaning & Enrichment): Normalizes schemas (casts strings to explicit DateType and DecimalType), removes redundant self-conversion pairs, enforces deduplication via PySpark window functions, and derives inverse currency exchange rates.
3. Gold Layer (Analytics & Aggregates): Materializes business-level views and fact tables powered by window functions and rolling aggregations optimized for downstream analytics and BI tools (Power BI, Tableau).

## Tools & Technologies
- Platform: Databricks (Lakehouse Architecture & Delta Lake)
- Languages: Python / PySpark (pyspark.sql, Window Functions, Aggregations), Databricks SQL
- API / Data Source: Frankfurter Currency API
- Integration & BI: Power BI (via Partner Connect / DirectQuery) & Delta Table exports

## Key Analysis & Gold Layer Deliverables

### 1. 

### Key Insights

### 2. 

### Key Insights
