# RideNow Data Engineering Pipeline

A containerized, end-to-end data engineering pipeline built with PySpark. This project transforms raw taxi trip data into high-value, aggregated metrics using a Medallion Architecture (Bronze/Silver/Gold).

## 🚀 Pipeline Architecture
* **Bronze:** Raw Parquet/CSV ingestion.
* **Silver:** Data cleaning, type casting, surrogate key generation, and data quality validation.
* **Gold:** Business-level aggregations (Daily Revenue, Tip Rates) ready for BI consumption.
* **Quarantine:** Isolated storage for records failing validation, now including support for automated root-cause audit scripts.

## 🛠️ Engineering Highlights
* **Dead Letter Queue (Quarantine):** Invalid records are automatically routed to a `/quarantine` folder, ensuring auditability and zero data loss.
* **Data Quality Gates:** Implemented a 'fail-fast' approach; the pipeline utilizes assertions to halt execution if data doesn't meet quality standards.
* **Performance Optimization:** 
    * Implemented `.repartition()` and `.coalesce()` to solve the "Small File Problem," ensuring optimal Parquet file sizes for downstream storage and read performance.
    * Containerized with Docker to ensure a consistent, reproducible environment.
* **Idempotency:** Designed to allow safe, repeated runs without data duplication through partition-level overwrites.
* **SQL-Based Auditability:** Integrated DuckDB-ready SQL audit scripts to perform ad-hoc root-cause analysis on quarantined records, allowing for quick identification of data quality trends without requiring full pipeline re-execution.

## 🏗️ Getting Started
1. Clone the repository.
2. Run the pipeline:
   `docker compose up`
3. Processed results are available in `data/silver/` and `data/gold/`.
4. **Data Quality Auditing:** Use the provided SQL audit scripts to analyze quarantined records:
   `duckdb < audit_quarantine.sql`
