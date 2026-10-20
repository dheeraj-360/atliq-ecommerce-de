# AtliQ Commerce — End-to-End Data Engineering Capstone

An Azure-based end-to-end Data Engineering project that builds a scalable analytics pipeline for a fictional e-commerce business.

The objective is to extract operational and external source data, transform it into reliable analytical datasets, and enable business reporting using modern Azure Data Engineering technologies.

**Project Status:** Milestone 1 Completed | Milestones 2–7 Planned

---

## 1. Business Problem

AtliQ Commerce is a growing e-commerce company that stores transactional data such as customers, products, orders, order items, and payments in an operational (OLTP) database.

As the business grows, leadership needs answers to questions such as:

- How much revenue are we generating?
- Which products and categories perform best?
- Which cities contribute the most revenue?
- How many customers are new versus returning?
- How effectively do website visitors convert into paying customers?

Running complex analytical queries directly on the OLTP database can impact transactional performance.

**Solution:** Build a separate analytics lakehouse that ingests transactional and external data, cleans and transforms it, and makes it available for business intelligence reporting.

---

## 2. Project Architecture

The project follows a **Medallion Architecture (Bronze → Silver → Gold)** with nightly batch processing.

```text
             DATA SOURCES
                  |
       +----------+----------+
       |                     |
  Azure SQL DB          External Files
     (OLTP)             CSV / JSON
       |                     |
       +----------+----------+
                  |
                  v
          Azure Data Factory
       Metadata-Driven Ingestion
                  |
                  v
          ADLS Gen2 — Bronze
          Raw Data / Parquet
                  |
                  v
        Azure Databricks — Silver
        PySpark + Delta Lake
        Cleaning / MERGE
                  |
                  v
          dbt Core — Gold
          Star Schema / Facts
          Dimensions / Funnel
                  |
                  v
          Microsoft Fabric
          Power BI Dashboards

Cross-cutting:
Unity Catalog | Git | CI/CD
Data Quality | Monitoring
```

### Architecture Components

| Component | Responsibility |
|---|---|
| Azure SQL Database | Operational data source (OLTP) |
| Azure Data Factory | Data ingestion and orchestration |
| Azure Data Lake Storage Gen2 | Centralized cloud data storage |
| Azure Databricks | Distributed data processing using PySpark |
| Delta Lake | Reliable transactional tables |
| Unity Catalog | Lakehouse governance |
| dbt Core | SQL-based analytical transformations |
| Microsoft Fabric | Analytical reporting and dashboards |
| GitHub | Version control and CI/CD |

---

## 3. Project Milestones

| Milestone | Description | Status |
|---|---|---|
| M1 | Build OLTP Database and Transaction Simulator | ✅ Completed |
| M2 | Metadata-Driven Ingestion using ADF | Planned |
| M3 | Bronze to Silver using PySpark and Delta Lake | Planned |
| M4 | Gold Star Schema using dbt Core | Planned |
| M5 | Automate Nightly Incremental Pipeline | Planned |
| M6 | Reporting using Microsoft Fabric | Planned |
| M7 | CI/CD, Monitoring and Reliability | Planned |

---

## 4. Milestone 1 — Build the OLTP Database

**Status: Completed**

### Objective

Set up the operational database for AtliQ Commerce, load historical transactional data, and simulate new daily transactions to support incremental ingestion in subsequent milestones.

### Technologies Used

- Azure SQL Database
- SQL Server (T-SQL)
- Visual Studio Code
- MSSQL Extension
- Python
- pyodbc
- ODBC Driver 18 for SQL Server
- Git

### Implementation Steps

**Step 1 — Azure SQL Database Setup**

- Created Azure SQL Database using the Azure SQL Free Offer.
- Configured SQL authentication.
- Configured database networking and client IP access.
- Established a connection using the MSSQL extension in VS Code.

Database name: `atliq_commerce_db`

**Step 2 — OLTP Schema Creation**

Executed `01_schema_ddl.sql` to create five transactional tables:

1. `dbo.customers`
2. `dbo.products`
3. `dbo.orders`
4. `dbo.order_items`
5. `dbo.payments`

The database follows a normalized relational structure with primary-key and foreign-key relationships.

**Step 3 — Historical Data Loading**

Executed the supplied SQL INSERT scripts to populate the OLTP database.

### Initial Source Data Validation

| Table | Validated Records |
|---|---:|
| customers | 1,008 |
| products | 60 |
| orders | 10,000 |
| order_items | 23,155 |
| payments | 8,320 |

All five table counts matched the expected seed-data values.

**Step 4 — ETL Control Table**

Executed `07_etl_control_table.sql`.

The ETL control table stores metadata used by the ingestion pipeline, including:

- Source table information
- Load type (Full / Incremental)
- Watermark column
- Last loaded timestamp

This metadata will drive the generic ADF ingestion pipeline in Milestone 2.

### 4.1 Daily Transaction Simulator

Configured `daily_order_simulator.py` to generate new transactional data in Azure SQL.

**Purpose**

In a real e-commerce application, customers continuously place orders and make payments. Since this project does not include an actual storefront, the Python simulator reproduces transactional activity.

The simulator:

- Connects to Azure SQL using `pyodbc`.
- Selects existing customers and products.
- Generates new orders, order items, and payments.
- Uses UTC timestamps.
- Supports updating existing orders.
- Intentionally introduces data quality inconsistencies for downstream cleaning.

### Simulator Commands

Generate 20 new orders:

```bash
python scripts/daily_order_simulator.py --orders 20
```

Generate 20 orders and update up to five existing orders:

```bash
python scripts/daily_order_simulator.py --orders 20 --update-existing 5
```

Generate transactions without intentionally messy data:

```bash
python scripts/daily_order_simulator.py --orders 20 --clean
```

### Simulator Validation

Executed:

```bash
python scripts/daily_order_simulator.py --orders 20
```

Successfully generated 20 new orders.

| Metric | Result |
|---|---:|
| Initial Orders | 10,000 |
| Newly Generated Orders | 20 |
| Total Orders After Simulation | 10,020 |
| Existing Orders Updated | 0 |

Verified the updated order count directly in Azure SQL.

### 4.2 SQL Validation Queries

**Check source row counts**

```sql
SELECT 'customers' AS table_name,
       COUNT(*) AS row_count
FROM dbo.customers

UNION ALL
SELECT 'products', COUNT(*)
FROM dbo.products

UNION ALL
SELECT 'orders', COUNT(*)
FROM dbo.orders

UNION ALL
SELECT 'order_items', COUNT(*)
FROM dbo.order_items

UNION ALL
SELECT 'payments', COUNT(*)
FROM dbo.payments;
```

**Verify ETL control metadata**

```sql
SELECT *
FROM etl.control_table;
```

**Check recently inserted orders**

```sql
SELECT TOP 20
    order_id,
    customer_id,
    order_date,
    status,
    order_amount,
    created_at,
    updated_at
FROM dbo.orders
ORDER BY order_id DESC;
```

### 4.3 Key Learnings from Milestone 1

- Difference between OLTP and OLAP workloads.
- Creating and connecting to Azure SQL Database.
- Understanding normalized transactional tables.
- Executing SQL DDL and DML scripts.
- Validating source data using reconciliation queries.
- Understanding metadata-driven ingestion configuration.
- Connecting Python applications to Azure SQL using ODBC.
- Simulating INSERT and UPDATE operations.
- Understanding the importance of `created_at` and `updated_at` in incremental processing.
- Managing Azure resource costs and free-tier limits.

---

## 5. Upcoming Implementation

### Milestone 2 — Ingestion using Azure Data Factory

**Status: Planned**

- Configure Azure Data Factory and ADLS Gen2.
- Create linked services and parameterized datasets.
- Build a metadata-driven ingestion pipeline.
- Implement Full and Incremental loading.
- Use ETL control-table watermarks.
- Ingest SQL tables, CSV files, and clickstream JSON.
- Validate data landing in Bronze.

### Milestone 3 — Silver Transformation

**Status: Planned**

- Configure Azure Databricks and Unity Catalog.
- Read Bronze data using PySpark.
- Perform data cleaning and standardization.
- Implement Delta Lake MERGE.
- Handle missing, invalid, and duplicate data.
- Add data quality logging and event quarantine.

### Milestone 4 — Gold Data Modeling

**Status: Planned**

- Configure dbt Core with Databricks.
- Create staging models.
- Build fact and dimension tables.
- Implement star schema.
- Build website conversion funnel models.
- Add dbt data quality tests.

### Milestone 5 — Nightly Pipeline Automation

**Status: Planned**

- Schedule nightly ingestion and transformation.
- Implement safe incremental processing.
- Test pipeline retries and failure recovery.
- Validate idempotency.
- Implement end-to-end reconciliation.

### Milestone 6 — Microsoft Fabric Reporting

**Status: Planned**

- Connect Gold tables to Microsoft Fabric.
- Build Power BI reports.
- Revenue trend analysis.
- Best-selling product analysis.
- City-wise revenue analysis.
- New versus returning customers.
- Website conversion funnel.

### Milestone 7 — CI/CD and Reliability

**Status: Planned**

- Configure GitHub Actions.
- Implement automated dbt builds and tests.
- Manage credentials securely.
- Configure pipeline failure alerts.
- Implement execution logging and monitoring.
- Finalize project documentation.

---

## 6. Repository Structure

Current repository organization:

```text
atliq-ecommerce-de/
|
├── sql/
│   ├── 01_schema_ddl.sql
│   ├── 02_insert_customers.sql
│   ├── 03_insert_products.sql
│   ├── 04_insert_orders.sql
│   ├── 05_insert_order_items.sql
│   ├── 06_insert_payments.sql
│   ├── 07_etl_control_table.sql
│   └── validation queries
│
├── scripts/
│   └── daily_order_simulator.py
│
├── requirements.txt
├── .gitignore
└── readme.md
```

Additional directories for ADF, Databricks, dbt, documentation, and CI/CD will be added as implementation progresses.

---

## 7. Local Setup

### Prerequisites

- Python 3.10+
- ODBC Driver 18 for SQL Server
- Azure SQL Database
- VS Code
- MSSQL Extension

### Install Python Dependencies

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### Database Connection Configuration

Create a local `.env` file containing:

```dotenv
AZ_SQL_SERVER=your-server.database.windows.net
AZ_SQL_DB=atliq_commerce_db
AZ_SQL_USER=your_username
AZ_SQL_PASSWORD=your_password
```

**Security:** Never commit `.env` files, passwords, or access tokens to GitHub.

---

## 8. Data Quality Considerations

The starter dataset intentionally includes real-world data quality issues:

- Inconsistent city names.
- Non-standard order statuses.
- Test customer accounts.
- Duplicate order items.
- Payment gateway retries.
- Supplier pricing inconsistencies.
- Marketing channel variations.
- Malformed clickstream JSON.
- Bot-generated website events.

These issues will be addressed through Silver transformations and dbt staging models in upcoming milestones.

---

## 9. Security and Cost Management

- Using the Azure SQL Database Free Offer.
- Restricted database access using firewall rules.
- Enabled encrypted database connections.
- Kept database credentials outside Git.
- Configured `.gitignore` to exclude local secrets and Python virtual environments.
- Monitoring Azure resource usage and costs.

Azure Data Factory, Databricks, and other services may incur additional costs in later milestones.

---

## 10. Current Progress Summary

**Milestone 1 has been completed and validated.**

The operational Azure SQL database is ready, historical data has been loaded, and new transactions can be simulated using Python.

**Next Step:** Milestone 2 — Build the metadata-driven ingestion pipeline using Azure Data Factory and Azure Data Lake Storage Gen2.

---

## Project Reference

This project is based on the Codebasics *AtliQ Commerce — End-to-End Data Engineering Capstone: Technical Requirements & Step-by-Step Implementation Guide*.

The implementation follows the supplied project requirements, with engineering decisions, validations, and documentation added during development.