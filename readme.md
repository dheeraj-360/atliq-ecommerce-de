# AtliQ Commerce — End-to-End Data Engineering Capstone

An Azure-based retail analytics pipeline built using **Azure SQL, Azure Data Factory, ADLS Gen2, Azure Databricks, Delta Lake, dbt Core, Microsoft Fabric, and GitHub Actions**.

The project separates transactional operations from analytics, ingests and cleans intentionally messy retail data, builds dimensional models, and presents revenue, customer, product, and website-conversion insights.

> **Status (10 November 2026):** M1–M7 implemented, including a Fabric dashboard, scheduled pipeline, audit logging, email notifications, and successful dbt CI. Final seed-checkpoint reconciliation and repeat-run idempotency evidence should be attached before submission if not already captured. The implemented CI workflow validates Pull Requests; it does **not** automatically deploy code to production.

## 1. Business problem

AtliQ Commerce processes customers, products, orders, order items, and payments in a normalized operational (OLTP) database. Running large analytical queries against that database risks affecting operational workloads. Leadership needs a separate reporting layer to answer:

- How does revenue change over time?
- Which products, categories, and cities contribute the most revenue?
- How many purchasing customers are new versus returning?
- How do website sessions progress from product view to purchase?

**Solution:** Land operational and external data in a lakehouse, clean it with PySpark and Delta Lake, model analytics with dbt, and serve dashboards with Microsoft Fabric/Power BI.

## 2. Architecture

```text
                        DATA SOURCES
          Azure SQL (OLTP)       External files
          5 transaction tables   Supplier CSV | Marketing CSV | Clickstream JSON
                     \           /
                      \         /
                 Azure Data Factory (ADF)
             Metadata-driven full/incremental copy
                           |
                           v
                  ADLS Gen2 — BRONZE
          Raw Parquet (SQL), landed CSV and JSON files
                           |
                           v
               Azure Databricks — SILVER
       PySpark cleaning, Delta MERGE, quarantine, DQ log
               Unity Catalog: atliq_dl.silver
                           |
                           v
                    dbt Core — GOLD
         Staging views, facts, dimensions, dbt tests
               Unity Catalog: atliq_dl.gold
                           |
                           v
             Microsoft Fabric / Power BI
             Executive + Customer/Conversion

   Orchestration: ADF nightly master pipeline / Databricks jobs
   Reliability: Azure SQL audit log, run alerts, DQ logging
   Version control + CI: GitHub / GitHub Actions / atliq_dl.ci
```

**OLTP versus OLAP:** Azure SQL retains normalized tables optimized for transactional writes. The separate Gold star schema is optimized for reporting reads, so analytical queries do not have to scan the operational database directly.

### Technologies

| Layer | Technology | Responsibility |
|---|---|---|
| Operational source | Azure SQL Database, T-SQL | Orders, customers, products, items, payments; ETL metadata |
| File sources | CSV and JSON Lines | Supplier history, marketing spend, website events |
| Ingestion/orchestration | Azure Data Factory | Parameterized ingestion, watermarks, scheduled pipelines |
| Data lake | Azure Data Lake Storage Gen2 | Bronze file storage and Delta locations |
| Processing | Azure Databricks, PySpark, Delta Lake | Silver standardization, incremental MERGE, data quality |
| Governance | Unity Catalog (`atliq_dl`) | Catalog and schema organization |
| Analytics modeling | dbt Core + Databricks | Gold facts/dimensions and tests |
| Reporting | Microsoft Fabric / Power BI | Business dashboards |
| Source control and CI | GitHub + GitHub Actions | PR-triggered dbt builds/tests |

## 3. Milestone implementation

| Milestone | Implementation | Status |
|---|---|---|
| M1 — OLTP | Azure SQL schema, seed data, control table, Python transaction simulator | Completed |
| M2 — Ingestion | ADF metadata-driven SQL ingest and external file landing | Completed |
| M3 — Silver | Databricks/PySpark cleaning, Delta MERGE, DQ logging, web-event processing | Completed |
| M4 — Gold | dbt staging views, star-schema models and web-session fact, tests | Completed |
| M5 — Nightly sync | ADF orchestration and scheduled trigger; incremental processing | Implemented; repeat-run proof to document |
| M6 — Reporting | Microsoft Fabric semantic model and Power BI dashboards | Completed |
| M7 — Reliability/CI | GitHub Actions CI, Azure SQL run audit, success/failure email alerts | Implemented; evidence to collect |

### M1 — Operational database and simulator

Five transactional tables were created using the supplied SQL assets: `dbo.customers`, `dbo.products`, `dbo.orders`, `dbo.order_items`, and `dbo.payments`. `etl.control_table` stores ingestion metadata, including load type, watermark column and last-loaded timestamp.

**Original seed counts (before simulation and cleaning):**

| Source table | Seed rows |
|---|---:|
| customers | 1,008 |
| products | 60 |
| orders | 10,000 |
| order_items | 23,155 |
| payments | 8,320 |

The Python simulator adds realistic new orders, associated items and payments, and can advance existing order statuses. One documented local test inserted 20 orders (10,000 → 10,020).

```bash
python scripts/daily_order_simulator.py --orders 20
python scripts/daily_order_simulator.py --orders 20 --update-existing 5
python scripts/daily_order_simulator.py --orders 20 --clean
```

Use the script path that actually exists in the cloned repository. The simulator reads Azure SQL connection settings from a local `.env` file (never commit secrets).

### M2 — Metadata-driven ADF ingestion

ADF reads configuration from `etl.control_table` and applies the appropriate load mode:

- **Full load:** reload smaller reference tables, such as customers/products.
- **Incremental load:** use a timestamp watermark for orders (`updated_at`), order items (`created_at`) and payments (`updated_at`).
- **Run boundary:** capture a run-start UTC timestamp and advance a table's watermark only after a successful copy. This avoids skipping source records arriving during the run.
- **External files:** land supplier and marketing CSVs and preserve clickstream JSON Lines in Bronze for downstream parsing.

The ADF source JSON is maintained in the GitHub collaboration branch under `adf/`. The separate `adf_publish` branch contains generated deployment templates and **should not be merged into `main` merely because ADF Publish updated it**.

The existing nightly ADF orchestration and trigger are version-controlled (including the `tr_atliq_nightly_0600_ist` trigger). See the actual ADF JSON for current task dependencies and parameters.

### M3 — Silver layer (PySpark + Delta Lake)

Databricks notebooks under `databricks/notebooks/` implement source-specific cleaning and write governed Delta tables in `atliq_dl.silver`.

Examples of source-data rules:

- Normalize cities, emails, product/category labels, order statuses and payment methods.
- Flag QA accounts (`@atliq-test.com`) for exclusion from business reporting.
- Replace placeholder order dates with the appropriate creation date.
- Merge updated orders/payments by business key rather than blindly appending new versions.
- Parse supplier costs and marketing data defensively, including inconsistent formatting and duplicate rows.
- Quarantine malformed clickstream JSON, deduplicate by event ID, reject invalid sessions/products and remove bot sessions.
- Record cleaning-rule effects in `atliq_dl.silver.dq_log`.

Silver preserves auditable records where business-level deduplication is intentionally deferred to dbt staging.

**Incremental behavior:** `MERGE` updates a matching order/payment when the incoming version is newer and inserts previously unseen records. Insert-only order items use insert semantics. Empty incremental batches should be handled safely rather than treated as missing required data.

### M4 — dbt Gold layer

The dbt project lives in `atliq_gold/` and reads Silver through declared dbt sources. Staging views implement business filtering/deduplication, and Gold models support a star schema and conversion analysis.

**Gold models:**

| Model | Role / grain |
|---|---|
| `fact_sales` | One cleaned order-item row; quantity, price, revenue and order status |
| `dim_customers` | Customer attributes and first-order information |
| `dim_product` | Product/category and latest supplier-cost attributes |
| `dim_date` | Calendar attributes for consistent date filtering |
| `fact_web_sessions` | One row per website session with funnel-stage flags |

The actual dbt model is named `dim_customers` (plural), as confirmed by CI logs. The source guide uses `dim_customer` as an illustrative name.

**Business rules:** Gold excludes test-customer orders; staging removes double-submitted lines and duplicate payment retries; website funnel metrics count sessions reaching each stage. Gross revenue excludes cancelled orders, while returned revenue should be separated to calculate net revenue in reporting.

### M5 — Nightly orchestration and run audit

The master ADF pipeline orchestrates ingestion and downstream transformations, using Databricks processing and dbt Gold builds. A scheduled trigger and success/failure email notifications have been configured.

`sql/pipeline_run_audit.sql` defines `etl.pipeline_run_audit` and `etl.usp_log_pipeline_run` in Azure SQL. The audit stores:

- Run ID, pipeline name, UTC run date and start/finish timestamps
- Status (`Running`, `Succeeded`, `Failed`)
- Current source-table totals (orders, order items, payments) on success
- Failure stage/message where available

**Clarification:** `source_*_rows` are current Azure SQL source totals, **not** per-batch copy counts or Gold fact rows.

**Idempotency evidence (to capture/attach):** run the complete nightly pipeline twice with no intervening source changes or simulator execution, and compare the following Gold result after each successful run:

```sql
SELECT
    COUNT(*) AS fact_rows,
    SUM(gross_revenue) AS total_gross_revenue
FROM atliq_dl.gold.fact_sales;
```

Both values must remain unchanged. Save screenshots/outputs as `docs/idempotency_before.png` and `docs/idempotency_after.png` (or equivalent). A successful pipeline run alone does not prove idempotency.

### M6 — Microsoft Fabric / Power BI dashboard

The Fabric semantic model (`sm_atliq_commerce`) uses Gold tables to drive two main report pages:

- **Overview:** revenue, orders, cancellation/return indicators, revenue trend, top products and cities, with filtering controls.
- **Customer & Conversion Analysis:** active/purchasing customers, session/purchase conversion KPIs, new versus returning customers and the website conversion funnel.

The required five analytical views are covered: revenue trend, top products, top cities, new vs returning purchasing customers, and sessions → product views → add to cart → checkout → purchases. A hidden working page (`New_Vs_Returning`) was used while developing the report.

> The dashboard uses refreshed data that may include simulator-generated orders. Avoid comparing an unfiltered current dashboard directly with historical **seed-only** checkpoints.

### M7 — GitHub Actions CI and reliability

GitHub Actions runs the project build and configured dbt tests automatically when a Pull Request targets `main`:

```text
Feature branch -> Pull Request to main -> GitHub Actions
  -> install Python/dbt -> dbt deps -> dbt build --target ci
  -> check result -> review and merge
```

- Workflow: `.github/workflows/ci.yml`
- Profile: `atliq_gold/profiles.yml`, using GitHub Secrets `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`
- Production target: `atliq_dl.gold`
- Isolated CI target: `atliq_dl.ci`
- Gold `location_root` uses `{{ target.schema }}` so CI files use a separate ADLS location.

**Verified CI result (10 November 2026):**

| Result | Count |
|---|---:|
| Table models created | 5 |
| Staging view models created | 6 |
| dbt data tests passed | 8 |
| **Total successful nodes** | **19 / 19** |
| Warnings / errors / skips | **0 / 0 / 0** |
| dbt build duration (reported) | **34.42 seconds** |

The CI logs explicitly show models created in `ci`, including `ci.fact_sales`, `ci.dim_product`, and `ci.fact_web_sessions`. Tests cover uniqueness, not-null checks, accepted order statuses, and relationships between sales facts and dimensions.

**Scope:** This is **CI**, not an automated production deployment. Merging a Pull Request changes GitHub source code; a separate production execution is required to update `atliq_dl.gold`.

Failure/success email notifications are configured for the pipeline. The separate Azure SQL audit table records orchestration history.

## 4. Data-quality validation

The supplied dataset intentionally contains messy source data. The implementation guide provides reference checkpoints for the original seed period (through **31 August 2026**) after cleaning. These values are **expected benchmarks**, not claims about the latest live tables.

| Seed-period check | Guide benchmark |
|---|---:|
| Customers excluding QA | 1,000 |
| Products | 60 |
| Valid orders excluding QA | 9,961 |
| Clean fact sales items | 22,959 |
| Payments after deduplication | 8,183 |
| Gross revenue excluding cancelled | 32,424,661.00 |
| Returned revenue | 4,574,944.00 |
| Net revenue | 27,849,717.00 |
| Supplier current-cost rows | 60 |
| Clean marketing rows | 3,037 |
| Clean web events | 93,436 |
| Web sessions | 23,193 |
| Sessions reaching purchase | 3,446 |

Seed clickstream funnel benchmark: **23,193 sessions → 13,309 product views → 7,391 add to cart → 4,442 checkout → 3,446 purchases** (approximately **14.86%** session-to-purchase conversion).

**Evidence to attach:** capture the actual seed-scoped reconciliation, plus `atliq_dl.silver.dq_log` rule counts. Where the simulator or updates alter records, describe the differences rather than claiming an exact benchmark match.

Example DQ summary query:

```sql
SELECT run_date, table_name, rule, SUM(rows_affected) AS rows_affected
FROM atliq_dl.silver.dq_log
GROUP BY run_date, table_name, rule
ORDER BY run_date DESC, table_name, rule;
```

## 5. Repository layout

The structure below describes the intended source-controlled components; names/files should be checked against the actual repository before submission.

```text
atliq-ecommerce-de/
├── sql/                         # OLTP seed/control scripts + pipeline_run_audit.sql
├── scripts/                     # Daily order simulator
├── adf/                         # Factory pipelines, datasets, linked services, triggers
├── databricks/
│   ├── notebooks/               # Silver processing and DQ utilities
│   └── jobs/                    # Exported atliq_silver_pipeline job definition
├── atliq_gold/
│   ├── models/                  # Staging and Gold SQL models / schema tests
│   ├── dbt_project.yml
│   └── profiles.yml             # env_var() only; no literal secrets
├── .github/
│   └── workflows/
│       └── ci.yml               # PR-triggered dbt CI
├── docs/                        # Screenshots, validation, architecture evidence
├── .gitignore
├── requirements.txt
└── README.md
```

`adf_publish` is a separate ADF-generated deployment branch, **not** an additional directory to merge into `main`.

## 6. Running the project

### Prerequisites

- Azure SQL and ADLS access, Azure Data Factory, an Azure Databricks workspace with Unity Catalog, and Fabric reporting access
- Python 3.10+ with ODBC Driver 18 for the simulator
- dbt Core with the Databricks adapter for Gold modeling
- Git and VS Code

### Local simulator

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a local `.env` based on `.env.example`, with the appropriate `AZ_SQL_SERVER`, `AZ_SQL_DB`, `AZ_SQL_USER`, and `AZ_SQL_PASSWORD` variables. **Never commit `.env` or tokens.**

### dbt

From `atliq_gold/`, run dbt using a properly configured connection. The project's checked-in `profiles.yml` obtains connection details from environment variables:

```powershell
$env:DATABRICKS_HOST = "<workspace-host-without-https>"
$env:DATABRICKS_HTTP_PATH = "<sql-warehouse-http-path>"
# Set DATABRICKS_TOKEN securely in your local environment; do not paste it into code or Git.
dbt deps
dbt build --target ci
```

The `ci` target builds **test** models; it is not a production deployment. Normal local workflows may use a separate profile in the user's `.dbt` directory. Specify `--profiles-dir` explicitly when you need to select a particular profile location.

### GitHub CI

Open a Pull Request from a feature branch into `main`. The workflow runs `dbt deps` and `dbt build --target ci` using GitHub repository secrets. Check the PR **Checks** tab and GitHub Actions logs for the summary.

## 7. Security, operations and limitations

- Credentials are handled via environment variables, GitHub Secrets, and local ignored `.env` files; no credentials belong in source control.
- The ADF collaboration branch holds editable factory JSON; ADF Publish maintains separate deployment artifacts in `adf_publish`.
- Git-connected Databricks notebooks and the exported job JSON are source-controlled; **changes made to the job in the Databricks UI must be exported and committed again** until job-as-code deployment is implemented.
- CI currently uses a shared persistent `ci` schema: successive builds can replace previous CI tables; it is not a per-PR disposable environment.
- Pipeline alerts and audit logs aid troubleshooting but do not, by themselves, prove data correctness.
- Azure services may incur costs. Monitor resource usage and stop/delete resources after submission if no longer needed.

## 8. Submission evidence checklist

- [x] Source code for M1–M4 in GitHub (SQL, ADF, Databricks, dbt)
- [x] Databricks Silver job configuration exported to GitHub
- [x] Fabric dashboard created
- [x] GitHub Actions dbt CI passed — **19/19**, no errors
- [x] Success/failure email notifications configured
- [x] Audit table/procedure implemented and logging confirmed
- [ ] Attach successful nightly trigger/run screenshot
- [ ] Attach idempotency before/after Gold query results
- [ ] Attach actual seed-scoped DQ reconciliation and rule-count report
- [ ] Attach Fabric dashboard and CI logs/screenshots
- [ ] Confirm README file paths match the final repository structure
- [ ] Verify no credentials/tokens were committed, including Git history

These unchecked items are **submission evidence or checks still to be confirmed**, not claims that those features were never implemented.

## 9. Key engineering decisions

1. **OLTP/OLAP separation:** Protect transaction-serving workloads from expensive analytical reporting.
2. **Control-table watermarks:** Reuse one metadata-driven ingestion pattern; use UTC run-start boundaries and update watermarks only after successful copies.
3. **Bronze before cleaning:** Preserve incoming source data for debugging, audit, and replay.
4. **Silver Delta MERGE:** Apply updated source records using keys and timestamp comparisons; do not append mutable entities into Silver blindly.
5. **Explicit data-quality rules:** Standardize messy values, quarantine bad events, record cleaning counts, and use dbt tests for final model checks.
6. **Idempotent Gold rebuild:** Build table materializations from cleaned Silver/staging models to avoid duplicate accumulation; confirm with a two-run comparison.
7. **Isolated CI:** Run the same dbt logic and tests against the separate `atliq_dl.ci` target rather than overwriting production Gold.

## Reference

Built for the **Codebasics — AtliQ Commerce: End-to-End Data Engineering Capstone**, based on the supplied *Technical Requirements & Step-by-Step Implementation Guide*. Expected data-quality benchmark totals are from that guide; actual production/simulator results must be validated separately.
