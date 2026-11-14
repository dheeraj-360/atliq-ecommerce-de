"""
AtliQ Phase 2 (LEARNER STARTER) — Streaming Ops DAG
====================================================
The stream never stops — but the SCHEDULED work around it is your job:
data-quality gate, table maintenance, daily rollup. Build an hourly DAG:

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Setup:
1. In docker-compose.yaml:  _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
   then: docker compose down && docker compose up -d
2. Airflow UI -> Admin -> Connections -> +
   Conn Id: databricks_default | Type: Databricks
   Host: https://<workspace>.cloud.databricks.com | Password: <PAT token>
   (Azure Databricks: Host is https://adb-<workspace-id>.<n>.azuredatabricks.net)
3. Paste your SQL warehouse HTTP path below.
"""


"""
AtliQ Phase 2 — Streaming Operations DAG

Hourly:
check_fresh_events >> optimize_tables >> refresh_daily_summary
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

# REPLACE with your Databricks SQL Warehouse HTTP path
SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/c50c2c04d1ab52cf"

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    # TASK 1 — Check recent events
    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            SELECT assert_true(
                COUNT(*) > 0,
                'DATA QUALITY FAILED: No Silver events in last 2 hours'
            )
            FROM atliq.streaming.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL 2 HOURS
        """,
    )

    # TASK 2 — Optimize Silver and Gold tables
    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            "OPTIMIZE atliq.streaming.silver_order_events",
            "OPTIMIZE atliq.streaming.gold_revenue_5min",
        ],
    )

    # TASK 3 — Create daily summary
    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            CREATE OR REPLACE TABLE
                atliq.streaming.gold_daily_summary AS
            SELECT
                CAST(event_ts AS DATE) AS event_date,
                COUNT_IF(event_type = 'order_placed')
                    AS orders_placed,
                COUNT_IF(event_type = 'payment_received')
                    AS orders_paid,
                COUNT_IF(event_type = 'order_cancelled')
                    AS orders_cancelled,
                SUM(
                    CASE
                        WHEN event_type = 'payment_received'
                        THEN order_amount
                        ELSE 0
                    END
                ) AS revenue
            FROM atliq.streaming.silver_order_events
            GROUP BY CAST(event_ts AS DATE)
        """,
    )

    # TASK 4 — Dependencies
    check_fresh_events >> optimize_tables >> refresh_daily_summary
