"""
Loads clickstream orders from S3 into Snowflake and builds a per-customer metrics table using Python/Pandas.

Waits for the parquet file to land in S3, bulk loads it into RAW_CLICKSTREAM_ORDERS,
then runs a pandas transform to produce PY_CUSTOMER_METRICS.

This is click stream data - Customer X spent $Y on date Z, using device W, after browsing for N seconds.
Total of 1 M rows in the data set.
Waiting for file in S3 -> parsing the user_agent field (example field: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15
 (KHTML, like Gecko) Version/17.3 Safari/605.1.15) -> to bring out browser = safari and os = macOS
We create a final METRICS table that has aggregate of data per customer -> total orders, total, spend, avg session duration, popular ordering platform etc
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Asset, ObjectStoragePath, dag, task

#Connections
# Production defaults match credit_card_provider_flow_AF3.  Local dev can
# override via .env when airflow_settings.yaml uses different conn IDs.
AWS_CONN_ID       = os.environ.get("AWS_CONN_ID",       "s3_read_write")
SNOWFLAKE_CONN_ID = os.environ.get("SNOWFLAKE_CONN_ID", "snowflake")

#Sources
S3_BUCKET = "vanshtuli-bucket"
S3_KEY    = "parquet/mock_clickstream_orders.parquet"
S3_URI    = f"s3://{S3_BUCKET}/{S3_KEY}"

# ── Snowflake target ──────────────────────────────────────────────────────────
SNOWFLAKE_DATABASE = "SANDBOX"
SNOWFLAKE_SCHEMA   = "VANSHTULI"
RAW_TABLE          = "RAW_CLICKSTREAM_ORDERS"
OUTPUT_TABLE       = "PY_CUSTOMER_METRICS"

# ── Asset (AF3 data-aware scheduling) ─────────────────────────────────────────
# Downstream DAGs can do schedule=[py_customer_metrics_asset] and run whenever
# this DAG materialises fresh data.  Using the dotted Snowflake identifier as
# the asset name (no "://") avoids the snowflake provider's strict URI validator.
py_customer_metrics_asset = Asset("SANDBOX.VANSHTULI.PY_CUSTOMER_METRICS")


@dag(
    dag_id="s3_to_snowflake_python",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "vansh-tuli", "retries": 0},
    owner_links={"vansh-tuli": "https://github.com/vanshtuli-lang"},
    tags=["e-commerce", "python", "pandas", "asset", "airflow3-demo"],
    doc_md=__doc__,
)
def s3_to_snowflake_python():

    # ── Wait for the file to land in S3 (deferrable = async / no worker slot)
    wait_for_s3_file = S3KeySensor(
        task_id      = "wait_for_s3_file",
        bucket_key   = S3_URI,
        aws_conn_id  = AWS_CONN_ID,
        deferrable   = True,
        poke_interval= 30,
        timeout      = 300,
    )

    @task
    def load_raw_data() -> int:
        """Read the Parquet via airflow.io ObjectStoragePath, bulk-load to Snowflake."""
        import pandas as pd
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.connector.pandas_tools import write_pandas

        source = ObjectStoragePath(S3_URI, conn_id=AWS_CONN_ID)
        with source.open("rb") as f:
            df = pd.read_parquet(f)

        sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with sf.get_conn() as conn:
            success, _, nrows, _ = write_pandas(
                conn,
                df,
                table_name        = RAW_TABLE,
                database          = SNOWFLAKE_DATABASE,
                schema            = SNOWFLAKE_SCHEMA,
                quote_identifiers = False,
            )
        assert success, "Snowflake write_pandas() reported failure"
        print(f"Loaded {nrows:,} rows into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE}")
        return nrows

    @task(outlets=[py_customer_metrics_asset])
    def python_transform() -> None:
        """
        Parse user_agent and roll up per-customer metrics entirely in Snowflake SQL.
        No data is pulled into the worker - avoids OOM on the 1M row table.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        hook.run(f"""
            WITH parsed AS (
                SELECT
                    CUSTOMER_ID,
                    ORDER_ID,
                    SUBTOTAL + TAX AS SPEND,
                    SESSION_DURATION_SECS,
                    CASE
                        WHEN USER_AGENT ILIKE '%Edg/%'    THEN 'Edge'
                        WHEN USER_AGENT ILIKE '%Chrome/%'  THEN 'Chrome'
                        WHEN USER_AGENT ILIKE '%Firefox/%' THEN 'Firefox'
                        WHEN USER_AGENT ILIKE '%Safari/%'  THEN 'Safari'
                        WHEN USER_AGENT ILIKE '%Opera%' OR USER_AGENT ILIKE '%OPR/%' THEN 'Opera'
                        ELSE 'Other'
                    END AS BROWSER,
                    CASE
                        WHEN USER_AGENT ILIKE '%Windows NT%' THEN 'Windows'
                        WHEN USER_AGENT ILIKE '%Macintosh%'  THEN 'macOS'
                        WHEN USER_AGENT ILIKE '%Android%'    THEN 'Android'
                        WHEN USER_AGENT ILIKE '%iPhone%' OR USER_AGENT ILIKE '%iPad%' THEN 'iOS'
                        WHEN USER_AGENT ILIKE '%Linux%'      THEN 'Linux'
                        ELSE 'Other'
                    END AS OS
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE}
            ),
            base_metrics AS (
                SELECT
                    CUSTOMER_ID,
                    COUNT(ORDER_ID)                      AS TOTAL_ORDERS,
                    ROUND(SUM(SPEND), 2)                 AS TOTAL_SPEND,
                    ROUND(AVG(SESSION_DURATION_SECS), 2) AS AVG_SESSION_DURATION_SECS
                FROM parsed
                GROUP BY CUSTOMER_ID
            ),
            top_browser AS (
                SELECT CUSTOMER_ID, BROWSER AS MOST_COMMON_BROWSER
                FROM parsed
                GROUP BY CUSTOMER_ID, BROWSER
                QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY COUNT(*) DESC) = 1
            ),
            top_os AS (
                SELECT CUSTOMER_ID, OS AS MOST_COMMON_OS
                FROM parsed
                GROUP BY CUSTOMER_ID, OS
                QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY COUNT(*) DESC) = 1
            )
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{OUTPUT_TABLE}
                (CUSTOMER_ID, TOTAL_ORDERS, TOTAL_SPEND, AVG_SESSION_DURATION_SECS, MOST_COMMON_BROWSER, MOST_COMMON_OS)
            SELECT
                m.CUSTOMER_ID, m.TOTAL_ORDERS, m.TOTAL_SPEND, m.AVG_SESSION_DURATION_SECS,
                b.MOST_COMMON_BROWSER, o.MOST_COMMON_OS
            FROM base_metrics m
            JOIN top_browser b ON m.CUSTOMER_ID = b.CUSTOMER_ID
            JOIN top_os      o ON m.CUSTOMER_ID = o.CUSTOMER_ID
        """)

        print(f"Transform complete: metrics written into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{OUTPUT_TABLE}")

    wait_for_s3_file >> load_raw_data() >> python_transform()


s3_to_snowflake_python()
