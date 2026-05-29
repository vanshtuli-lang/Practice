"""
S3 → Snowflake clickstream pipeline (native Python + Pandas + SQL)

Flow:
  1. wait_for_s3_file — deferrable S3KeySensor (frees worker slot while waiting)
  2. load_raw_data    — ObjectStoragePath pulls the Parquet, write_pandas loads it
  3. python_transform — parse user_agent, aggregate per-customer metrics,
                        write to py_customer_metrics

Astro / Airflow 3 features used:
  • airflow.io ObjectStorage — cloud-agnostic file I/O
  • Deferrable S3 sensor     — async waiting via Astro Runtime triggerer
  • Asset outlet             — downstream DAGs can schedule on data changes
  • owner_links              — owner clickable in the Airflow UI

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
                auto_create_table = True,
                overwrite         = True,
                quote_identifiers = False,
            )
        assert success, "Snowflake write_pandas() reported failure"
        print(f"Loaded {nrows:,} rows → {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE}")
        return nrows

    @task(outlets=[py_customer_metrics_asset])
    def python_transform() -> int:
        """
        Read raw_clickstream_orders, parse user_agent, aggregate per-customer
        metrics, and write the mart to py_customer_metrics.
        """
        import numpy as np
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.connector.pandas_tools import write_pandas

        sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # ── Load the raw landing table into Pandas ────────────────────────────
        raw = sf.get_pandas_df(
            f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE}"
        )
        raw.columns = [c.lower() for c in raw.columns]   # normalise UPPER → lower

        # ── Parse user_agent → browser  (priority order matters: Edge before
        #    Chrome; Chrome's UA contains "Safari" as a compatibility hint) ───
        ua = raw["user_agent"]
        raw["browser"] = np.select(
            [
                ua.str.contains("Edg/",       case=False, na=False),
                ua.str.contains("Chrome/",    case=False, na=False),
                ua.str.contains("Firefox/",   case=False, na=False),
                ua.str.contains("Safari/",    case=False, na=False),
                ua.str.contains("Opera|OPR/", case=False, na=False, regex=True),
            ],
            ["Edge", "Chrome", "Firefox", "Safari", "Opera"],
            default="Other",
        )

        # ── Parse user_agent → operating system  (Android before Linux:
        #    Android UAs contain "Linux" too) ────────────────────────────────
        raw["os"] = np.select(
            [
                ua.str.contains("Windows NT",  case=False, na=False),
                ua.str.contains("Macintosh",   case=False, na=False),
                ua.str.contains("Android",     case=False, na=False),
                ua.str.contains("iPhone|iPad", case=False, na=False, regex=True),
                ua.str.contains("Linux",       case=False, na=False),
            ],
            ["Windows", "macOS", "Android", "iOS", "Linux"],
            default="Other",
        )

        # ── Aggregate per-customer metrics ────────────────────────────────────
        metrics = (
            raw.groupby("customer_id")
            .agg(
                total_orders              = ("order_id",              "count"),
                total_subtotal            = ("subtotal",              "sum"),
                total_tax                 = ("tax",                   "sum"),
                avg_session_duration_secs = ("session_duration_secs", "mean"),
                most_common_browser       = ("browser", lambda s: s.mode()[0]),
                most_common_os            = ("os",      lambda s: s.mode()[0]),
            )
            .reset_index()
        )
        metrics["total_spend"] = (metrics["total_subtotal"] + metrics["total_tax"]).round(2)
        metrics["avg_session_duration_secs"] = metrics["avg_session_duration_secs"].round(2)
        metrics.drop(columns=["total_subtotal", "total_tax"], inplace=True)

        # ── Write the mart back to Snowflake ──────────────────────────────────
        with sf.get_conn() as conn:
            success, _, nrows, _ = write_pandas(
                conn,
                metrics,
                table_name        = OUTPUT_TABLE,
                database          = SNOWFLAKE_DATABASE,
                schema            = SNOWFLAKE_SCHEMA,
                auto_create_table = True,
                overwrite         = True,
                quote_identifiers = False,
            )
        assert success, "Snowflake write_pandas() reported failure"
        print(f"Wrote {nrows:,} customer rows → {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{OUTPUT_TABLE}")
        return nrows

    wait_for_s3_file >> load_raw_data() >> python_transform()


s3_to_snowflake_python()
