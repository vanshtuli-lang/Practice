"""
Same clickstream ingestion as s3_to_snowflake_python but hands the transform off to dbt via Cosmos.

Waits for the parquet to land in S3, loads it into RAW_CLICKSTREAM_ORDERS,
then Cosmos runs the dbt models and tests from there.

This is click stream data - Customer X spent $Y on date Z, using device W, after browsing for N seconds.
Total of 1 M rows in the data set.
Waiting for file in S3 -> parsing the user_agent field (example field: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15
 (KHTML, like Gecko) Version/17.3 Safari/605.1.15) -> to bring out browser = safari and os = macOS
 We create a view in the STG area that aggregates data by customers.
We create a final METRICS table that has aggregate of data per customer -> total orders, total, spend, avg session duration, popular ordering platform etc
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Asset, ObjectStoragePath, dag, task

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from cosmos.profiles import (
    SnowflakeEncryptedPrivateKeyFilePemProfileMapping,
    SnowflakeUserPasswordProfileMapping,
)

# Local dev can override these via .env when conn IDs differ
AWS_CONN_ID       = os.environ.get("AWS_CONN_ID",       "s3_read_write")
SNOWFLAKE_CONN_ID = os.environ.get("SNOWFLAKE_CONN_ID", "snowflake")

# Pick the right Cosmos profile mapping for password vs RSA key auth
_AUTH = os.environ.get("SNOWFLAKE_AUTH", "password").lower()
_ProfileMapping = (
    SnowflakeEncryptedPrivateKeyFilePemProfileMapping if _AUTH == "key"
    else SnowflakeUserPasswordProfileMapping
)

S3_BUCKET = "vanshtuli-bucket"
S3_KEY    = "parquet/mock_clickstream_orders.parquet"
S3_URI    = f"s3://{S3_BUCKET}/{S3_KEY}"

SNOWFLAKE_DATABASE = "SANDBOX"
SNOWFLAKE_SCHEMA   = "VANSHTULI"
RAW_TABLE          = "RAW_CLICKSTREAM_ORDERS"

DBT_PROJECT_PATH = Path(__file__).parent / "dbt" / "clickstream_analytics"

# Downstream DAGs subscribe to this asset to run whenever fresh metrics land
fct_customer_metrics_asset = Asset("SANDBOX.VANSHTULI.FCT_CUSTOMER_METRICS")


@dag(
    dag_id="s3_to_snowflake_dbt",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "vansh-tuli", "retries": 0},
    owner_links={"vansh-tuli": "https://github.com/vanshtuli-lang"},
    tags=["e-commerce", "dbt", "cosmos", "asset", "airflow3-demo"],
    doc_md=__doc__,
)
def s3_to_snowflake_dbt():

    # Deferrable so the worker slot is freed while we wait
    wait_for_s3_file = S3KeySensor(
        task_id      = "wait_for_s3_file",
        bucket_key   = S3_URI,
        aws_conn_id  = AWS_CONN_ID,
        deferrable   = True,
        poke_interval= 30,
        timeout      = 300,
    )

    @task(outlets=[fct_customer_metrics_asset])
    def load_raw_data() -> int:
        """Read the Parquet via airflow.io ObjectStoragePath, bulk-load to Snowflake."""
        import pandas as pd
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.connector.pandas_tools import write_pandas

        # airflow.io abstracts the cloud — same code would work for gs:// or az://
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

    # Cosmos renders each dbt model as its own Airflow task; tests run AFTER_ALL
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="clickstream_analytics",
            target_name="dev",
            profile_mapping=_ProfileMapping(
                conn_id=SNOWFLAKE_CONN_ID,
                profile_args={
                    "database": SNOWFLAKE_DATABASE,
                    "schema":   SNOWFLAKE_SCHEMA,
                },
            ),
        ),
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
        render_config=RenderConfig(
            load_method  = LoadMode.DBT_LS,
            test_behavior= TestBehavior.AFTER_ALL,
        ),
        operator_args={"retries": 0},
    )

    wait_for_s3_file >> load_raw_data() >> dbt_transform


s3_to_snowflake_dbt()
