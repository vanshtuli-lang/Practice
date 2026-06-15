"""
CSV File Ingestor: picks up an encrypted CSV from S3, decrypts it, runs it through
the Landing Zone job, loads it into Snowflake, fixes any bad dates, and logs the run.

Schedule: 5 AM EST daily. Processes one file per run.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Asset, dag, task

AWS_CONN_ID       = os.environ.get("AWS_CONN_ID",       "s3_read_write")
SNOWFLAKE_CONN_ID = os.environ.get("SNOWFLAKE_CONN_ID", "vansh_snowflake")

S3_BUCKET      = "vanshtuli-bucket"
S3_KEY         = "CSV/sample_orders_demo.csv.pgp"

SNOWFLAKE_DB     = "SANDBOX"
SNOWFLAKE_SCHEMA = "VANSHTULI"
LANDING_TABLE    = "CSV_LANDING_ZONE"
AUDIT_TABLE      = "CSV_INGEST_AUDIT"

# Downstream DAGs can subscribe to this — they'll auto-trigger when we're done
landing_zone_ready = Asset("csv-landing-zone-ready")


@dag(
    dag_id="csv_file_ingestor",
    schedule="0 10 * * *",   # 5 AM EST (UTC-5)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    default_args={"owner": "vansh-tuli", "retries": 1},
    tags=["csv", "s3", "snowflake", "landing-zone"],
    doc_md=__doc__,
)
def csv_file_ingestor():

    # Wait for the file to show up in S3 before doing anything
    wait_for_file = S3KeySensor(
        task_id       = "wait_for_file",
        bucket_name   = S3_BUCKET,
        bucket_key    = S3_KEY,
        aws_conn_id   = AWS_CONN_ID,
        deferrable    = True,
        poke_interval = 60,
        timeout       = 3600,
    )

    @task
    def decrypt_file() -> str:
        """
        In prod: pull the PGP private key from Astro's secrets backend and call gnupg.decrypt().
        For this demo we just copy the file and strip the .pgp extension.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        decrypted_key = S3_KEY.removesuffix(".pgp")
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        print(f"Decrypting s3://{S3_BUCKET}/{S3_KEY}")
        hook.copy_object(
            source_bucket_key  = S3_KEY,
            dest_bucket_key    = decrypted_key,
            source_bucket_name = S3_BUCKET,
            dest_bucket_name   = S3_BUCKET,
        )
        print(f"Decrypted file ready at: {decrypted_key}")
        return decrypted_key

    @task
    def trigger_lz_job(decrypted_key: str) -> str:
        """
        Kicks off the Landing Zone job. In prod this would call TriggerDagRunOperator
        or an HTTP hook — mocked here so the demo runs standalone.
        """
        print(f"[LZ Job] Triggering for source: s3://{S3_BUCKET}/{decrypted_key}")
        print(f"[LZ Job] Payload: source_bucket={S3_BUCKET}, source_key={decrypted_key}, target_schema={SNOWFLAKE_SCHEMA}")
        print("[LZ Job] Acknowledged - proceeding to load")
        return decrypted_key

    @task
    def load_to_snowflake(s3_key: str) -> int:
        """Read the CSV from S3 and load it into the Snowflake landing zone."""
        import io
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.connector.pandas_tools import write_pandas

        raw = S3Hook(aws_conn_id=AWS_CONN_ID).read_key(key=s3_key, bucket_name=S3_BUCKET)
        df  = pd.read_csv(io.StringIO(raw))

        # Tag each row so we always know which file it came from
        df["_source_file"] = s3_key.split("/")[-1]
        df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with hook.get_conn() as conn:
            success, _, nrows, _ = write_pandas(
                conn, df,
                table_name        = LANDING_TABLE,
                database          = SNOWFLAKE_DB,
                schema            = SNOWFLAKE_SCHEMA,
                auto_create_table = True,
                quote_identifiers = False,
            )

        assert success, "Snowflake write_pandas() failed"
        print(f"Loaded {nrows} rows into {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{LANDING_TABLE}")
        return nrows

    @task
    def normalize_dates() -> int:
        """
        Find any dates in MM/DD/YYYY format and flip them to YYYY-MM-DD.
        Runs entirely as a Snowflake UPDATE — nothing pulled into the worker.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        filename = S3_KEY.split("/")[-1].removesuffix(".pgp")
        fqt      = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{LANDING_TABLE}"
        hook     = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        bad = hook.get_records(f"""
            SELECT COUNT(*) FROM {fqt}
            WHERE _SOURCE_FILE = '{filename}'
              AND (TRY_TO_DATE(ORDER_DATE) IS NULL OR TRY_TO_DATE(SHIP_DATE) IS NULL)
        """)[0][0]

        if bad > 0:
            print(f"{bad} rows have non-ISO dates — normalizing to YYYY-MM-DD")
            hook.run(f"""
                UPDATE {fqt}
                SET
                    ORDER_DATE = TO_VARCHAR(TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
                    SHIP_DATE  = TO_VARCHAR(TRY_TO_DATE(SHIP_DATE,  'MM/DD/YYYY'), 'YYYY-MM-DD')
                WHERE _SOURCE_FILE = '{filename}'
                  AND (TRY_TO_DATE(ORDER_DATE) IS NULL OR TRY_TO_DATE(SHIP_DATE) IS NULL)
            """)
        else:
            print("All dates already ISO 8601 — nothing to fix")

        return bad

    @task(outlets=[landing_zone_ready])
    def log_audit(rows_loaded: int, date_fixes: int) -> None:
        """Write one audit row for this run and signal that the landing zone is ready."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(f"""
            INSERT INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{AUDIT_TABLE}
                (SOURCE_FILE, ROWS_LOADED, DATE_FIXES_APPLIED, STATUS, LOGGED_AT)
            VALUES ('{S3_KEY}', {rows_loaded}, {date_fixes}, 'SUCCESS', CURRENT_TIMESTAMP())
        """)
        print(f"Audit logged — {rows_loaded} rows, {date_fixes} date fix(es)")
        print("landing_zone_ready asset emitted - any downstream DAGs will now trigger")

    #dependency management for the DAG
    decrypted  = decrypt_file()
    lz_done    = trigger_lz_job(decrypted)
    rows       = load_to_snowflake(lz_done)
    fixes      = normalize_dates()
    log_audit(rows, fixes)

    wait_for_file >> decrypted


csv_file_ingestor()
