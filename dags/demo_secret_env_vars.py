"""
Demo: Astro secret environment variable validation.

Talk track: secrets set in the Astro UI (marked Secret) are injected at runtime
as environment variables — this DAG reads them and logs masked values so you can
confirm resolution without ever exposing the raw secret in logs or code.
"""

import os
import logging
from datetime import datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="demo_secret_env_vars",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "secrets", "astro"],
)
def demo_secret_env_vars():

    @task
    def print_secrets():
        api_key = os.environ.get("DEMO_API_KEY", "<not set>")
        env_secret = os.environ.get("DEMO_ENV_SECRET", "<not set>")

        # Mask so we can confirm resolution without leaking values in logs
        def mask(v):
            return v[:4] + "****" if len(v) > 4 else "****"

        log.info("DEMO_API_KEY     = %s", mask(api_key))
        log.info("DEMO_ENV_SECRET  = %s", mask(env_secret))

    print_secrets()


demo_secret_env_vars()
