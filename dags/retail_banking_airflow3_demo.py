"""
Daily credit card approval pipeline — Airflow 3 / Astro Runtime demo.

Flow:
  1. Ingest     – read customer CSV from S3 (vanshtuli-bucket)
  2. Branch     – split customers by income tier (>$100k = premium)
  3a. Premium   – assign Infinite Sapphire card, limit = 15% of income (parallel)
  3b. Standard  – assign Classic Rewards card,  limit = 8%  (floor $2k) (parallel)
  4. Summary    – join both branches, print approval metrics
  5. UPSERT     – mock write to Core Banking System (CBS) database
"""

from __future__ import annotations
from datetime import datetime
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

PREMIUM_THRESHOLD  = 100_000
PREMIUM_PRODUCT    = "Infinite Sapphire"
STANDARD_PRODUCT   = "Classic Rewards"
PREMIUM_RATE       = 0.15
STANDARD_RATE      = 0.08
STANDARD_FLOOR     = 2_000

S3_BUCKET = "vanshtuli-bucket"
S3_KEY    = "CSV/daily_customer_applications.csv"

default_args = {
    "owner": "banking-data-engineering",
    "retries": 0,
}


@dag(
    dag_id="retail_banking_airflow3_demo",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["retail-banking", "credit-cards", "airflow3-demo"],
)
def retail_banking_airflow3_demo():

    @task()
    def ingest_customer_data() -> list[dict]:
        """Read the daily application CSV from S3."""
        import csv
        import io
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")
        content = hook.read_key(key=S3_KEY, bucket_name=S3_BUCKET)

        customers = []
        for row in csv.DictReader(io.StringIO(content)):
            customers.append({
                "application_id": row["application_id"].strip(),
                "customer_name":  row["customer_name"].strip(),
                "annual_income":  int(row["annual_income"].strip()),
            })

        incomes = [c["annual_income"] for c in customers]
        print(f"Loaded {len(customers)} applications | "
              f"income range ${min(incomes):,} – ${max(incomes):,}")
        return customers

    @task.branch
    def route_by_income_tier(customers: list[dict], **context) -> list[str]:
        ti = context["ti"]
        premium  = [c for c in customers if c["annual_income"] >  PREMIUM_THRESHOLD]
        standard = [c for c in customers if c["annual_income"] <= PREMIUM_THRESHOLD]

        ti.xcom_push(key="premium_customers",  value=premium)
        ti.xcom_push(key="standard_customers", value=standard)

        print(f"Premium ({len(premium)}) → approve_premium_cards")
        print(f"Standard ({len(standard)}) → approve_standard_cards")

        branches = []
        if premium:
            branches.append("approve_premium_cards")
        if standard:
            branches.append("approve_standard_cards")
        return branches

    @task()
    def approve_premium_cards(**context) -> list[dict]:
        """Assign Infinite Sapphire card; limit = 15% of annual income."""
        customers = context["ti"].xcom_pull(
            task_ids="route_by_income_tier", key="premium_customers"
        ) or []

        portfolio = []
        for c in customers:
            limit = round(c["annual_income"] * PREMIUM_RATE, 2)
            portfolio.append({**c, "card_product": PREMIUM_PRODUCT, "credit_limit": limit, "income_tier": "PREMIUM"})
            print(f"  {c['application_id']}  {c['customer_name']:25s}  ${c['annual_income']:,}  →  ${limit:,.2f}")

        print(f"\n{len(portfolio)} approved on {PREMIUM_PRODUCT} | "
              f"volume ${sum(p['credit_limit'] for p in portfolio):,.2f}")
        return portfolio

    @task()
    def approve_standard_cards(**context) -> list[dict]:
        """Assign Classic Rewards card; limit = 8% of income, minimum $2,000."""
        customers = context["ti"].xcom_pull(
            task_ids="route_by_income_tier", key="standard_customers"
        ) or []

        portfolio = []
        for c in customers:
            limit = round(max(c["annual_income"] * STANDARD_RATE, STANDARD_FLOOR), 2)
            floor_note = " [floor applied]" if c["annual_income"] * STANDARD_RATE < STANDARD_FLOOR else ""
            portfolio.append({**c, "card_product": STANDARD_PRODUCT, "credit_limit": limit, "income_tier": "STANDARD"})
            print(f"  {c['application_id']}  {c['customer_name']:25s}  ${c['annual_income']:,}  →  ${limit:,.2f}{floor_note}")

        print(f"\n{len(portfolio)} approved on {STANDARD_PRODUCT} | "
              f"volume ${sum(p['credit_limit'] for p in portfolio):,.2f}")
        return portfolio

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def log_approval_summary(
        premium_portfolio:  list | None = None,
        standard_portfolio: list | None = None,
    ) -> dict:
        all_accounts = (premium_portfolio or []) + (standard_portfolio or [])

        if not all_accounts:
            return {"total_accounts": 0, "total_credit_volume": 0.0, "approved_portfolios": []}

        total_vol = sum(c["credit_limit"] for c in all_accounts)
        summary = {
            "total_accounts":      len(all_accounts),
            "total_credit_volume": round(total_vol, 2),
            "avg_credit_limit":    round(total_vol / len(all_accounts), 2),
            "approved_portfolios": all_accounts,
        }
        print(summary)
        return summary

    @task()
    def upsert_to_snowflake(summary: dict) -> None:
        """
        Batch MERGE into SANDBOX.VANSHTULI.CREDIT_ACCOUNTS via a single hook.run() call.
        Builds a multi-row VALUES clause so all records hit Snowflake in one round-trip.
        Safe to re-run — existing rows get their limit refreshed, new ones get inserted.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        portfolios = summary.get("approved_portfolios", [])
        if not portfolios:
            print("No records to upsert.")
            return

        hook = SnowflakeHook(snowflake_conn_id="snowflake")

        # Build a single VALUES block for all rows so the entire batch is one statement
        row_placeholders = ",\n            ".join(["(%s, %s, %s, %s, %s, %s)"] * len(portfolios))
        params = []
        for r in portfolios:
            params.extend([
                r["application_id"], r["customer_name"], r["annual_income"],
                r["card_product"], r["credit_limit"], r["income_tier"],
            ])

        merge_sql = f"""
            MERGE INTO SANDBOX.VANSHTULI.CREDIT_ACCOUNTS AS target
            USING (
                SELECT APPLICATION_ID, CUSTOMER_NAME, ANNUAL_INCOME,
                       CARD_PRODUCT, CREDIT_LIMIT, INCOME_TIER
                FROM VALUES
                    {row_placeholders}
                AS source(APPLICATION_ID, CUSTOMER_NAME, ANNUAL_INCOME,
                          CARD_PRODUCT, CREDIT_LIMIT, INCOME_TIER)
            ) AS source
            ON target.APPLICATION_ID = source.APPLICATION_ID
            WHEN MATCHED THEN UPDATE SET
                CREDIT_LIMIT = source.CREDIT_LIMIT,
                LAST_UPDATED = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                APPLICATION_ID, CUSTOMER_NAME, ANNUAL_INCOME,
                CARD_PRODUCT, CREDIT_LIMIT, INCOME_TIER
            ) VALUES (
                source.APPLICATION_ID, source.CUSTOMER_NAME, source.ANNUAL_INCOME,
                source.CARD_PRODUCT, source.CREDIT_LIMIT, source.INCOME_TIER
            )
        """

        hook.run(merge_sql, parameters=params)

        print(f"\nUPSERT → SANDBOX.VANSHTULI.CREDIT_ACCOUNTS ({len(portfolios)} records)")
        print(f"Batch MERGE committed via hook.run() — {len(portfolios)} rows processed.")

    # --- wire up the DAG ---
    customers = ingest_customer_data()
    branch    = route_by_income_tier(customers)

    premium_portfolio  = approve_premium_cards()
    standard_portfolio = approve_standard_cards()
    branch >> [premium_portfolio, standard_portfolio]

    summary = log_approval_summary(
        premium_portfolio=premium_portfolio,
        standard_portfolio=standard_portfolio,
    )
    upsert_to_snowflake(summary)


retail_banking_airflow3_demo()
