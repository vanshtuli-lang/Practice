"""
Daily credit card approval pipeline — Airflow 3 / Astro Runtime demo.

Flow:
  1. Ingest     – read 50-record customer CSV from include/
  2. Branch     – split customers by income tier (>$100k = premium)
  3a. Premium   – assign Infinite Sapphire card, limit = 15% of income (parallel)
  3b. Standard  – assign Classic Rewards card,  limit = 8%  (floor $2k) (parallel)
  4. Summary    – join both branches, print approval metrics
  5. UPSERT     – mock write to Core Banking System (CBS) database
"""

from __future__ import annotations
import csv
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

PREMIUM_THRESHOLD  = 100_000
PREMIUM_PRODUCT    = "Infinite Sapphire"
STANDARD_PRODUCT   = "Classic Rewards"
PREMIUM_RATE       = 0.15
STANDARD_RATE      = 0.08
STANDARD_FLOOR     = 2_000

CSV_PATH = "/usr/local/airflow/include/daily_customer_applications.csv"

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
        """Read the daily application CSV and return a list of customer dicts."""
        customers = []
        with open(CSV_PATH, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
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
        """
        Split customers into premium vs standard lists and push both to XCom
        under named keys.  Return the task IDs that should run next — Airflow
        automatically SKIPs any branch not listed here.
        """
        ti = context["ti"]

        premium  = [c for c in customers if c["annual_income"] >  PREMIUM_THRESHOLD]
        standard = [c for c in customers if c["annual_income"] <= PREMIUM_THRESHOLD]

        # Named keys so Tasks 3a/3b can pull their slice independently
        ti.xcom_push(key="premium_customers",  value=premium)
        ti.xcom_push(key="standard_customers", value=standard)

        print(f"Premium ({len(premium)}) → approve_premium_cards")
        print(f"Standard ({len(standard)}) → approve_standard_cards")

        """Logic below is handling cases where only 1 type exist and hence other task can be skipped. 
            In the case of both types, the tasks will run in parallel."""
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
            premium_portfolio: list | None = None,
            standard_portfolio: list | None = None,
    ) -> dict:
        all_accounts = (premium_portfolio or []) + (standard_portfolio or [])

        if not all_accounts:
            return {"total_accounts": 0, "total_credit_volume": 0.0, "approved_portfolios": []}

        total_vol = sum(c["credit_limit"] for c in all_accounts)

        summary = {
            "total_accounts": len(all_accounts),
            "total_credit_volume": round(total_vol, 2),
            "avg_credit_limit": round(total_vol / len(all_accounts), 2),
            "approved_portfolios": all_accounts,
        }

        print(summary)
        return summary

    @task()
    def upsert_to_core_banking(summary: dict) -> None:
        """
        Mock UPSERT to the credit_accounts table.

        In production this would use PostgresHook with an ON CONFLICT DO UPDATE
        statement so re-runs are safe.
        """
        portfolios    = summary.get("approved_portfolios", [])
        total         = summary.get("total_accounts", 0)
        insert_count  = 0
        update_count  = 0

        print(f"\nUPSERT → cbs_postgres_prod.public.credit_accounts  ({total} records)")
        print("-" * 68)

        for idx, r in enumerate(portfolios):
            # Every 3rd record simulates a returning customer (would be a SELECT in prod)
            if idx % 3 == 0:
                update_count += 1
                print(f"  UPDATE  {r['application_id']}  {r['customer_name']:25s}  limit → ${r['credit_limit']:,.2f}")
            else:
                insert_count += 1
                print(f"  INSERT  {r['application_id']}  {r['customer_name']:25s}  limit = ${r['credit_limit']:,.2f}")

        print("-" * 68)
        print(f"  Inserted: {insert_count}  |  Updated: {update_count}  |  Total: {total}")
    #dependencies and wiring the dag
    customers = ingest_customer_data()
    branch    = route_by_income_tier(customers)

    # Tasks 3a/3b pull their data from the branch task's named XCom keys,
    # so we wire the branch edge explicitly rather than passing args here.
    premium_portfolio  = approve_premium_cards()
    standard_portfolio = approve_standard_cards()
    branch >> [premium_portfolio, standard_portfolio]

    summary = log_approval_summary(
        premium_portfolio=premium_portfolio,
        standard_portfolio=standard_portfolio,
    )
    upsert_to_core_banking(summary)


retail_banking_airflow3_demo()
