"""
Demo: Bakehouse Sales Report.

Talk track: reads directly from the Databricks bakehouse sample data, validates it,
then fans out three console reports in parallel using dynamic task mapping — one per
dimension. No tables written, no side effects. Pure read-and-report.
"""

from airflow.sdk import dag, task
from pendulum import datetime

DATABRICKS_CONN_ID      = "databricks_emea"
SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/127187a13c10e4c1"

SOURCE = "samples.bakehouse.sales_transactions"


@dag(
    dag_id="databricks_bakehouse_gold",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    doc_md=__doc__,
    tags=["databricks", "demo", "airflow3"],
    default_args={"owner": "data-engineering", "retries": 1},
)
def databricks_bakehouse_gold():

    @task
    def validate_source() -> int:
        """Check the source table exists and has data."""
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

        hook = DatabricksSqlHook(
            databricks_conn_id=DATABRICKS_CONN_ID,
            http_path=SQL_WAREHOUSE_HTTP_PATH,
        )
        row = hook.get_first(f"SELECT COUNT(*) FROM {SOURCE}")
        total = row[0]

        print("=" * 50)
        print(f"  SOURCE TABLE : {SOURCE}")
        print(f"  TOTAL ROWS   : {total:,}")
        print("=" * 50)

        assert total > 0, "Source table is empty!"
        return total

    @task
    def get_dimensions() -> list[str]:
        """Return the dimensions we want to report on."""
        return ["by_store", "by_product", "by_day_of_week"]

    @task
    def print_report(dimension: str) -> None:
        """Run the right query for this dimension and print results."""
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

        hook = DatabricksSqlHook(
            databricks_conn_id=DATABRICKS_CONN_ID,
            http_path=SQL_WAREHOUSE_HTTP_PATH,
        )

        if dimension == "by_store":
            rows = hook.get_records(f"""
                SELECT storeID,
                       COUNT(*)       AS transactions,
                       SUM(netAmount) AS revenue
                FROM {SOURCE}
                GROUP BY storeID
                ORDER BY revenue DESC
                LIMIT 5
            """)
            print("\n📦 TOP 5 STORES BY REVENUE")
            print(f"{'Store':<15} {'Transactions':>15} {'Revenue':>15}")
            print("-" * 47)
            for r in rows:
                print(f"{str(r[0]):<15} {r[1]:>15,} {float(r[2]):>15,.2f}")

        elif dimension == "by_product":
            rows = hook.get_records(f"""
                SELECT product,
                       SUM(quantity)  AS units_sold,
                       SUM(netAmount) AS revenue
                FROM {SOURCE}
                GROUP BY product
                ORDER BY units_sold DESC
                LIMIT 5
            """)
            print("\n🥐 TOP 5 PRODUCTS BY UNITS SOLD")
            print(f"{'Product':<20} {'Units Sold':>12} {'Revenue':>15}")
            print("-" * 49)
            for r in rows:
                print(f"{str(r[0]):<20} {r[1]:>12,} {float(r[2]):>15,.2f}")

        elif dimension == "by_day_of_week":
            rows = hook.get_records(f"""
                SELECT date_format(dateTime, 'EEEE') AS day,
                       COUNT(*)                      AS transactions,
                       ROUND(AVG(netAmount), 2)      AS avg_sale
                FROM {SOURCE}
                GROUP BY day
                ORDER BY transactions DESC
            """)
            print("\n📅 SALES BY DAY OF WEEK")
            print(f"{'Day':<15} {'Transactions':>15} {'Avg Sale':>12}")
            print("-" * 44)
            for r in rows:
                print(f"{str(r[0]):<15} {r[1]:>15,} {float(r[2]):>12,.2f}")

    # ── wire it up ────────────────────────────────────────────────────────────
    validated  = validate_source()
    dimensions = get_dimensions()
    reports    = print_report.expand(dimension=dimensions)

    validated >> dimensions >> reports


databricks_bakehouse_gold()
