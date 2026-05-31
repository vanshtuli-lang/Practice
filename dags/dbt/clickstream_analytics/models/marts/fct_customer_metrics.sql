{{ config(unique_key='customer_id') }}

/*
  fct_customer_metrics.sql
  ════════════════════════
  Mart-layer fact table: one row per customer with aggregated spend and
  engagement metrics derived from the stg_clickstream staging view.

  Transformation steps
  ────────────────────
  1. ranked       — adds an order-recency rank per customer using an
                    analytical window function.  Rank 1 = most recent order.
                    Directly mirrors Branch B's pandas .rank() computation.
  2. spend_agg    — GROUP BY customer_id for monetary and session metrics.
  3. browser_mode — finds the most frequently used browser per customer.
                    QUALIFY + ROW_NUMBER avoids a self-join and is idiomatic
                    Snowflake SQL.  Ties broken alphabetically for determinism.
  4. os_mode      — same approach for operating system.
  5. final        — joins all CTEs on customer_id.

  Design notes
  ────────────
  • Materialised as a TABLE so the mart is pre-computed and query-ready for
    downstream BI tools and the benchmark evaluator task.
  • QUALIFY is Snowflake-specific syntax; it filters the result of window
    functions without a subquery wrapper — equivalent to a WHERE clause on
    the window function result.
  • The output column names intentionally match the pandas branch output in
    python_transform_branch so both tables can be diffed for correctness.
*/

WITH staged AS (

    SELECT * FROM {{ ref('stg_clickstream') }}

),

/*
  Step 1 — Order recency ranking
  ──────────────────────────────
  RANK() assigns the same rank to ties (identical order_date within a
  customer), matching pandas .rank(method='min').  Rank 1 = newest order.
*/
ranked AS (

    SELECT
        *,
        RANK() OVER (
            PARTITION BY customer_id
            ORDER BY     order_date DESC
        ) AS order_rank

    FROM staged

),

/*
  Step 2 — Per-customer spend and session aggregation
*/
spend_agg AS (

    SELECT
        customer_id,
        COUNT(DISTINCT order_id)                AS total_orders,
        ROUND(SUM(subtotal),            2)      AS total_subtotal,
        ROUND(SUM(tax),                 2)      AS total_tax,
        ROUND(SUM(subtotal + tax),      2)      AS total_spend,
        ROUND(AVG(session_duration_secs), 2)    AS avg_session_duration_secs

    FROM ranked
    GROUP BY customer_id

),

/*
  Step 3 — Most common browser per customer
  ──────────────────────────────────────────
  Inner query counts (customer_id, browser) frequency.
  QUALIFY keeps only the row with the highest frequency; alphabetical tie-
  breaking ensures deterministic output across re-runs.
*/
browser_mode AS (

    SELECT
        customer_id,
        browser AS most_common_browser

    FROM (
        SELECT
            customer_id,
            browser,
            COUNT(*) AS freq
        FROM ranked
        GROUP BY customer_id, browser
    )
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY     freq DESC, browser ASC
        ) = 1

),

/*
  Step 4 — Most common OS per customer  (identical pattern to browser_mode)
*/
os_mode AS (

    SELECT
        customer_id,
        os AS most_common_os

    FROM (
        SELECT
            customer_id,
            os,
            COUNT(*) AS freq
        FROM ranked
        GROUP BY customer_id, os
    )
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY     freq DESC, os ASC
        ) = 1

),

/*
  Step 5 — Final join: one row per customer
*/
final AS (

    SELECT
        s.customer_id,
        s.total_orders,
        s.total_subtotal,
        s.total_tax,
        s.total_spend,
        s.avg_session_duration_secs,
        b.most_common_browser,
        o.most_common_os

    FROM       spend_agg    s
    LEFT JOIN  browser_mode b  ON s.customer_id = b.customer_id
    LEFT JOIN  os_mode      o  ON s.customer_id = o.customer_id

)

SELECT * FROM final
