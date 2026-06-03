{{ config(unique_key='customer_id') }}

/*
  Mart fact table: one row per customer with aggregated spend, session, and
  most-common browser/OS metrics derived from stg_clickstream. Materialised as
  a table so downstream BI queries are fast.
*/

WITH staged AS (

    SELECT * FROM {{ ref('stg_clickstream') }}

),

-- Order recency rank per customer (1 = newest)
ranked AS (

    SELECT
        *,
        RANK() OVER (
            PARTITION BY customer_id
            ORDER BY     order_date DESC
        ) AS order_rank

    FROM staged

),

-- Per-customer spend and session aggregates
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

-- Most common browser per customer; alphabetical tiebreak for determinism
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

-- Most common OS per customer
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

-- One row per customer
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
