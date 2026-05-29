/*
  stg_clickstream.sql
  ═══════════════════
  Staging model for the raw clickstream source.

  Responsibilities
  ────────────────
  1. Select and alias all raw columns (no business logic lives in the source).
  2. Parse the free-text `user_agent` field into structured `browser` and `os`
     columns using Snowflake CASE / ILIKE expressions.

  Design notes
  ────────────
  • Materialised as a VIEW so it always reflects the latest raw landing data.
  • The CASE ordering matters: Chrome must be matched *before* Safari because
    Chrome's UA string also contains the token "Safari".
  • ILIKE is used throughout for case-insensitive matching — UA strings are
    not guaranteed to be consistently cased across browser versions.
  • The parsed columns intentionally mirror the regex extraction performed in
    Branch B (python_transform_branch) so the two branches produce equivalent
    output for a fair benchmark comparison.
*/

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_clickstream_orders') }}

),

parsed AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        subtotal,
        tax,
        click_id,
        session_duration_secs,

        /*
          browser
          ───────
          Detects Chrome before Safari because Chrome UAs include the token
          "Safari" as a compatibility hint.  Edge is detected before Chrome
          for the same reason (Edge UAs also contain "Chrome").
        */
        CASE
            WHEN user_agent ILIKE '%Edg/%'                        THEN 'Edge'
            WHEN user_agent ILIKE '%Chrome/%'
             AND user_agent NOT ILIKE '%Chromium%'                THEN 'Chrome'
            WHEN user_agent ILIKE '%Firefox/%'                    THEN 'Firefox'
            WHEN user_agent ILIKE '%Safari/%'
             AND user_agent NOT ILIKE '%Chrome%'                  THEN 'Safari'
            WHEN user_agent ILIKE '%Opera%'
              OR user_agent ILIKE '%OPR/%'                        THEN 'Opera'
            ELSE 'Other'
        END AS browser,

        /*
          os
          ──
          Platform detection from common UA tokens.  Android must precede
          Linux because Android UAs include the token "Linux".
          iPhone / iPad detection covers both mobile Safari and Chrome-on-iOS.
        */
        CASE
            WHEN user_agent ILIKE '%Windows NT%'                  THEN 'Windows'
            WHEN user_agent ILIKE '%Macintosh%'
              OR user_agent ILIKE '%Mac OS X%'                    THEN 'macOS'
            WHEN user_agent ILIKE '%Android%'                     THEN 'Android'
            WHEN user_agent ILIKE '%iPhone%'
              OR user_agent ILIKE '%iPad%'                        THEN 'iOS'
            WHEN user_agent ILIKE '%Linux%'                       THEN 'Linux'
            ELSE 'Other'
        END AS os

    FROM source

)

SELECT * FROM parsed
