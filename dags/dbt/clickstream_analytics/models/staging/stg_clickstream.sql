/*
  Staging view for the raw clickstream source. Parses the free-text user_agent
  field into structured browser and os columns. CASE ordering matters: Edge and
  Chrome must be matched before Safari because their UAs also contain "Safari".
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

        -- Edge before Chrome before Safari — each UA contains the next one's token
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

        -- Android before Linux — Android UAs contain "Linux"
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
