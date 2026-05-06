WITH raw_sessions AS (
    SELECT
        *,
        SAFE_CAST(created_at AS TIMESTAMP) AS created_at_ts
    FROM {{ source('raw_fuzzy', 'website_sessions') }}
)
SELECT
    SAFE_CAST(website_session_id AS INT64) AS website_session_id,
    created_at_ts AS created_at,
    DATE(created_at_ts) AS session_date,
    DATE_TRUNC(DATE(created_at_ts), MONTH) AS session_month,
    EXTRACT(YEAR FROM created_at_ts) AS session_year,
    SAFE_CAST(user_id AS INT64) AS user_id,
    is_repeat_session,
    IF(is_repeat_session = 0, 'new', 'returning') AS visitor_type,
    COALESCE(utm_source, 'direct') AS utm_source,
    COALESCE(utm_campaign, 'none') AS utm_campaign,
    COALESCE(utm_content, 'none') AS utm_content,
    COALESCE(device_type, 'unknown') AS device_type,
    COALESCE(http_referer, 'direct') AS http_referer,
    CASE
        WHEN utm_source = 'gsearch' AND utm_campaign = 'brand' THEN 'gsearch_brand'
        WHEN utm_source = 'gsearch' AND utm_campaign = 'nonbrand' THEN 'gsearch_nonbrand'
        WHEN utm_source = 'bsearch' AND utm_campaign = 'brand' THEN 'bsearch_brand'
        WHEN utm_source = 'bsearch' AND utm_campaign = 'nonbrand' THEN 'bsearch_nonbrand'
        WHEN utm_source = 'socialbook' THEN 'socialbook'
        WHEN utm_source IN ('direct', 'NULL') OR http_referer IS NULL THEN 'direct'
        ELSE 'other'
    END AS channel
FROM raw_sessions
