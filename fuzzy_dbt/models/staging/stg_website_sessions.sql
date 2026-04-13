SELECT
    SAFE_CAST(website_session_id AS INT64) AS website_session_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(user_id AS INT64) AS user_id,
    is_repeat_session,
    NULLIF(utm_source, 'NULL') AS utm_source,
    NULLIF(utm_campaign, 'NULL') AS utm_campaign,
    device_type,
    http_referer
FROM {{ source('raw_fuzzy', 'website_sessions') }}