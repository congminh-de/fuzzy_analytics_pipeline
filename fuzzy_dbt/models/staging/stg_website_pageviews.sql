SELECT
    SAFE_CAST(website_pageview_id AS INT64) AS website_pageview_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(website_session_id AS INT64) AS website_session_id,
    pageview_url
FROM {{ source('raw_fuzzy', 'website_pageviews') }}