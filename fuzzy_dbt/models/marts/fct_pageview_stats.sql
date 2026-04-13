{{ config(materialized='table') }}

WITH first_pageview AS (
    SELECT
        website_session_id,
        MIN(website_pageview_id) AS first_pv_id
    FROM {{ ref('stg_website_pageviews') }}
    GROUP BY 1
),
session_landing AS (
    SELECT
        fp.website_session_id,
        p.pageview_url AS landing_page
    FROM first_pageview fp
    JOIN {{ ref('stg_website_pageviews') }} p ON fp.first_pv_id = p.website_pageview_id
    WHERE p.pageview_url LIKE '/lander%'
    OR p.pageview_url = '/home'
),
lander_time AS (
    SELECT
        p.pageview_url AS landing_page,
        DATE(MIN(s.created_at)) AS first_seen,
        DATE(MAX(s.created_at)) AS last_seen,
        COUNT(DISTINCT s.website_session_id) AS sessions
    FROM first_pageview fp
    JOIN {{ ref('stg_website_pageviews') }} p ON fp.first_pv_id = p.website_pageview_id
    JOIN {{ ref('stg_website_sessions') }} s ON fp.website_session_id = s.website_session_id
    GROUP BY 1
)
SELECT
    sl.landing_page,
    l.first_seen,
    l.last_seen,
    DATE_DIFF(l.last_seen, l.first_seen, MONTH) AS period,
    COUNT(DISTINCT sl.website_session_id) AS sessions,
    COUNT(DISTINCT o.order_id) AS orders,
    ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT sl.website_session_id), 4) AS cv_pct
FROM session_landing sl
LEFT JOIN {{ ref('stg_orders') }} o ON sl.website_session_id = o.website_session_id
LEFT JOIN lander_time l ON l.landing_page = sl.landing_page
GROUP BY 1, 2, 3, 4