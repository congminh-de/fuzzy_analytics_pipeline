{{ config(materialized='table') }}

SELECT
    CASE
        WHEN utm_source = 'NULL' THEN 'direct'
        WHEN utm_source = 'gsearch' AND utm_campaign = 'brand'    THEN 'google brand'
        WHEN utm_source = 'gsearch' AND utm_campaign = 'nonbrand' THEN 'google nonbrand'
        WHEN utm_source = 'bsearch' AND utm_campaign = 'brand'    THEN 'bing brand'
        WHEN utm_source = 'bsearch' AND utm_campaign = 'nonbrand' THEN 'bing nonbrand'
        WHEN utm_source = 'socialbook' THEN 'social'
        ELSE utm_source
    END AS channel,
    COUNT(DISTINCT s.website_session_id) AS sessions,
    COUNT(DISTINCT o.order_id) AS orders,
    ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT s.website_session_id), 4) AS cvr_pct,
    ROUND(SUM(o.price_usd) / COUNT(DISTINCT s.website_session_id), 4) AS rps,
    ROUND(SUM(o.price_usd), 0) AS total_revenue,
    ROUND(AVG(o.price_usd), 2) AS aov
FROM {{ ref('stg_website_sessions') }} s
LEFT JOIN {{ ref('stg_orders') }} o ON s.website_session_id = o.website_session_id
GROUP BY 1
ORDER BY sessions DESC