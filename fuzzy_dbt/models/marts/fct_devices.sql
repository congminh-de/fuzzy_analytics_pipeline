{{ config(materialized='table') }}

SELECT
    s.device_type,
    COUNT(DISTINCT s.website_session_id) - COUNT(DISTINCT o.order_id) AS sessions,
    COUNT(DISTINCT o.order_id) AS orders,
    ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT s.website_session_id), 4)  AS cvr_pct,
    ROUND(SUM(o.price_usd) / COUNT(DISTINCT s.website_session_id), 4) AS rps,
    ROUND(AVG(o.price_usd), 2) AS aov
FROM {{ ref('stg_website_sessions') }} s
LEFT JOIN {{ ref('stg_orders') }} o ON s.website_session_id = o.website_session_id
GROUP BY 1
ORDER BY sessions DESC