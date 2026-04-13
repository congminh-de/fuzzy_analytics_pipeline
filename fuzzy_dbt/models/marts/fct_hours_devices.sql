{{ config(materialized='table') }}

SELECT
    EXTRACT(HOUR FROM s.created_at) AS hour_of_day,
    FORMAT_DATE('%A', DATE(s.created_at)) AS day_of_week,
    EXTRACT(DAYOFWEEK FROM s.created_at) AS day_num,
    s.device_type,
    COUNT(DISTINCT s.website_session_id) AS sessions,
    COUNT(DISTINCT o.order_id) AS orders,
    ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT s.website_session_id), 4) AS cvr,
    ROUND(SUM(o.price_usd) / COUNT(DISTINCT s.website_session_id), 4) AS rps
FROM {{ ref('stg_website_sessions') }} s
LEFT JOIN {{ ref('stg_orders') }} o ON s.website_session_id = o.website_session_id
GROUP BY 1, 2, 3, 4
ORDER BY 3, 1, 2