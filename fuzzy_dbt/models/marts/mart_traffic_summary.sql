{{ config(materialized='table') }}

SELECT
    s.session_month,
    s.session_year,
    s.utm_source,
    s.utm_campaign,
    s.channel,
    s.device_type,
    s.visitor_type,
    COUNT(DISTINCT s.website_session_id) AS sessions,
    COUNT(DISTINCT s.user_id) AS unique_users,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(o.gross_revenue) AS gross_revenue,
    SAFE_DIVIDE(COUNT(DISTINCT o.order_id), COUNT(DISTINCT s.website_session_id)) AS conversion_rate
FROM {{ ref('stg_sessions') }} s
LEFT JOIN {{ ref('stg_orders') }} o
ON s.website_session_id = o.website_session_id
GROUP BY 1,2,3,4,5,6,7