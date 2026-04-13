{{ config(materialized='table') }}

WITH user_stats AS (
  SELECT
    user_id,
    COUNT(order_id) AS order_count,
    MIN(created_at) AS first_order_date,
    MAX(created_at) AS last_order_date,
    SUM(price_usd) AS total_revenue,
    AVG(price_usd) AS avg_order_value,
    DATE_DIFF(MAX(created_at), MIN(created_at), DAY) AS days_between_orders
  FROM {{ ref('stg_orders') }}
  GROUP BY 1
)
SELECT
  CASE WHEN order_count = 1 THEN 'single'
       WHEN order_count = 2 THEN 'double'
       ELSE 'multi (3+)'
  END AS buyer_type,
  COUNT(*) AS users,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(avg_order_value), 2) AS avg_aov,
  ROUND(AVG(days_between_orders), 0) AS avg_days_between_orders
FROM user_stats
GROUP BY 1
ORDER BY 2 DESC