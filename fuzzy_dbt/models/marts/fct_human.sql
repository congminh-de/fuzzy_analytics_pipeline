{{ config(materialized='table') }}

SELECT
  ROUND(AVG(users_mom_pct), 4) AS avg_users_mom_growth,
  ROUND(AVG(customers_mom_pct), 4) AS avg_customers_mom_growth,
  ROUND(AVG(total_users), 1) AS avg_users,
  SUM(customers) AS total_customers,
  ROUND(AVG(customers), 1) AS avg_customers
FROM (
  WITH monthly_customers AS (
  SELECT
    DATE_TRUNC(created_at, MONTH) AS month,
    COUNT(DISTINCT user_id) AS customers
  FROM {{ ref('stg_orders') }}
  GROUP BY 1
),
monthly_sessions AS (
  SELECT
    DATE_TRUNC(created_at, MONTH) AS month,
    COUNT(DISTINCT CASE WHEN is_repeat_session = 0 THEN user_id END) AS new_users,
    COUNT(DISTINCT CASE WHEN is_repeat_session = 1 THEN user_id END) AS returning_users,
    COUNT(DISTINCT user_id) AS total_users
  FROM {{ ref('stg_website_sessions') }}
  GROUP BY 1
)
SELECT
  s.month,
  s.new_users,
  s.returning_users,
  s.total_users,
  c.customers,
  ROUND(s.returning_users / s.total_users, 4) AS returning_rate_pct,
  ROUND(c.customers / s.total_users, 4) AS visitor_to_buyer_pct,
  ROUND((s.total_users - LAG(s.total_users) OVER (ORDER BY s.month)) / LAG(s.total_users) OVER (ORDER BY s.month), 4) AS users_mom_pct,
  ROUND((c.customers - LAG(c.customers) OVER (ORDER BY s.month)) / LAG(c.customers) OVER (ORDER BY s.month), 4) AS customers_mom_pct
FROM monthly_sessions s
LEFT JOIN monthly_customers c USING (month)
ORDER BY 1
)
WHERE users_mom_pct IS NOT NULL