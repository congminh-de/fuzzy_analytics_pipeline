{{ config(materialized='table') }}

WITH monthly_sessions AS (
  SELECT
    DATE_TRUNC(created_at, MONTH) AS month,
    COUNT(DISTINCT user_id) AS total_users
  FROM {{ ref('stg_website_sessions') }}
  GROUP BY 1
),
monthly_buyers AS (
  SELECT
    DATE_TRUNC(created_at, MONTH) AS month,
    COUNT(DISTINCT user_id) AS buyers
  FROM {{ ref('stg_orders') }}
  GROUP BY 1
)
SELECT
  s.month,
  COALESCE(b.buyers, 0) AS buyers,
  s.total_users - COALESCE(b.buyers, 0) AS viewers_only,
  s.total_users AS total_users,
  ROUND(COALESCE(b.buyers, 0) / s.total_users, 4) AS visitor_to_buyer_rate
FROM monthly_sessions s
LEFT JOIN monthly_buyers b USING (month)
ORDER BY 1