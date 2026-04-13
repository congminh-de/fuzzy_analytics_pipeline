{{ config(materialized='table') }}

WITH first_order AS (
  SELECT
    user_id,
    DATE_TRUNC(MIN(created_at), MONTH) AS cohort_month
  FROM {{ ref('stg_orders') }}
  GROUP BY 1
),
user_orders AS (
  SELECT
    user_id,
    DATE_TRUNC(created_at, MONTH) AS order_month
  FROM {{ ref('stg_orders') }}
),
cohort_data AS (
  SELECT
    f.cohort_month,
    DATE_DIFF(DATE(u.order_month), DATE(f.cohort_month), MONTH) AS months_since_first,
    COUNT(DISTINCT u.user_id) AS users
  FROM first_order f
  JOIN user_orders u USING (user_id)
  GROUP BY 1, 2
),
cohort_size AS (
  SELECT cohort_month, users AS cohort_users
  FROM cohort_data
  WHERE months_since_first = 0
)
SELECT
  c.cohort_month,
  c.months_since_first,
  c.users,
  s.cohort_users,
  ROUND(c.users / s.cohort_users, 4) AS retention_pct
FROM cohort_data c
JOIN cohort_size s USING (cohort_month)
WHERE c.cohort_month <= '2014-09-01'  
  AND c.months_since_first <= 6
ORDER BY 1, 2