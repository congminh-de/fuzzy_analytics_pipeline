{{ config(materialized='table') }}

WITH ranked AS (
  SELECT user_id, order_rank, primary_product
  FROM {{ ref('fct_repeated_buyers') }}
  WHERE order_rank IN (1,2)
)
SELECT
  r1.primary_product   AS first_product,
  r2.primary_product   AS second_product,
  COUNT(*)             AS users
FROM ranked r1
JOIN ranked r2
  ON r1.user_id = r2.user_id
  AND r1.order_rank = 1
  AND r2.order_rank = 2
GROUP BY 1,2
ORDER BY users DESC