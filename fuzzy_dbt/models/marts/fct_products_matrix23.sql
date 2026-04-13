{{ config(materialized='table') }}

WITH ranked AS (
  SELECT user_id, order_rank, primary_product
  FROM {{ ref('fct_repeated_buyers') }}
  WHERE order_rank IN (2,3)
)
SELECT
  r2.primary_product AS second_product,
  r3.primary_product AS third_product,
  COUNT(*) AS users
FROM ranked r2
JOIN ranked r3
  ON r2.user_id = r3.user_id
  AND r2.order_rank = 2
  AND r3.order_rank = 3
GROUP BY 1,2
ORDER BY users DESC