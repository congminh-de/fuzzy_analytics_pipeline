{{ config(materialized='table') }}

SELECT
  CASE
    WHEN channel = 'NULL' THEN 'direct'
    ELSE channel
  END AS channel,
  COUNT(DISTINCT user_id) AS repeat_buyers
FROM {{ ref('fct_repeated_buyers') }}
WHERE order_rank = 1
GROUP BY 1
ORDER BY 2 DESC