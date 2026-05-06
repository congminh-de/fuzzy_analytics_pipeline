SELECT
  SAFE_CAST(order_item_refund_id AS INT64) AS order_item_refund_id,
  CAST(created_at AS TIMESTAMP) AS created_at,
  DATE(created_at) AS refund_date,
  DATE_TRUNC(DATE(created_at), MONTH) AS refund_month,
  SAFE_CAST(order_item_id AS INT64) AS order_item_id,
  SAFE_CAST(order_id AS INT64) AS order_id,
  CAST(refund_amount_usd AS FLOAT64) AS refund_amount
FROM {{ source('raw_fuzzy', 'order_item_refunds') }}
