SELECT
    SAFE_CAST(order_item_refund_id AS INT64) AS order_item_refund_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(order_item_id AS INT64) AS order_item_id,
    SAFE_CAST(order_id AS INT64) AS order_id,
    refund_amount_usd
FROM {{ source('raw_fuzzy', 'order_item_refunds') }}