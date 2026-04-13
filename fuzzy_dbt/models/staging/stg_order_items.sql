SELECT
    SAFE_CAST(order_item_id AS INT64) AS order_item_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(order_id AS INT64) AS order_id,
    SAFE_CAST(product_id AS INT64) AS product_id,
    is_primary_item,
    price_usd,
    cogs_usd
FROM {{ source('raw_fuzzy', 'order_items') }}