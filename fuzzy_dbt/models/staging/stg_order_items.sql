SELECT
    SAFE_CAST(order_item_id AS INT64) AS order_item_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    DATE(created_at) AS item_date,
    DATE_TRUNC(DATE(created_at), MONTH) AS item_month,
    SAFE_CAST(order_id AS INT64) AS order_id,
    SAFE_CAST(product_id AS INT64) AS product_id,
    is_primary_item,
    IF(is_primary_item = 0, 1, 0) AS is_cross_sell_item,
    CAST(price_usd AS FLOAT64) AS item_revenue,
    CAST(cogs_usd  AS FLOAT64) AS item_cogs,
    CAST(price_usd AS FLOAT64) - CAST(cogs_usd AS FLOAT64) AS item_gross_profit
FROM {{ source('raw_fuzzy', 'order_items') }}