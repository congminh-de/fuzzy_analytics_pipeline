SELECT
    SAFE_CAST(order_id AS INT64) AS order_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(website_session_id AS INT64) AS website_session_id,
    SAFE_CAST(user_id AS INT64) AS user_id,
    SAFE_CAST(primary_product_id AS INT64) AS primary_product_id,
    items_purchased,
    price_usd,
    cogs_usd
FROM {{ source('raw_fuzzy', 'orders') }}