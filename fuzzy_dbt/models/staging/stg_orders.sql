WITH raw_orders AS (
    SELECT 
        *,
        SAFE_CAST(created_at AS TIMESTAMP) AS created_at_ts,
        SAFE_CAST(price_usd AS FLOAT64) AS price_f64,
        SAFE_CAST(cogs_usd AS FLOAT64) AS cogs_f64
    FROM {{ source('raw_fuzzy', 'orders') }}
)
SELECT 
    SAFE_CAST(order_id AS INT64) AS order_id,
    created_at_ts AS created_at,
    DATE(created_at_ts) AS order_date,
    DATE_TRUNC(DATE(created_at_ts), MONTH) AS order_month,
    EXTRACT(YEAR FROM created_at_ts) AS order_year,
    EXTRACT(DAYOFWEEK FROM created_at_ts) AS day_of_week,
    website_session_id,
    user_id,
    primary_product_id,
    items_purchased,
    price_f64 AS gross_revenue,
    cogs_f64 AS cogs,
    price_f64 - cogs_f64 AS gross_profit,
    SAFE_DIVIDE(price_f64 - cogs_f64, price_f64) AS gross_margin,
    IF(items_purchased > 1, 1, 0) AS is_multi_item
FROM raw_orders



