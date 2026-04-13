{{ config(materialized='table') }}

SELECT
    p.product_name,
    DATE(p.created_at) AS launch_date,
    ROUND(AVG(oi.price_usd), 2) AS price,
    ROUND(SUM(oi.price_usd), 2) AS gross_revenue,
    COUNT(oi.order_item_id) AS units_sold,
    ROUND((SUM(oi.price_usd) - SUM(oi.cogs_usd)) / SUM(oi.price_usd), 4) AS gross_margin_pct,
    ROUND((SUM(oi.price_usd) - SUM(oi.cogs_usd) - COALESCE(SUM(r.refund_amount_usd), 0)) / NULLIF(SUM(oi.price_usd) - COALESCE(SUM(r.refund_amount_usd), 0), 0), 4) AS net_margin_pct,
    ROUND(COUNT(r.order_item_refund_id) / COUNT(oi.order_item_id), 4) AS refund_rate,
    ROUND(SUM(CASE WHEN oi.is_primary_item = 0 THEN 1 ELSE 0 END) / COUNT(oi.order_item_id), 4) AS add_on_rate,
    SUM(CASE WHEN oi.is_primary_item = 0 THEN 1 ELSE 0 END) AS add_on_item,
    SUM(CASE WHEN oi.is_primary_item = 1 THEN 1 ELSE 0 END) AS primary_item
FROM {{ ref('stg_products') }} p
JOIN {{ ref('stg_order_items') }} oi
    ON p.product_id = oi.product_id
LEFT JOIN {{ ref('stg_order_item_refunds') }} r
    ON oi.order_item_id = r.order_item_id
GROUP BY 1, 2
ORDER BY launch_date