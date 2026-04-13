{{ config(materialized='table') }}

WITH refund_timing AS (
    SELECT
        p.product_name,
        r.order_item_refund_id,
        DATE_DIFF(DATE(r.created_at), DATE(oi.created_at), DAY) AS days_to_refund
    FROM {{ ref('stg_order_item_refunds') }} r
    JOIN {{ ref('stg_order_items') }} oi
        ON r.order_item_id = oi.order_item_id
    JOIN {{ ref('stg_products') }} p
        ON oi.product_id = p.product_id
)
SELECT
    product_name,
    CASE
        WHEN days_to_refund <= 7 THEN '0-7 days'
        WHEN days_to_refund <= 15 THEN '8-15 days'
        ELSE '15+ days'
    END AS timing_bucket,
    COUNT(*) AS refund_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY product_name), 4) AS pct_of_product_refunds
FROM refund_timing
GROUP BY 1, 2
ORDER BY 1, 2