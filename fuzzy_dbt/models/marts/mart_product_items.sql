{{ config(materialized='table') }}

WITH item_base AS (
    SELECT
        oi.item_month,
        oi.product_id,
        p.product_name,
        p.product_short_name,
        COUNT(oi.order_item_id) AS items_sold,
        SUM(oi.item_revenue) AS item_gross_revenue,
        SUM(oi.item_gross_profit) AS item_gross_profit
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_products') }} p USING(product_id)
    GROUP BY 1,2,3,4  
),
refund_base AS (
    SELECT
        DATE_TRUNC(r.refund_date, MONTH) AS refund_month,
        oi.product_id,
        COUNT(r.order_item_refund_id) AS refund_count,
        SUM(r.refund_amount) AS refund_amount
    FROM {{ ref('stg_order_item_refunds') }} r
    LEFT JOIN {{ ref('stg_order_items') }} oi USING(order_item_id)
    GROUP BY 1,2
)
SELECT
    ib.item_month,
    ib.product_id,
    ib.product_name,
    ib.product_short_name,
    ib.items_sold,
    ib.item_gross_revenue,
    ib.item_gross_profit,
    COALESCE(rb.refund_count, 0) AS refund_count,
    COALESCE(rb.refund_amount, 0) AS refund_amount
FROM item_base ib
LEFT JOIN refund_base rb
ON ib.product_id = rb.product_id AND ib.item_month = rb.refund_month