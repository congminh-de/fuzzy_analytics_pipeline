{{ config(materialized='table') }}

SELECT
    o.order_date,
    o.order_month,
    o.order_year,
    o.primary_product_id,
    p.product_name,
    p.product_short_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.is_multi_item) AS multi_item_orders,
    COUNT(DISTINCT o.user_id) AS unique_buyers,
    SUM(o.gross_revenue) AS gross_revenue,
    SUM(o.cogs) AS cogs,
    SUM(o.gross_profit) AS gross_profit,
    SAFE_DIVIDE(SUM(o.gross_profit), SUM(o.gross_revenue)) AS gross_margin_pct,
    SAFE_DIVIDE(SUM(o.gross_revenue), COUNT(DISTINCT o.order_id)) AS aov
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_products') }} p ON o.primary_product_id = p.product_id
GROUP BY 1,2,3,4,5,6