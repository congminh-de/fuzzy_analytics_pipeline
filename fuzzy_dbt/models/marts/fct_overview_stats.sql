{{ config(materialized='table') }}

WITH monthly_stats AS (
    SELECT
        DATE_TRUNC(created_at, MONTH) AS month,
        EXTRACT(YEAR FROM created_at) AS year,
        SUM(price_usd) AS gross_revenue,
        SUM(price_usd) - SUM(cogs_usd) AS gross_profit,
        SUM(items_purchased) AS total_sold,
        COUNT(DISTINCT order_id) AS total_orders,
        AVG(price_usd) AS aov,
        AVG(items_purchased) AS avg_items_per_order,
        COUNTIF(items_purchased > 1) / COUNT(*) AS multi_item_rate
    FROM {{ ref('stg_orders') }}
    GROUP BY 1,2
),
refunds AS (
    SELECT
        DATE_TRUNC(created_at, MONTH) AS month,
        EXTRACT(YEAR FROM created_at) AS year,
        SUM(refund_amount_usd) AS total_refunds,
        COUNT(*) AS refund_count
    FROM {{ ref('stg_order_item_refunds') }}
    GROUP BY 1,2
),
combine AS (
    SELECT
        m.*,
        COALESCE(r.total_refunds, 0) AS total_refunds,
        COALESCE(r.refund_count, 0) AS refund_count,
        m.gross_revenue - COALESCE(r.total_refunds, 0) AS net_revenue,
        m.gross_profit - COALESCE(r.total_refunds, 0) AS net_profit,
        ROUND(COALESCE(r.total_refunds,0) / m.gross_revenue * 100.0, 2) AS refund_rate_pct,
        COALESCE(LAG(m.gross_revenue) OVER(PARTITION BY m.year ORDER BY m.month), 0) AS prev_revenue,
        COALESCE(LAG(m.total_orders) OVER(PARTITION BY m.year ORDER BY m.month), 0) AS prev_orders,
        AVG(m.gross_revenue) OVER(PARTITION BY m.year) AS avg_revenue
    FROM monthly_stats m 
    LEFT JOIN refunds r USING (month)
),
advanced AS (
    SELECT
        month,
        year,
        ROUND(gross_revenue, 2) AS gross_revenue,
        ROUND(SAFE_DIVIDE(gross_revenue, avg_revenue), 2) AS seasonality_index,
        ROUND(net_revenue, 2) AS net_revenue,
        ROUND(gross_profit, 2) AS gross_profit,
        ROUND(net_profit, 2) AS net_profit,
        total_sold,
        total_orders,
        ROUND(aov, 2) AS aov,
        ROUND(avg_items_per_order, 3) AS avg_items_per_order,
        ROUND(multi_item_rate, 3) AS multi_item_rate,
        ROUND(total_refunds, 2) AS total_refunds,
        refund_count,
        refund_rate_pct,
        ROUND(SAFE_DIVIDE(100.0 * (gross_revenue - prev_revenue), prev_revenue), 2) AS revenue_growth_pct,
        ROUND(SAFE_DIVIDE(100.0 * (total_orders - prev_orders), prev_orders), 2) AS orders_growth_pct,
        ROUND(SUM(gross_revenue) OVER(ORDER BY month), 2) AS cumulative_revenue,
        ROUND(SUM(gross_profit) OVER(ORDER BY month), 2) AS cumulative_profit
    FROM combine
)
SELECT * FROM advanced