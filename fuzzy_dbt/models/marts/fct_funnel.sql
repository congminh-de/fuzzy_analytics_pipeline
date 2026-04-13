{{ config(materialized='table') }}

WITH funnel_steps AS (
    SELECT
        s.website_session_id,
        MAX(CASE WHEN p.pageview_url = '/products' THEN 1 ELSE 0 END) AS saw_products,
        MAX(CASE WHEN p.pageview_url IN 
            ('/the-original-mr-fuzzy',
            '/the-forever-love-bear',
            '/the-birthday-sugar-panda',
            '/the-hudson-river-mini-bear')
            THEN 1 ELSE 0 END) AS saw_product_detail,
        MAX(CASE WHEN p.pageview_url = '/cart' THEN 1 ELSE 0 END) AS saw_cart,
        MAX(CASE WHEN p.pageview_url IN ('/billing','/billing-2') THEN 1 ELSE 0 END) AS saw_billing,
        MAX(CASE WHEN p.pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END) AS saw_converted
    FROM {{ ref('stg_website_sessions') }} s JOIN {{ ref('stg_website_pageviews') }} p ON s.website_session_id = p.website_session_id
    GROUP BY 1
)
SELECT
    COUNT(*) AS total_sessions,
    SUM(saw_products) AS to_products,
    SUM(saw_product_detail) AS to_product_detail,
    SUM(saw_cart) AS to_cart,
    SUM(saw_billing) AS to_billing,
    SUM(saw_converted) AS converted,
    ROUND(SUM(saw_products)/COUNT(*), 3) AS pct_to_products,
    ROUND(SUM(saw_product_detail)/SUM(saw_products), 3) AS pct_to_detail,
    ROUND(SUM(saw_cart)/SUM(saw_product_detail), 3) AS pct_to_cart,
    ROUND(SUM(saw_billing)/SUM(saw_cart), 3) AS pct_to_billing,
    ROUND(SUM(saw_converted)/SUM(saw_billing), 3) AS pct_to_order
FROM funnel_steps