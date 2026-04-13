{{ config(materialized='table') }}

WITH product_views AS (
    SELECT
        p.pageview_url,
        COUNT(DISTINCT p.website_session_id) AS sessions_viewed
    FROM {{ ref('stg_website_pageviews') }} p
    WHERE pageview_url IN (
        '/the-original-mr-fuzzy',
        '/the-forever-love-bear',
        '/the-birthday-sugar-panda',
        '/the-hudson-river-mini-bear'
    )
    GROUP BY 1
),
product_orders AS (
    SELECT
        CASE product_id
            WHEN 1 THEN '/the-original-mr-fuzzy'
            WHEN 2 THEN '/the-forever-love-bear'
            WHEN 3 THEN '/the-birthday-sugar-panda'
            WHEN 4 THEN '/the-hudson-river-mini-bear'
        END AS pageview_url,
        COUNT(DISTINCT order_id) AS orders
    FROM {{ ref('stg_order_items') }}
    WHERE is_primary_item = 1
    GROUP BY 1
)
SELECT
    CASE pageview_url
        WHEN '/the-original-mr-fuzzy' THEN 'Mr. Fuzzy'
        WHEN '/the-forever-love-bear' THEN 'Love Bear'
        WHEN '/the-birthday-sugar-panda' THEN 'Sugar Panda'
        WHEN '/the-hudson-river-mini-bear' THEN 'Mini Bear'
    END AS product_name,
    v.sessions_viewed,
    o.orders,
    v.sessions_viewed - o.orders AS viewed_not_bought,
    ROUND(o.orders / v.sessions_viewed, 4) AS cvr
FROM product_views v
LEFT JOIN product_orders o USING (pageview_url)
ORDER BY orders DESC