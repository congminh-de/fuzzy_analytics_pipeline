{{ config(materialized='table') }}

WITH repeat_users AS (
  SELECT user_id
  FROM {{ ref('stg_orders') }}
  GROUP BY 1
  HAVING COUNT(order_id) > 1
),
order_ranked AS (
  SELECT
    o.user_id,
    o.order_id,
    o.created_at AS order_time,
    ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.created_at) AS order_rank,
    LAG(o.created_at) OVER (PARTITION BY o.user_id ORDER BY o.created_at) AS prev_time,
    o.price_usd AS order_value,
    o.items_purchased
  FROM {{ ref('stg_orders') }} o
  WHERE o.user_id IN (SELECT user_id FROM repeat_users)
),
combine AS (
SELECT
  r.user_id,
  r.order_rank,
  r.order_time,
  EXTRACT(HOUR FROM r.order_time) AS hour_of_day,
  TIMESTAMP_DIFF(r.order_time, r.prev_time, DAY) AS time_diff,
  r.order_value,
  r.items_purchased,
  s.device_type,
  COALESCE(s.utm_source, 'direct') AS channel,
  s.utm_campaign,
  lp.pageview_url AS landing_page,
  p_main.product_name AS primary_product,
  p_add.product_name AS addon_product
FROM order_ranked r
JOIN {{ ref('stg_orders') }} o ON r.order_id = o.order_id
JOIN {{ ref('stg_website_sessions') }} s ON o.website_session_id = s.website_session_id
LEFT JOIN (
  SELECT
    website_session_id,
    pageview_url
  FROM {{ ref('stg_website_pageviews') }}
  WHERE website_pageview_id IN (
    SELECT MIN(website_pageview_id)
    FROM {{ ref('stg_website_pageviews') }}
    GROUP BY website_session_id)
) lp ON s.website_session_id = lp.website_session_id
LEFT JOIN {{ ref('stg_order_items') }} oi_main ON r.order_id = oi_main.order_id AND oi_main.is_primary_item = 1
LEFT JOIN {{ ref('stg_products') }} p_main ON oi_main.product_id = p_main.product_id
LEFT JOIN {{ ref('stg_order_items') }} oi_add ON r.order_id = oi_add.order_id AND oi_add.is_primary_item = 0
LEFT JOIN {{ ref('stg_products') }} p_add ON oi_add.product_id = p_add.product_id
ORDER BY r.user_id, r.order_rank
)
SELECT
  *,
  CASE
    WHEN time_diff <= 7   THEN '0-7 days'
    WHEN time_diff <= 14  THEN '8-14 days'
    WHEN time_diff <= 30  THEN '15-30 days'
    WHEN time_diff <= 60  THEN '31-60 days'
    WHEN time_diff <= 90  THEN '61-90 days'
    ELSE '90+ days'
END AS days_bucket
FROM combine
