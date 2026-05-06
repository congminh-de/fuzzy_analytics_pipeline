SELECT
    SAFE_CAST(website_pageview_id AS INT64) AS website_pageview_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    DATE(created_at) AS pageview_date,
    DATE_TRUNC(DATE(created_at), MONTH) AS pageview_month,
    SAFE_CAST(website_session_id AS INT64) AS website_session_id,
    pageview_url,
    CASE
      WHEN pageview_url IN ('/home','/lander-2','/lander-3','/lander-5','/lander-1','/lander-4') THEN 'landing'
      WHEN pageview_url = '/products' THEN 'products'
      WHEN pageview_url IN (
        '/the-original-mr-fuzzy',
        '/the-forever-love-bear',
        '/the-birthday-sugar-panda',
        '/the-hudson-river-mini-bear') THEN 'product_detail'
      WHEN pageview_url = '/cart' THEN 'cart'
      WHEN pageview_url = '/shipping' THEN 'shipping'
      WHEN pageview_url LIKE '%billing%' THEN 'billing'
      WHEN pageview_url = '/thank-you-for-your-order' THEN 'converted'
      ELSE 'other'
    END AS funnel_step
FROM {{ source('raw_fuzzy', 'website_pageviews') }}