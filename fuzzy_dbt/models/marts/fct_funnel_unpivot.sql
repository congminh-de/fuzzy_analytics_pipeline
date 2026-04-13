{{ config(materialized='table') }}

SELECT 1 AS step_order, 'Session' AS step_name, total_sessions AS users FROM {{ ref('fct_funnel') }}
UNION ALL
SELECT 2, 'Products page', to_products, FROM {{ ref('fct_funnel') }}
UNION ALL
SELECT 3, 'Product detail', to_product_detail FROM {{ ref('fct_funnel') }}
UNION ALL
SELECT 4, 'Cart', to_cart FROM {{ ref('fct_funnel') }}
UNION ALL
SELECT 5, 'Billing', to_billing FROM {{ ref('fct_funnel') }}
UNION ALL
SELECT 6, 'Converted', converted FROM {{ ref('fct_funnel') }}
ORDER BY step_order