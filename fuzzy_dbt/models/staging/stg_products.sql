SELECT
    SAFE_CAST(product_id AS INT64) AS product_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    product_name
FROM {{ source('raw_fuzzy', 'products') }}