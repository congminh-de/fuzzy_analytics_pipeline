SELECT 
    SAFE_CAST(product_id AS INT64) AS product_id,
    SAFE_CAST(created_at AS TIMESTAMP) AS launch_date,
    product_name,
    CASE product_id
        WHEN 1 THEN 'Mr. Fuzzy'
        WHEN 2 THEN 'Love Bear'
        WHEN 3 THEN 'Sugar Panda'
        ELSE 'Mini Bear'
    END AS product_short_name 
FROM {{ source('raw_fuzzy', 'products') }}