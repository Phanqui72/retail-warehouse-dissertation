-- Khách hàng (định danh giả danh) + tỉnh/vùng
WITH c AS (SELECT * FROM {{ ref('stg_customers') }})
SELECT
    c.customer_id,
    c.customer_unique_id,
    CASE WHEN g.province_code IS NULL THEN '-1' ELSE c.customer_province_code END AS province_code,
    COALESCE(g.province_name, 'Không xác định')   AS province_name,
    COALESCE(g.region, 'Không xác định')          AS region,
    c.first_seen_channel
FROM c
LEFT JOIN {{ source('silver', 'geolocation') }} g ON g.province_code = c.customer_province_code

UNION ALL

SELECT '-1', NULL, '-1', 'Không xác định', 'Không xác định', NULL
