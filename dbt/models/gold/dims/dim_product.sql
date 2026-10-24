-- Sản phẩm (trạng thái hiện tại, snapshot 2026-09-30) kèm thương hiệu, danh mục. Lịch sử SCD2 làm ở bước sau.
WITH p AS (SELECT * FROM {{ ref('stg_products') }})
SELECT
    p.product_id,
    p.product_code,
    p.barcode,
    p.product_name,
    p.parent_code,
    p.parent_name,
    p.product_type_code,
    p.product_status_code,
    p.brand_id,
    COALESCE(b.brand_name, 'Không xác định')     AS brand_name,
    b.positioning                                AS brand_positioning,
    p.category_id,
    COALESCE(c.category_l1, 'Chưa phân loại')    AS category_l1,
    COALESCE(c.category_l2, 'Chưa phân loại')    AS category_l2,
    p.attr_color,
    p.attr_material,
    p.attr_shape,
    p.attr_size,
    p.attr_pack_qty,
    p.retail_price,
    p.import_price,
    p.avg_cost,
    p.wholesale_price,
    p.weight_gram,
    p.created_at,
    p.updated_at,
    p.discontinued_date,
    p.is_price_anomaly,
    p.is_unknown_color
FROM p
LEFT JOIN {{ source('silver', 'brands') }}     b ON b.brand_id = p.brand_id
LEFT JOIN {{ source('silver', 'categories') }} c ON c.category_id = p.category_id

UNION ALL

SELECT CAST(-1 AS bigint), 'UNKNOWN', NULL, 'Không xác định', NULL, NULL, NULL, NULL, NULL, 'Không xác định', NULL,
       NULL, 'Chưa phân loại', 'Chưa phân loại', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, NULL, NULL, NULL
