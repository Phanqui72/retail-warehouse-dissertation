-- Dòng đơn hàng; product_id trùng (PRD-01) được quy về product_id gốc qua product_id_map
SELECT
    i.order_product_id,
    i.order_id,
    COALESCE(m.canonical_product_id, i.product_id) AS product_id,
    i.product_id                                   AS source_product_id,
    i.product_code,
    i.quantity,
    i.original_price,
    i.unit_price,
    i.total_amount,
    i.line_discount_amount,
    i.avg_cost,
    i.partial_return_status,
    {{ to_local('i.order_updated_at') }}           AS order_updated_at,
    i.is_unknown_product,
    i.is_cost_anomaly,
    i.is_price_anomaly,
    i._silver_run_id
FROM {{ source('silver', 'order_items') }} i
LEFT JOIN {{ source('silver', 'product_id_map') }} m
       ON m.duplicate_product_id = i.product_id
