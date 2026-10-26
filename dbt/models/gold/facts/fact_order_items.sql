{{ config(properties={"partitioning": "ARRAY['month(order_date)']"}) }}
-- Grain: 1 dòng = 1 dòng đơn hàng (1 SKU trong 1 đơn). Mang theo khóa của đơn (khách, shop, ngày, trạng thái)
-- để phân tích theo sản phẩm không cần join fact_orders.
WITH i AS (SELECT * FROM {{ ref('stg_order_items') }})
SELECT
    i.order_product_id,
    i.order_id,
    CASE WHEN p.product_id IS NULL THEN CAST(-1 AS bigint) ELSE i.product_id END AS product_id,
    i.source_product_id,
    i.product_code,
    o.customer_id,
    o.shop_code,
    o.province_code,
    o.status_code,
    o.order_date,
    o.order_date_key,
    -- số lượng / đơn giá / số tiền gốc
    i.quantity,
    i.original_price,
    i.unit_price,
    i.line_discount_amount,
    i.total_amount,
    i.avg_cost,
    i.quantity * i.original_price                AS list_amount,     -- theo giá niêm yết
    i.quantity * i.avg_cost                      AS cost_amount,     -- giá vốn của dòng
    i.partial_return_status,
    i.order_updated_at,
    -- cờ chất lượng từ Silver
    i.is_unknown_product,
    i.is_cost_anomaly,
    i.is_price_anomaly,
    i._silver_run_id,
    {{ gold_loaded_at() }}                       AS _gold_loaded_at
FROM i
JOIN {{ ref('fact_orders') }} o       ON o.order_id = i.order_id
LEFT JOIN {{ ref('dim_product') }} p  ON p.product_id = i.product_id
