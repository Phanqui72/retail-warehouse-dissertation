{{ config(properties={"partitioning": "ARRAY['month(order_date)']"}) }}
-- Grain: 1 dòng = 1 đơn hàng (mọi trạng thái). Giữ nguyên các khoản tiền gốc; định nghĩa doanh thu làm ở lớp mart sau.
-- Khóa ngoại không khớp dimension -> giá trị "Không xác định" (-1) để Power BI không mất dòng.
WITH o AS (SELECT * FROM {{ ref('stg_orders') }}),
items AS (
    SELECT order_id,
           count(*)                     AS line_count,
           sum(quantity)                AS item_quantity,
           sum(total_amount)            AS items_total_amount,
           sum(quantity * avg_cost)     AS items_cost_amount
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.marketplace_order_sn,
    o.original_order_id,
    -- khóa ngoại
    CASE WHEN c.customer_id   IS NULL THEN '-1' ELSE o.customer_id   END AS customer_id,
    CASE WHEN s.shop_code     IS NULL THEN '-1' ELSE o.shop_code     END AS shop_code,
    CASE WHEN g.province_code IS NULL THEN '-1' ELSE o.province_code END AS province_code,
    CASE WHEN st.status_code  IS NULL THEN -1   ELSE o.status_code   END AS status_code,
    CASE WHEN o.reason_code IS NULL THEN NULL
         WHEN r.reason_code IS NULL THEN -1     ELSE o.reason_code   END AS reason_code,
    CASE WHEN o.carrier_id IS NULL THEN NULL
         WHEN ca.carrier_id IS NULL THEN -1     ELSE o.carrier_id    END AS carrier_id,
    CASE WHEN o.depot_id IS NULL THEN NULL
         WHEN w.depot_id IS NULL THEN -1        ELSE o.depot_id      END AS depot_id,
    o.sale_channel,
    o.order_type_code,
    o.order_mode_code,
    -- ngày / giờ (giờ địa phương)
    o.order_date,
    {{ date_key('o.order_date') }}               AS order_date_key,
    o.delivered_date,
    {{ date_key('o.delivered_date') }}           AS delivered_date_key,
    o.created_at,
    o.packed_at,
    o.delivered_at,
    o.updated_at,
    -- số tiền gốc của đơn (VND)
    o.cod_amount,
    o.business_payment_amount,
    o.order_discount_amount,
    o.transfer_amount,
    o.deposit_amount,
    o.ecom_fee_amount,
    o.cod_fee_amount,
    o.customer_ship_fee_amount,
    o.ship_fee_amount,
    o.return_fee_amount,
    -- tổng hợp từ dòng đơn
    COALESCE(i.line_count, 0)                    AS line_count,
    COALESCE(i.item_quantity, 0)                 AS item_quantity,
    i.items_total_amount,
    i.items_cost_amount,
    -- khác
    o.weight_gram,
    o.gift_count,
    o.voucher_code,
    o.province_name_raw,
    o.is_time_anomaly,
    o._silver_run_id,
    {{ gold_loaded_at() }}                       AS _gold_loaded_at
FROM o
LEFT JOIN items i                                         ON i.order_id = o.order_id
LEFT JOIN {{ ref('dim_customer') }}     c  ON c.customer_id   = o.customer_id
LEFT JOIN {{ ref('dim_channel_shop') }} s  ON s.shop_code     = o.shop_code
LEFT JOIN {{ ref('dim_geography') }}    g  ON g.province_code = o.province_code
LEFT JOIN {{ ref('dim_order_status') }} st ON st.status_code  = o.status_code
LEFT JOIN {{ ref('dim_order_reason') }} r  ON r.reason_code   = o.reason_code
LEFT JOIN {{ ref('dim_carrier') }}      ca ON ca.carrier_id   = o.carrier_id
LEFT JOIN {{ ref('dim_warehouse') }}    w  ON w.depot_id      = o.depot_id
