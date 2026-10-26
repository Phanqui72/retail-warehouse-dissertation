-- Grain: 1 dòng = 1 lần thanh toán của đơn (order_id, payment_sequential)
WITH pm AS (SELECT * FROM {{ ref('stg_order_payments') }})
SELECT
    pm.order_id,
    pm.payment_sequential,
    pm.payment_type,
    pm.payment_status,
    pm.payment_amount,
    pm.paid_at,
    CAST(pm.paid_at AS date)                     AS paid_date,
    {{ date_key('CAST(pm.paid_at AS date)') }}   AS paid_date_key,
    o.order_date_key,
    o.order_id IS NOT NULL                       AS has_order,       -- false: thanh toán không tìm thấy đơn (đối soát sau)
    pm.is_invalid_amount,
    pm._silver_run_id,
    {{ gold_loaded_at() }}                       AS _gold_loaded_at
FROM pm
LEFT JOIN {{ ref('fact_orders') }} o ON o.order_id = pm.order_id
