SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_amount,
    payment_status,
    {{ to_local('paid_at') }} AS paid_at,
    is_invalid_amount,
    _silver_run_id
FROM {{ source('silver', 'order_payments') }}
