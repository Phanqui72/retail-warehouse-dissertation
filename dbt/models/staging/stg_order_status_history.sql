SELECT
    order_id,
    status_code,
    {{ to_local('changed_at') }} AS changed_at,
    changed_by,
    _silver_run_id
FROM {{ source('silver', 'order_status_history') }}
