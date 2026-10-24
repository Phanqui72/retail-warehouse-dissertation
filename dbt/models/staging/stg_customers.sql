SELECT
    customer_id,
    customer_unique_id,
    customer_province_code,
    customer_province_name_raw,
    first_seen_channel,
    _silver_run_id
FROM {{ source('silver', 'customers') }}
