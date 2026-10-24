SELECT reason_code, reason_name, reason_type
FROM {{ source('silver', 'order_reasons') }}

UNION ALL

SELECT -1, 'Không xác định', NULL
WHERE NOT EXISTS (SELECT 1 FROM {{ source('silver', 'order_reasons') }} WHERE reason_code = -1)
