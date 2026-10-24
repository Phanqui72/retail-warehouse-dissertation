SELECT carrier_id, carrier_name, carrier_short_name
FROM {{ source('silver', 'carriers') }}

UNION ALL

SELECT -1, 'Không xác định', 'Không xác định'
WHERE NOT EXISTS (SELECT 1 FROM {{ source('silver', 'carriers') }} WHERE carrier_id = -1)
