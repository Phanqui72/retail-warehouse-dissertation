SELECT depot_id, depot_code, warehouse_name, warehouse_role, region, province_code, is_stock_alert, opened_date
FROM {{ source('silver', 'warehouses') }}

UNION ALL

SELECT -1, NULL, 'Không xác định', NULL, NULL, NULL, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM {{ source('silver', 'warehouses') }} WHERE depot_id = -1)
