-- Tỉnh/thành (63 tỉnh, địa giới cũ) + dòng "Không xác định" (-1)
SELECT province_code, province_name, province_type, region, economic_region
FROM {{ source('silver', 'geolocation') }}

UNION ALL

SELECT '-1', 'Không xác định', NULL, 'Không xác định', 'Không xác định'
WHERE NOT EXISTS (SELECT 1 FROM {{ source('silver', 'geolocation') }} WHERE province_code = '-1')
