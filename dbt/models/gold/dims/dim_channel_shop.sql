-- Kênh bán / gian hàng
SELECT shop_code, sale_channel, platform, channel_name, shop_name, is_marketplace, opened_date, closed_date
FROM {{ source('silver', 'channels_shops') }}

UNION ALL

SELECT '-1', NULL, 'Không xác định', 'Không xác định', 'Không xác định', NULL, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM {{ source('silver', 'channels_shops') }} WHERE shop_code = '-1')
