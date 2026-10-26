-- Trạng thái đơn hàng kèm các cờ phân loại (is_demand, is_net_sale, …) lấy nguyên từ danh mục nguồn
SELECT status_code, status_name, status_group, is_demand, is_net_sale, is_delivered, is_cancelled, is_returned, is_final
FROM {{ source('silver', 'order_status') }}

UNION ALL

SELECT -1, 'Không xác định', 'Không xác định', 0, 0, 0, 0, 0, 0
WHERE NOT EXISTS (SELECT 1 FROM {{ source('silver', 'order_status') }} WHERE status_code = -1)
