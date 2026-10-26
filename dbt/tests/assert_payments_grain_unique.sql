-- Khóa ghép (order_id, payment_sequential) phải duy nhất
SELECT order_id, payment_sequential, count(*) AS n
FROM {{ ref('fact_order_payments') }}
GROUP BY 1, 2
HAVING count(*) > 1
