-- Gold không được làm mất / nhân dòng so với Silver. Trả về dòng nào = test FAIL.
WITH c AS (
    SELECT 'orders' AS bang,
           (SELECT count(*) FROM {{ source('silver', 'orders') }})                  AS silver_rows,
           (SELECT count(*) FROM {{ ref('fact_orders') }})                          AS gold_rows
    UNION ALL
    SELECT 'order_items',
           (SELECT count(*) FROM {{ source('silver', 'order_items') }}),
           (SELECT count(*) FROM {{ ref('fact_order_items') }})
    UNION ALL
    SELECT 'order_payments',
           (SELECT count(*) FROM {{ source('silver', 'order_payments') }}),
           (SELECT count(*) FROM {{ ref('fact_order_payments') }})
    UNION ALL
    SELECT 'order_status_history',
           (SELECT count(*) FROM {{ source('silver', 'order_status_history') }}),
           (SELECT count(*) FROM {{ ref('fact_order_status_events') }})
)
SELECT * FROM c WHERE silver_rows <> gold_rows
