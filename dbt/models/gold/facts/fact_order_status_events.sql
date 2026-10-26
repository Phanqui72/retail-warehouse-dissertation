{{ config(properties={"partitioning": "ARRAY['month(changed_date)']"}) }}
-- Grain: 1 dòng = 1 lần đơn đổi trạng thái. event_seq / previous_status_code phục vụ phân tích luồng xử lý
-- và đối soát HIS-02/HIS-03 sau này.
WITH h AS (SELECT * FROM {{ ref('stg_order_status_history') }})
SELECT
    h.order_id,
    CASE WHEN st.status_code IS NULL THEN -1 ELSE h.status_code END AS status_code,
    h.changed_at,
    CAST(h.changed_at AS date)                                    AS changed_date,
    {{ date_key('CAST(h.changed_at AS date)') }}                  AS changed_date_key,
    h.changed_by,
    row_number() OVER (PARTITION BY h.order_id ORDER BY h.changed_at, h.status_code)  AS event_seq,
    lag(h.status_code) OVER (PARTITION BY h.order_id ORDER BY h.changed_at, h.status_code) AS previous_status_code,
    o.order_id IS NOT NULL                                        AS has_order,
    h._silver_run_id,
    {{ gold_loaded_at() }}                                        AS _gold_loaded_at
FROM h
LEFT JOIN {{ ref('dim_order_status') }} st ON st.status_code = h.status_code
LEFT JOIN {{ ref('fact_orders') }} o      ON o.order_id = h.order_id
