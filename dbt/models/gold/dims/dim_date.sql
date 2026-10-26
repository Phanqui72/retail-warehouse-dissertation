-- Lịch ngày trong khoảng GOLD_DATE_START → GOLD_DATE_END (settings.env) + 1 dòng "Không xác định" (date_key = -1)
WITH days AS (
    SELECT d
    FROM UNNEST(sequence(DATE '{{ env_var("GOLD_DATE_START") }}', DATE '{{ env_var("GOLD_DATE_END") }}', INTERVAL '1' DAY)) AS t(d)
)
SELECT
    {{ date_key('d') }}                                   AS date_key,
    d                                                     AS date_day,
    year(d)                                               AS year,
    quarter(d)                                            AS quarter,
    month(d)                                              AS month,
    day_of_month(d)                                       AS day_of_month,
    day_of_week(d)                                        AS day_of_week,       -- 1 = Thứ 2 … 7 = Chủ nhật (ISO)
    CASE day_of_week(d) WHEN 1 THEN 'Thứ 2' WHEN 2 THEN 'Thứ 3' WHEN 3 THEN 'Thứ 4' WHEN 4 THEN 'Thứ 5'
                        WHEN 5 THEN 'Thứ 6' WHEN 6 THEN 'Thứ 7' ELSE 'Chủ nhật' END AS day_name,
    week_of_year(d)                                       AS iso_week,
    year_of_week(d)                                       AS iso_year,
    format_datetime(CAST(d AS timestamp), 'yyyy-MM')      AS year_month,
    CAST(date_trunc('month', d) AS date)                  AS month_start,
    day_of_week(d) IN (6, 7)                              AS is_weekend,
    month(d) = day_of_month(d)                            AS is_double_day,     -- ngày đôi (9/9, 11/11, 12/12…) của sàn TMĐT
    d = last_day_of_month(d)                              AS is_month_end
FROM days

UNION ALL

SELECT -1, NULL, NULL, NULL, NULL, NULL, NULL, 'Không xác định', NULL, NULL, NULL, NULL, NULL, NULL, NULL
