{#- Đổi timestamp có múi giờ (Silver lưu UTC) sang giờ địa phương TZ (settings.env), bỏ phần múi giờ -#}
{% macro to_local(col) -%}
CAST({{ col }} AT TIME ZONE '{{ env_var("TZ") }}' AS timestamp(6))
{%- endmacro %}

{#- Khóa ngày dạng số yyyymmdd; ngày NULL -> -1 (dòng "Không xác định" của dim_date) -#}
{% macro date_key(d) -%}
COALESCE(year({{ d }}) * 10000 + month({{ d }}) * 100 + day_of_month({{ d }}), -1)
{%- endmacro %}

{#- Thời điểm dựng bảng Gold -#}
{% macro gold_loaded_at() -%}
CAST(current_timestamp AS timestamp(6) with time zone)
{%- endmacro %}
