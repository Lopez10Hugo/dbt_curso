-- macros/convert_time_utc.sql
{% macro convert_time_utc(timestamp_col) %}
    CONVERT_TIMEZONE('UTC', {{ timestamp_col }})
{% endmacro %}
