{% test check_email(model, column_name) %}

select *
from {{ model }}
where not regexp_like({{ column_name }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')

{% endtest %}
