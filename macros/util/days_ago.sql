{% macro days_ago(col_name, num_days) %}
    {{ col_name }} > current_date - INTERVAL '{{ num_days }} day'
{%- endmacro -%}
