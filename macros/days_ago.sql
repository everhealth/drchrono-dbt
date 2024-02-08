{% macro days_ago(col_name, num_days) %}

    {% if env_var('DBT_ENVIRONMENT') == 'test' %}
        {{ col_name }} > current_date - INTERVAL '{{ num_days }} day'
    {% else %}
        1=1
    {% endif %}
    
{%- endmacro -%}
