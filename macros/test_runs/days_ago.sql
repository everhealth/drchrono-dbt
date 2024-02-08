{% macro days_ago(col_name) %}

    {% if env_var('DBT_ENVIRONMENT', '') == 'test' %}
        AND {{ col_name }} > current_date - INTERVAL '90 day'
    {% endif %}
    
{%- endmacro -%}
