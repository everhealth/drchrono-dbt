{% macro apply_limit_if_test() %}
  {% if var('DBT_ENVIRONMENT', '') == 'test' %}
    LIMIT 100
  {% endif %}
{% endmacro %}
