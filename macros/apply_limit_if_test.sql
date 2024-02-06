{% macro apply_limit_if_test() %}
  {% if env_var('DBT_ENVIRONMENT', '') == 'test' %}
    LIMIT 100
  {% endif %}
{% endmacro %}
