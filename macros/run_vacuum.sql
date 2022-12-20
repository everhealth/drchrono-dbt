{% macro run_vacuum(table, percent) %}
    {% set query %}
        vacuum {{table}} to {{percent|default(95)}}
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
