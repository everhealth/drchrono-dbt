{%- macro filter_pg(tbl_alias) -%}

    {% if env_var('DBT_ENVIRONMENT', '') == 'test' %}
        {{ tbl_alias }}.practice_group_id = 1479
    {% else %}
        1=1
    {% endif %}

{%- endmacro -%}
