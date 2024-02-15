{%- macro doctor_fields(tbl="d") -%}
    {{ tbl }}.doctor_id, {{ tbl }}.doc_fullname
{%- endmacro -%}
