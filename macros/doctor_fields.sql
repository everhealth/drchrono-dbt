{%- macro doctor_fields(tbl) -%}
    {{ tbl }}.doctor_id as doctor_id, {{ tbl }}.doc_fullname as doc_fullname

{%- endmacro -%}
