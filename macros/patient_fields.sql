{%- macro patient_fields(tbl) -%}
    {{ tbl }}.patient_id,{{ tbl }}.chart_id,{{ tbl }}.patient_fullname

{%- endmacro -%}
