{% set results = run_query('select 1 as id') %}
{% do results.print_table() %}
