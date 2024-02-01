SELECT tinf."schema" AS table_schema, tinf."table" AS table_name, tinf.size AS used_mb, tinf.tbl_rows AS "rows"
FROM svv_tables tab
         JOIN svv_table_info tinf ON tab.table_schema::text = tinf."schema" AND tab.table_name::text = tinf."table"
WHERE tab.table_type::text = 'BASE TABLE'::text
  AND tab.table_schema::text <> 'pg_catalog'::text
  AND tab.table_schema::text <> 'information_schema'::text
  AND tinf.tbl_rows > 1::numeric(38, 0)
ORDER BY tinf.tbl_rows DESC
