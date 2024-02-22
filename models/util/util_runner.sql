
with aging_ar_bucket_options as ( select * FROM  {{ ref("aging_ar_bucket_options") }} ),

aging_ar_date_types as ( select * FROM  {{ ref("aging_ar_date_types") }} ),

aging_ar_group_by_options as ( select * FROM  {{ ref("aging_ar_group_by_options") }} ),

daysheet_date_types as ( select * FROM  {{ ref("daysheet_date_types") }} ),

daysheet_group_by_options as ( select * FROM  {{ ref("daysheet_group_by_options") }} )

SELECT 1