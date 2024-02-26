{{ config(
    post_hook = "DROP TABLE IF EXISTS public_util.util_runner"
) }}

WITH aging_ar_bucket_options AS (SELECT * FROM {{ ref("aging_ar_bucket_options") }})

, aging_ar_date_types AS (SELECT * FROM {{ ref("aging_ar_date_types") }})

, aging_ar_group_by_options AS (SELECT * FROM {{ ref("aging_ar_group_by_options") }})

, daysheet_date_types AS (SELECT * FROM {{ ref("daysheet_date_types") }})

, daysheet_group_by_options AS (SELECT * FROM {{ ref("daysheet_group_by_options") }})

SELECT 1
