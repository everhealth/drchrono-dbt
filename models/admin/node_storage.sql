{{
  config(
    materialized = "incremental",
    post_hook="DELETE FROM {{ this }} WHERE snapshot_time < CURRENT_TIMESTAMP - INTERVAL '7 days'"
  )
}}
WITH storage_data AS (
  SELECT
    node,
    used,
    capacity,
    ROUND(used::DECIMAL / capacity, 4) AS percent_used,
    CURRENT_TIMESTAMP AS snapshot_time
  FROM stv_node_storage_capacity
)

SELECT
  node,
  used,
  capacity,
  percent_used,
  snapshot_time
FROM storage_data
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  WHERE snapshot_time > (select max(snapshot_time) from {{ this }})
{% endif %}
ORDER BY snapshot_time, node

