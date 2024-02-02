SELECT
    node,
    used,
    capacity,
    round(used::decimal/capacity, 4) AS percent_used
FROM stv_node_storage_capacity 
ORDER BY node
